/** This is a server for summits. 
 * It allows for subscribing for 
 *  - summit-ids and 
 *  - connections
 */

use core::time::Duration;
use std::f64;
use futures::{SinkExt, StreamExt, stream::SplitSink};
use warp::filters::ws::{Message, WebSocket};
use warp::Filter;

use std::sync::Arc; 
use tokio::sync::RwLock; 
use tokio::sync::Mutex; 

use std::collections::HashMap; 

use serde::{Serialize, Deserialize}; 

#[macro_use]
extern crate diesel; 
use dotenv; 

pub mod schema; 
pub mod models; 

use models::*;

use diesel::prelude::*; 
use diesel::pg::PgConnection;
use std::env; 

type ClientId = u128; 

#[derive(Deserialize, Debug)]
enum IncomingMessage {
    Subscription(NodeSubscriptionMessage), 
    Desubscription(NodeDesubscriptionMessage)
}

#[derive(Deserialize, Debug)]
struct NodeSubscriptionMessage {
    node_id: NodeId
}

#[derive(Deserialize, Debug)]
struct NodeDesubscriptionMessage {
    node_id: NodeId
}

// #[derive(Deserialize, Debug)]
// struct ConnectionSubscriptionMessage {
//     from: NodeId, 
//     to: NodeId, 
//     unsubscribe: bool
// }

#[derive(Serialize, Debug)] 
struct InitMessage {
    node_ids: Vec<NodeId>
}

#[derive(Serialize, Debug)] 
struct NodeUpdateMessage {
    id: NodeId, 
    data: Option<NodeData>, 
    connections: Option<NodeConnections>, 
    roles: Option<NodeRoles>, 
    geometry: Option<NodeGeometry>
}

#[derive(Serialize, Debug)] 
struct ConnectionUpdateMessage {
    from: NodeId, 
    to: NodeId, 
    notes: Option<String>, 
    share: Option<f32>
}

#[derive(Serialize, Debug)] 
struct NodeGeometry{
    x: f64, // is there a derive, that makes everything an Option? Apply to all node field structs
    y: f64, 
    r: f64
}

#[derive(Serialize, Debug)] 
struct NodeData {
    title: String, 
    notes: String
}

#[derive(Serialize, Debug)] 
struct NodeRoles {
    roles: Vec<NodeRole>
}

#[derive(Serialize, Debug)] 
struct NodeRole {
    name: String, 
    identity: String
}

#[derive(Serialize, Debug)] 
struct NodeConnections {
    from: Vec<NodeId>, 
    to: Vec<NodeId>
}

type Client = SplitSink<WebSocket, Message>; 

struct Subs {
    by_nodes: HashMap<NodeId, Vec<ClientId>>, 
    by_clients: HashMap<ClientId, Vec<NodeId>>,  
    clients: HashMap<ClientId, Client>, 
    next_client_id: ClientId
}

impl Subs {
    pub fn new() -> Self {
        Self {
            by_nodes: HashMap::new(), 
            by_clients: HashMap::new(), 
            clients: HashMap::new(), 
            next_client_id: 0
        }
    }
    pub fn add_client(&mut self, sender: Client) -> ClientId {
        let id = self.next_client_id; 
        self.next_client_id += 1; 
        self.clients.insert(id, sender); 
        self.by_clients.insert(id, Vec::new()); 
        id
    }
    pub fn unsubscribe(&mut self, client_id: ClientId, node_id: NodeId) {
        match self.by_nodes.get_mut(&node_id) {
            Some(loc) => {
                loc.retain(|&i| i != client_id); 
            }, 
            None => ()
        };

        match self.by_clients.get_mut(&client_id) {
            Some(lon) => {
                lon.retain(|i| !i.eq(&node_id));
            }, 
            None => ()
        }; 
    }
    pub fn subscribe(&mut self, client_id: ClientId, node_id: NodeId) {
        match self.by_nodes.get_mut(&node_id) {
            Some(loc) => {
                loc.push(client_id); 
            }, 
            None => {
                let loc : Vec<ClientId> = vec![client_id]; 
                self.by_nodes.insert(node_id, loc); 
            }
        };

        match self.by_clients.get_mut(&client_id) {
            Some(lon) => {
                lon.push(node_id);
            }, 
            None => {
                let lon : Vec<NodeId> = vec![node_id]; 
                self.by_clients.insert(client_id, lon); 
            }
        }; 
    }
}



#[tokio::main]
async fn main() {
    dotenv::dotenv().ok(); 

    println!("establishing connection to db"); 
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
    let db_error = format!("Error connecting to {}", db_url); 
    let db_connection = Arc::new(Mutex::new(
        PgConnection::establish(&db_url).expect(&db_error)
    ));

    let subs = Arc::new(RwLock::new(Subs::new())); 

    let routes = warp::path("v1")
        .and(warp::ws()) 
        .map(move |ws: warp::ws::Ws| {
            let subs_clone = subs.clone();
            let db_connection_clone = db_connection.clone();
            ws.on_upgrade(move |socket| handle_ws_client(socket, subs_clone, db_connection_clone)) 
        });

    const PORT :u16 = 3030; 
    println!("about to serve on port {}", PORT); 
    warp::serve(routes).run(([127, 0, 0, 1], PORT)).await;
}

async fn handle_ws_client(
    websocket: WebSocket, 
    subs: Arc<RwLock<Subs>>, 
    db_connection: Arc<Mutex<PgConnection>>
) {
    println!("client connecting");

    let (mut sender, mut receiver) = websocket.split();

    send_seven_summits(&mut sender).await;

    let client_id = subs.write().await.add_client(sender); 

    while let Some(body) = receiver.next().await {
        let message = match body {
            Ok(msg) => msg, 
            Err(e) => {
                println!("error reading msg on websocket: {}", e); 
                break; 
            }
        }; 
        handle_ws_message(message, subs.clone(), db_connection.clone(), client_id).await; 
    }

    println!("client disconnected") 
}

async fn handle_ws_message(
    message: Message, 
    subs: Arc<RwLock<Subs>>, 
    db_connection: Arc<Mutex<PgConnection>>,
    client_id: ClientId
) {
    let msg = if let Ok(s) = message.to_str() {
        s
    } else {
        println!("not a string message");
        return;
    };

    match serde_json::from_str(msg) {
        Ok(incoming_message) => { 
            handle_message(incoming_message, &subs, db_connection, client_id).await
        }, 
        Err(err) => {
            println!("failed to json-parse message. {} {}", msg, err)
        }
    }

    std::thread::sleep(Duration::new(1, 0));
}

async fn handle_message(
    msg: IncomingMessage,
    subs: &Arc<RwLock<Subs>>,
    db_connection: Arc<Mutex<PgConnection>>,
    client_id: ClientId
) {
    match msg { 
        IncomingMessage::Subscription(sub) => {
            println!("client {} subscribing to node {}", client_id, sub.node_id);
            subs.write().await.subscribe(client_id, sub.node_id); 
            initial_update(client_id, db_connection, sub.node_id).await;
        }, 
        IncomingMessage::Desubscription(desub) => {
            println!("client {} unsubscribing from node {}", client_id, desub.node_id);
            subs.write().await.unsubscribe(client_id, desub.node_id); 
        }
    }
}

async fn initial_update(
    client_id: ClientId, 
    db_connection: Arc<Mutex<PgConnection>>,
    node_id: NodeId
) {
    use schema::nodes::dsl::*; 

    let result = nodes.find(node_id)
        .first::<Node>(&*db_connection.lock().await);

    match result {
        Ok(node) => {
            println!("{}", node.title); 
        }, 
        Err(err) => {
            println!("could not find node {} in db. {}", node_id, err)
        }
    }

    // use some database to get node
    // get node neighbors
    // get client node subscriptions
    // union(node neighbors, client node subscriptions) 
    // send node_update 
    // send connectionUpdates
}

async fn send_seven_summits(sender: &mut Client) {
    let node_ids = vec![]; 

    let message = InitMessage {
        node_ids
    }; 

    let message_string = serde_json::to_string(&message).unwrap(); 
    sender.send(Message::text(message_string)).await.unwrap();
}
