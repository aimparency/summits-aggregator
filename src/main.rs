/** This is a server for summits. 
 * It allows for subscribing for 
 *  - summit-ids and 
 *  - connections
 */

use core::time::Duration;
use futures::{SinkExt, StreamExt, stream::SplitSink};
use warp::filters::ws::{Message, WebSocket};
use warp::Filter;

use std::sync::Arc; 
use tokio::sync::RwLock; 
use tokio::sync::Mutex; 

use std::collections::HashMap; 

use serde::{Serialize, Deserialize}; 
use serde_with::skip_serializing_none; 

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
    NodeSubscription(NodeSubscriptionMessage), 
    NodeDesubscription(NodeDesubscriptionMessage)
    ,// temporary (mocking all the blockchain stuff away) 
    NodeCreation(NodeCreationMessage), 
    FlowCreation(FlowCreationMessage)
}

#[derive(Deserialize, Debug)]
struct NodeSubscriptionMessage {
    node_id: NodeId
}

#[derive(Deserialize, Debug)]
struct NodeDesubscriptionMessage {
    node_id: NodeId
}

#[derive(Deserialize, Debug)]
struct NodeCreationMessage {
    id: NodeId, 
    title: String, 
    notes: String
}

#[derive(Deserialize, Debug)]
struct FlowCreationMessage {
    from_id: NodeId, 
    into_id: NodeId, 
    notes: String, 
    share: f32
}

#[derive(Serialize, Debug)] 
enum OutgoingMessage {
    SevenSummits(SevenSummitsMessage),
    NodeUpdate(NodeUpdateMessage), 
    FlowUpdate(FlowUpdateMessage), 
}

#[derive(Serialize, Debug)] 
struct SevenSummitsMessage {
    node_ids: Vec<NodeId>
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct NodeUpdateMessage {
    id: NodeId, 
    // info
    title: Option<String>, 
    notes: Option<String>, 
    // geometry
    // x: Option<f64>, // is there a derive, that makes everything an Option? Apply to all node field structs
    // y: Option<f64>, 
    // r: Option<f64>
    // flows
    // flows_from: Option<Vec<NodeId>>, 
    // flows_into: Option<Vec<NodeId>>, 
    // flows_from_additions: Option<Vec<NodeId>>, 
    // flows_into_additions: Option<Vec<NodeId>>
    // roles
    // roles: Option<Vec<NodeRole>>
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct FlowUpdateMessage {
    from_id: NodeId, 
    into_id: NodeId, 
    notes: Option<String>, 
    share: Option<f32>
}

type Client = SplitSink<WebSocket, Message>; 

struct Subs {
    by_nodes: HashMap<NodeId, Vec<ClientId>>, 
    by_clients: HashMap<ClientId, Vec<NodeId>>,  
    clients: HashMap<ClientId, Arc<Mutex<Client>>>, 
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
        self.clients.insert(id, Arc::new(Mutex::new(sender)));
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
    pub fn get_clients_by_ids(&self, client_ids: Vec<ClientId>) -> Vec<Arc<Mutex<Client>>> {
        let mut clients = vec![]; 
        for client_id in client_ids {
            match self.clients.get(&client_id) {
                Some(client) => clients.push(client.clone()), 
                None => ()
            }
        }
        clients
    }
    pub fn get_client_ids_by_node_id(&self, node_id: NodeId) -> Vec<ClientId> {
        match self.by_nodes.get(&node_id) {
            Some(client_ids) => client_ids.clone(), 
            None => vec![]
        }
    }
    pub fn get_clients_by_node_id(
        &self, 
        node_id: NodeId
    ) -> Vec<Arc<Mutex<Client>>> {
        match self.by_nodes.get(&node_id) {
            Some(client_ids) => {
                let mut clients = vec![]; 
                for client_id in client_ids {
                    match self.clients.get(&client_id) {
                        Some(client) => clients.push(client.clone()), 
                        None => ()
                    }
                }
                clients
            }, 
            None => vec![]
        }
    }
    pub fn get_client(&self, client_id: ClientId) -> Option<Arc<Mutex<Client>>> {
        match self.clients.get(&client_id) {
            Some(client) => Some(client.clone()), 
            None => None
        }
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

    send_seven_summits(&mut sender, db_connection.clone()).await;

    let client_id = subs.write().await.add_client(sender);  // maybe wrap sender into arc and access it directly. Locking the whole sub for sending messages is not good. Locking clients individually is what we want to be doing

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
            handle_message(incoming_message, subs.clone(), db_connection, client_id).await
        }, 
        Err(err) => {
            println!("failed to json-parse message. {} {}", msg, err)
        }
    }

    std::thread::sleep(Duration::new(1, 0));
}

async fn handle_message(
    msg: IncomingMessage,
    subs: Arc<RwLock<Subs>>,
    db_connection: Arc<Mutex<PgConnection>>,
    client_id: ClientId
) {
    match msg { 
        IncomingMessage::NodeSubscription(sub) => {
            println!("client {} subscribing to node {}", client_id, sub.node_id);
            subs.write().await.subscribe(client_id, sub.node_id); 
            // lock client
            initial_update(client_id, db_connection, sub.node_id, subs.clone()).await;
            // release client
        }, 
        IncomingMessage::NodeDesubscription(desub) => {
            println!("client {} unsubscribing from node {}", client_id, desub.node_id);
            subs.write().await.unsubscribe(client_id, desub.node_id); 
        }, 
        IncomingMessage::NodeCreation(node_creation) => {
            println!("client {} creating node", client_id); 
            create_node(node_creation, subs, db_connection).await;
        },        
        IncomingMessage::FlowCreation(flow_creation) => {
            println!("client {} creating flow", client_id); 
            create_flow(flow_creation, subs, db_connection).await; 
        }
    }
}

async fn create_node(
    node_creation: NodeCreationMessage,
    subs: Arc<RwLock<Subs>>,
    db_connection: Arc<Mutex<PgConnection>>,
) {
    use schema::nodes::dsl::*; 

    let new_node = (
        id.eq(node_creation.id),
        title.eq(node_creation.title.clone()),
        notes.eq(node_creation.notes.clone())
    ); 
    
    diesel::insert_into(nodes)
        .values(&new_node)
        .execute(&*db_connection.lock().await)
        .unwrap(); 

    let subs_w = subs.read().await; 

    let clients = subs_w.get_clients_by_node_id(node_creation.id); 

    for c in clients {
        let msg = OutgoingMessage::NodeUpdate(NodeUpdateMessage { 
            id: node_creation.id, 
            title: Some(node_creation.title.clone()), 
            notes: Some(node_creation.notes.clone()), 
        });
        let json = serde_json::to_string(&msg).unwrap();

        c.lock().await.send(Message::text(json)).await.unwrap();
    }
}

async fn create_flow(
    flow_creation: FlowCreationMessage,
    subs: Arc<RwLock<Subs>>,
    db_connection: Arc<Mutex<PgConnection>>,
) {
    use schema::flows::dsl::*; 

    let new_flow = (
        from_id.eq(flow_creation.from_id),
        into_id.eq(flow_creation.into_id),
        notes.eq(flow_creation.notes.clone()), 
        share.eq(flow_creation.share)
    ); 

    let result = diesel::insert_into(flows)
        .values(&new_flow)
        .execute(&*db_connection.lock().await); 

    match result {
        Ok(..) => {
            send_create_flow_updates(
                flow_creation, 
                subs, 
            ).await
        }, 
        Err(err) => {
            println!("DB error when creating flow {}", err)
        }
    };
}

async fn send_create_flow_updates(
    flow_creation: FlowCreationMessage, 
    subs: Arc<RwLock<Subs>>
) {
    let subs_w = subs.read().await; 
    
    // send node update. Maybe onlye send connection update to everybody and let client handle it
    // let from_node_clients = subs_w.get_clients_by_ids(from_node_client_ids.clone());
    // for c in from_node_clients {
    //     let msg = OutgoingMessage::NodeUpdate(NodeUpdateMessage { 
    //         id: flow_creation.from_id, 
    //         title: None, 
    //         notes: None, 
    //         flows_from: None, 
    //         flows_into_additions: Some(vec![flow_creation.into_id]), 
    //         flows_into: None, 
    //         flows_from_additions: None
    //     });
    //     let json = serde_json::to_string(&msg).unwrap();

    //     c.lock().await.send(Message::text(json)).await.unwrap();
    // }

    // send node update. Maybe only send connection update...
    // let into_node_clients = subs_w.get_clients_by_ids(into_node_client_ids.clone()); 
    // for c in into_node_clients {
    //     let msg = OutgoingMessage::NodeUpdate(NodeUpdateMessage { 
    //         id: flow_creation.into_id, 
    //         title: None, 
    //         notes: None, 
    //         flows_from: None, 
    //         flows_from_additions: Some(vec![flow_creation.from_id]), 
    //         flows_into: None, 
    //         flows_into_additions: None
    //     });
    //     let json = serde_json::to_string(&msg).unwrap();

    //     c.lock().await.send(Message::text(json)).await.unwrap();
    // }
    
    let from_node_client_ids = subs_w.get_client_ids_by_node_id(flow_creation.from_id); 
    let into_node_client_ids = subs_w.get_client_ids_by_node_id(flow_creation.into_id); 

    let flow_client_ids = from_node_client_ids.into_iter().filter(|client_id| {
        for i in into_node_client_ids.iter() {
            if i.eq(client_id) {
                return false
            } 
        }
        true
    }).chain(into_node_client_ids.clone().into_iter()).collect();

    let flow_clients = subs_w.get_clients_by_ids(flow_client_ids); 

    for c in flow_clients {
        let msg = OutgoingMessage::FlowUpdate(FlowUpdateMessage { 
            from_id: flow_creation.from_id, 
            into_id: flow_creation.into_id, 
            notes: Some(flow_creation.notes.clone()), 
            share: Some(flow_creation.share)
        });
        let json = serde_json::to_string(&msg).unwrap();

        match c.lock().await.send(Message::text(json)).await {
            Ok(..) => (), 
            Err(..) => {
                // remove this client, its connection is probably closed
                // add this client to a list of to be cleaned clients
                ()
            }
        }
    }
}

async fn initial_update(
    client_id: ClientId,
    db_connection: Arc<Mutex<PgConnection>>,
    node_id: NodeId, 
    subs: Arc<RwLock<Subs>>
) {
    use schema::nodes::dsl::*; 
    use schema::flows::dsl::*; 

    let result = nodes.find(node_id)
        .first::<Node>(&*db_connection.lock().await);

    match result {
        Ok(node) => {
            println!("sending initial update for node {}", node.title); 
            let flows_into = flows.filter(from_id.eq(node_id))
                .load::<Flow>(&*db_connection.lock().await).unwrap(); 
            let flows_from = flows.filter(into_id.eq(node_id))
                .load::<Flow>(&*db_connection.lock().await).unwrap(); 
            
            let subs_w = subs.read().await; 
            
            let flow_updates = flows_from.iter().chain(flows_into.iter()); 

            match subs_w.get_client(client_id) {
                Some(c) => {
                    let msg = OutgoingMessage::NodeUpdate(NodeUpdateMessage {
                        id: node.id, 
                        title: Some(node.title), 
                        notes: Some(node.notes),
                    });
                    let json = serde_json::to_string(&msg).unwrap();

                    c.lock().await.send(Message::text(json)).await.unwrap();

                    for flow in flow_updates {
                        let msg = OutgoingMessage::FlowUpdate(FlowUpdateMessage {
                            from_id: flow.from_id, 
                            into_id: flow.into_id, 
                            notes: Some(flow.notes.clone()), 
                            share: Some(flow.share)
                        }); 
                        let txt = serde_json::to_string(&msg).unwrap();
                        c.lock().await.send(Message::text(txt)).await.unwrap();
                    }
                }, 
                None => {
                    println!("client does not exists") 
                }
            }

        }, 
        Err(err) => {
            println!("could not find node {} in db: {}", node_id, err); 
        }
    }
}

async fn send_seven_summits(sender: &mut Client, db_connection: Arc<Mutex<PgConnection>>) {
    use schema::nodes::dsl::*; 

    let result = nodes.limit(7).load::<Node>(&*db_connection.lock().await); 

    match result {
        Ok(rows) => {
            let msg = OutgoingMessage::SevenSummits(SevenSummitsMessage {
                node_ids: rows.iter().map(|n| n.id).collect()
            }); 
            let txt = serde_json::to_string(&msg).unwrap(); 
            sender.send(Message::text(txt)).await.unwrap();
        }, 
        Err(err) => {
            println!("faild to get 7 summits from db:{}", err) 
        }
    }
}

