use core::time::Duration;
use std::f64;
use futures::{SinkExt, StreamExt, stream::SplitSink};
use warp::filters::ws::{Message, WebSocket};
use warp::Filter;

use std::sync::Arc; 
use tokio::sync::RwLock; 

use std::collections::HashMap; 

use serde::{Serialize, Deserialize}; 

type NodeId = String;
type ClientId = u128; 

#[derive(Deserialize, Debug)]
struct Subscription {
    node_id: NodeId, 
    unsubscribe: bool
}

#[derive(Serialize, Debug)] 
struct NodeUpdate {
    id: NodeId, 
    data: Option<Data>, 
    connections: Option<Connections>, 
    roles: Option<ActiveRoles>, 
    geometry: Option<Geometry>
}

#[derive(Serialize, Debug)] 
struct Geometry{
    x: f64, 
    y: f64, 
    r: f64
}

#[derive(Serialize, Debug)] 
struct Data {
    title: String, 
    description: String
}

#[derive(Serialize, Debug)] 
struct ActiveRoles {
    roles: Vec<Role>
}

#[derive(Serialize, Debug)] 
struct Role {
    name: String, 
    identity: String
}

#[derive(Serialize, Debug)] 
struct Connection {
    from: NodeId, 
    to: NodeId, 
    value: f64
}

#[derive(Serialize, Debug)] 
struct Connections {
    from: HashMap<NodeId, Connection>, 
    to: HashMap<NodeId, Connection>
}

type Sender = SplitSink<WebSocket, Message>; 

struct Subs {
    by_nodes: HashMap<NodeId, Vec<ClientId>>, 
    by_clients: HashMap<ClientId, Vec<NodeId>>,  
    senders: HashMap<ClientId, Sender>, 
    next_id: ClientId
}

impl Subs {
    pub fn new() -> Self {
        Self {
            by_nodes: HashMap::new(), 
            by_clients: HashMap::new(), 
            senders: HashMap::new(), 
            next_id: 0
        }
    }
    pub fn add_client(&mut self, sender: Sender) -> ClientId {
        let id = self.next_id; 
        self.next_id += 1; 
        self.senders.insert(id, sender); 
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
    pub fn subscribe(&mut self, client_id: ClientId, node_id: &NodeId) {
        match self.by_nodes.get_mut(node_id) {
            Some(loc) => {
                loc.push(client_id); 
            }, 
            None => {
                let loc : Vec<ClientId> = vec![client_id]; 
                self.by_nodes.insert(node_id.into(), loc); 
            }
        };

        match self.by_clients.get_mut(&client_id) {
            Some(lon) => {
                lon.push(node_id.into());
            }, 
            None => {
                let lon : Vec<NodeId> = vec![node_id.into()]; 
                self.by_clients.insert(client_id, lon); 
            }
        }; 
    }
}

#[tokio::main]
async fn main() {
    let subs = Arc::new(RwLock::new(Subs::new())); 

    let routes = warp::path("v1")
        .and(warp::ws()) 
        .map(move |ws: warp::ws::Ws| {
            let subs_clone = subs.clone();
            ws.on_upgrade(move |socket| handle_ws_client(socket, subs_clone.clone())) 
        });

    const PORT :u16 = 3030; 
    println!("about to serve on port {}", PORT); 
    warp::serve(routes).run(([127, 0, 0, 1], PORT)).await;
}

async fn handle_ws_client(websocket: WebSocket, subs: Arc<RwLock<Subs>>) {
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
        handle_ws_message(message, subs.clone(), client_id).await; 
    }

    println!("client disconnected") 
}

async fn handle_ws_message(message: Message, subs: Arc<RwLock<Subs>>, client_id: ClientId) {
    // Skip any non-Text messages...
    println!("received message: {:?}", message);
    let msg = if let Ok(s) = message.to_str() {
        s
    } else {
        println!("ping-pong");
        return;
    };

    let sub: Subscription = serde_json::from_str(msg).unwrap();
    if sub.unsubscribe {
        println!("client {} subscribing to node {}", client_id, sub.node_id);
        subs.write().await.subscribe(client_id, &sub.node_id); 
    } else {
        println!("client {} unsubscribing from node {}", client_id, sub.node_id);
        subs.write().await.unsubscribe(client_id, sub.node_id); 
    }

    std::thread::sleep(Duration::new(1, 0));
}

async fn send_seven_summits(sender: &mut Sender) {
    for n in 0..7 {
        let v = 2.0 * 3.1415 / 7.0 * (n as f64); 
        let update = serde_json::to_string(&NodeUpdate {
            id: format!("summit{}", n), 
            data: Some(Data {
                title: format!("summit {}", n), 
                description: "descr".to_string()
            }), 
            connections: Some( Connections {
                from: HashMap::from([
                    (format!("summit{}", (n + 7 - 1) % 7), Connection {
                        from: format!("summit{}", (n + 7 - 1) % 7),
                        to: format!("summit{}", n), 
                        value: 0.5
                    })
                ]), 
                to: HashMap::from([
                    (format!("summit{}", (n + 1) % 7), Connection {
                        from: format!("summit{}", n),
                        to: format!("summit{}", (n + 1) % 7 ), 
                        value: 0.5
                    })
                ]), 
            }), 
            roles: None, 
            geometry: Some(Geometry {
                x: v.sin() * 6.0, 
                y: v.cos() * 6.0,
                r: 1.0
            })
        })
        .unwrap();
        sender.send(Message::text(update)).await.unwrap();
    }
}
