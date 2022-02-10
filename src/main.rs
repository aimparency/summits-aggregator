/** This is a server for summits. 
 * It allows clients to subscribe to node updates. 
 * And it will also aggregate state from the near blockchain. 
 */

use futures; 
use futures::{future, SinkExt, StreamExt, stream::SplitSink};

use tracing::info;
use warp::filters::ws::{Message, WebSocket};
use warp::Filter;

use std::sync::Arc; 
use tokio::sync::{
    RwLock,
    Mutex,
    mpsc
};

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

use diesel::r2d2::{self, ConnectionManager}; 

use std::env; 

// NEAR indexer node deps
use anyhow::Result;
use clap::Clap; 

use configs::{init_logging, Opts, SubCommand};

mod configs;


type ClientId = u128; 

#[derive(Deserialize, Debug)]
enum IncomingMessage {
    NodeSubscription(NodeSubscriptionMessage), 
    NodeDesubscription(NodeDesubscriptionMessage), 
    // temporary (mocking all the blockchain stuff away) 
    NodeCreation(NodeCreationMessage), 
    FlowCreation(FlowCreationMessage), 
    // bidirectional
    NodeRemoval(NodeRemovalMessage)
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
struct NodeRemovalMessage {
    node_id: NodeId
}

#[derive(Serialize, Debug)] 
enum OutgoingMessage {
    SevenSummits(SevenSummitsMessage),
    NodeUpdate(NodeUpdateMessage), 
    FlowUpdate(FlowUpdateMessage), 
    NodeRemoval(NodeRemovalMessage),
}

#[derive(Serialize, Debug)] 
struct SevenSummitsMessage {
    node_ids: Vec<NodeId>
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct NodeUpdateMessage {
    id: NodeId, 
    title: Option<String>, 
    notes: Option<String>, 
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
    pub fn unsubscribe_all_from_node(&mut self, node_id: NodeId) {
        match self.by_nodes.get_mut(&node_id) {
            Some(client_ids) => {
                for client_id in client_ids {
                    match self.by_clients.get_mut(&client_id) {
                        Some(node_ids) => {
                            node_ids.retain(|i| !i.eq(&node_id))
                        }, 
                        None => ()
                    } 
                }
            }, 
            None => ()
        }
        self.by_nodes.remove(&node_id);
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

async fn listen_blocks(mut stream: mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        info!(
            target: "indexer_example",
            "#{} {} Shards: {}, Transactions: {}, Receipts: {}, ExecutionOutcomes: {}",
            streamer_message.block.header.height,
            streamer_message.block.header.hash,
            streamer_message.shards.len(),
            streamer_message.shards.iter().map(|shard| 
                if let Some(chunk) = &shard.chunk { 
                    chunk.transactions.len() 
                } else { 
                    0usize 
                }
            ).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| 
                if let Some(chunk) = &shard.chunk {
                    chunk.receipts.len() 
                } else { 
                    0usize 
                }
            ).sum::<usize>(),
            streamer_message.shards.iter().map(|shard| shard.receipt_execution_outcomes.len()).sum::<usize>(),
        );
    }
}

type DbPool = Arc<r2d2::Pool<ConnectionManager<PgConnection>>>;

fn main() {
    dotenv::dotenv().ok(); 

    openssl_probe::init_ssl_cert_env_vars();
    init_logging();

    let opts: Opts = Opts::parse();

    let home_dir =
        opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));

    match opts.subcmd {
        SubCommand::Run(args) => {
            println!("debug level {}", args.debug_level); 

            // prepare aggregator 
            println!("establishing connection to db"); 
            let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
            let db_manager = ConnectionManager::<PgConnection>::new(&db_url);
            let db_pool = Arc::new(r2d2::Pool::builder().build(db_manager).unwrap()); 

            let subs = Arc::new(RwLock::new(Subs::new())); 


            const PORT :u16 = 3031; 


            // prepare indexer
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::LatestSynced,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            };


            // run in parallel 
            let sys = actix::System::new(); 
            sys.block_on(async move {
                // run aggregator
                let routes = warp::path("v1")
                    .and(warp::ws()) 
                    .map(move |ws: warp::ws::Ws| {
                        let subs_clone = subs.clone();
                        let pool_clone = db_pool.clone();
                        ws.on_upgrade(move |socket| handle_ws_client(socket, subs_clone, pool_clone)) 
                    });
                println!("about to serve on port {}", PORT); 
                actix::spawn(warp::serve(routes).run(([127, 0, 0, 1], PORT)));

                // run indexer
                println!("running indexer"); 
                let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer();
                actix::spawn(listen_blocks(stream));
            });
            sys.run().unwrap(); 
        }, 
        SubCommand::Init(config) => {
            near_indexer::indexer_init_configs(&home_dir, config.into()).unwrap(); 
        }
    }
}

async fn handle_ws_client(
    websocket: WebSocket, 
    subs: Arc<RwLock<Subs>>, 
    db_pool: DbPool
) -> () {
    tokio::spawn(async move {
        println!("client connecting");

        let (mut sender, mut receiver) = websocket.split();

        send_seven_summits(&mut sender, db_pool.clone()).await;

        let client_id = subs.write().await.add_client(sender);  // maybe wrap sender into arc and access it directly. Locking the whole sub for sending messages is not good. Locking clients individually is what we want to be doing

        while let Some(body) = receiver.next().await {
            let message = match body {
                Ok(msg) => msg, 
                Err(e) => {
                    println!("error reading msg on websocket: {}", e); 
                    break; 
                }
            }; 
            let db_pool_clone = db_pool.clone();
            let subs_clone = subs.clone();
            handle_ws_message(message, subs_clone, db_pool_clone, client_id).await;
        }

        println!("client disconnected"); 
    });
}

async fn handle_ws_message(
    message: Message, 
    subs: Arc<RwLock<Subs>>, 
    db_pool: DbPool,
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
            handle_message(incoming_message, subs.clone(), db_pool, client_id).await
        }, 
        Err(err) => {
            println!("failed to json-parse message. {} {}", msg, err)
        }
    }
}

async fn handle_message(
    msg: IncomingMessage,
    subs: Arc<RwLock<Subs>>,
    db_pool: DbPool,
    client_id: ClientId
) {
    match msg { 
        IncomingMessage::NodeSubscription(sub) => {
            println!("client {} subscribing to node {}", client_id, sub.node_id);
            subs.write().await.subscribe(client_id, sub.node_id); 
            // lock client
            initial_update(client_id, db_pool, sub.node_id, subs.clone()).await;
            // release client
        }, 
        IncomingMessage::NodeDesubscription(desub) => {
            println!("client {} unsubscribing from node {}", client_id, desub.node_id);
            subs.write().await.unsubscribe(client_id, desub.node_id); 
        }, 
        IncomingMessage::NodeCreation(node_creation) => {
            println!("client {} creating node", client_id); 
            create_node(node_creation, subs, db_pool).await;
        },        
        IncomingMessage::FlowCreation(flow_creation) => {
            println!("client {} creating flow", client_id); 
            create_flow(flow_creation, subs, db_pool).await; 
        }, 
        IncomingMessage::NodeRemoval(node_removal) => {
            println!("client {} removing node", client_id); 
            remove_node(node_removal, subs, db_pool).await; 
        }
    }
}

async fn create_node(
    node_creation: NodeCreationMessage,
    subs: Arc<RwLock<Subs>>,
    db_pool: DbPool,
) {
    use schema::nodes::dsl::*; 

    let new_node = (
        id.eq(node_creation.id),
        title.eq(node_creation.title.clone()),
        notes.eq(node_creation.notes.clone())
    ); 

    match db_pool.get() {
        Ok(connection) => {
            diesel::insert_into(nodes)
                .values(&new_node)
                .execute(&connection).unwrap();

            let subs_w = subs.read().await; 

            let clients = subs_w.get_clients_by_node_id(node_creation.id); 

            for client in clients {
                let msg = OutgoingMessage::NodeUpdate(NodeUpdateMessage { 
                    id: node_creation.id, 
                    title: Some(node_creation.title.clone()), 
                    notes: Some(node_creation.notes.clone()), 
                });
                let json = serde_json::to_string(&msg).unwrap();

                client.lock().await.send(Message::text(json)).await.unwrap();
            }
        }, 
        Err(err) => {
            println!("could not get db connection from pool. {}", err)
        }
    }
}

async fn create_flow(
    flow_creation: FlowCreationMessage,
    subs: Arc<RwLock<Subs>>,
    db_pool: DbPool,
) {
    use schema::flows::dsl::*; 

    let new_flow = (
        from_id.eq(flow_creation.from_id),
        into_id.eq(flow_creation.into_id),
        notes.eq(flow_creation.notes.clone()), 
        share.eq(flow_creation.share)
    ); 

    match db_pool.get() {
        Ok(connection) => {
            let result = diesel::insert_into(flows)
                .values(&new_flow)
                .execute(&connection); 

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
        }, 
        Err(err) => {
            println!("error getting connection from pool. {}", err)
        }
    }
}

async fn remove_node(
    node_removal: NodeRemovalMessage,
    subs: Arc<RwLock<Subs>>,
    db_pool: DbPool,
) {
    use schema::flows::dsl::*; 
    use schema::nodes::dsl::*; 

    match db_pool.get() {
        Ok(connection) => {
            diesel::delete(flows)
                .filter(from_id.eq(node_removal.node_id).or(
                        into_id.eq(node_removal.node_id)))
                .execute(&connection)
                .unwrap();

            let result = diesel::delete(nodes.find(node_removal.node_id))
                .execute(&connection);

            match result {
                Ok(..) => {
                    {
                        let subs_r = subs.read().await; 

                        let mut futures = vec![]; 

                        for client in subs_r.get_clients_by_node_id(node_removal.node_id) {
                            let future = send_remove_node_message(
                                node_removal.clone(), 
                                client.into()
                            ); 
                            futures.push(future); 
                        }
                        future::join_all(futures).await;
                    }
                    {
                        let mut subs_w = subs.write().await; 
                        subs_w.unsubscribe_all_from_node(node_removal.node_id); 
                    }
                }, 
                Err(err) => {
                    println!("could not delete node, {}", err) 
                }
            }
        }, 
        Err(err) => {
            println!("error getting connection from pool. {}", err)
        }
    }

}

async fn send_remove_node_message(
    node_removal: NodeRemovalMessage, 
    client: Arc<Mutex<Client>>
) -> Result<(), warp::Error> {
    let msg = OutgoingMessage::NodeRemoval(node_removal); 
    println!("sending node removal to client") ;
    let json = serde_json::to_string(&msg).unwrap();
    client.lock().await.send(Message::text(json)).await
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

    for client in flow_clients {
        let msg = OutgoingMessage::FlowUpdate(FlowUpdateMessage { 
            from_id: flow_creation.from_id, 
            into_id: flow_creation.into_id, 
            notes: Some(flow_creation.notes.clone()), 
            share: Some(flow_creation.share)
        });
        let json = serde_json::to_string(&msg).unwrap();

        match client.lock().await.send(Message::text(json)).await {
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
    db_pool: DbPool, 
    node_id: NodeId, 
    subs: Arc<RwLock<Subs>>
) {
    use schema::nodes::dsl::*; 

    match db_pool.get() {
        Ok(connection) => {
            let result = nodes.find(node_id)
                .first::<Node>(&connection);
            match result {
                Ok(node) => {
                    send_initial_updates(
                        node, 
                        client_id, 
                        connection, 
                        subs
                    ).await; 
                }, 
                Err(err) => {
                    println!("could not find node {} in db: {}", node_id, err); 
                }
            }
        }, 
        Err(err) => {
            println!("could not get db connection from pool. {}", err)
        }
    }
}

async fn send_initial_updates(
    node: Node, 
    client_id: ClientId, 
    connection: r2d2::PooledConnection<ConnectionManager<PgConnection>>, 
    subs: Arc<RwLock<Subs>>
) {
    use schema::flows::dsl::*; 

    println!("sending initial update for node {}", node.title); 
    let flows_into = flows.filter(from_id.eq(node.id))
        .load::<Flow>(&connection).unwrap(); 
    let flows_from = flows.filter(into_id.eq(node.id))
        .load::<Flow>(&connection).unwrap(); 
    
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
}

async fn send_seven_summits(sender: &mut Client, db_pool: DbPool) {
    use schema::nodes::dsl::*; 

    match db_pool.get() {
        Ok(connection) => {
            let result = nodes.limit(7).load::<Node>(&connection); 

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
        }, 
        Err(err) => {
            println!("could not get db connection from pool. {}", err)
        }
    }
}

