use actix::prelude::*;
use rand::{self, rngs::ThreadRng, Rng};

// use diesel::pg::PgConnection;
// use diesel::r2d2::{self, ConnectionManager}; 

use std::collections::{HashMap, HashSet};

use crate::types::*;

use serde::Serialize; 
use serde_with::skip_serializing_none; 


#[derive(Serialize, Debug)] 
enum Outgoing {
    SevenSummits(SevenSummits),
    NodeUpdate(NodeUpdate), 
    FlowUpdate(FlowUpdate), 
    NodeRemoval(NodeRemoval),
    FlowRemoval(FlowRemoval), 
}

#[derive(Serialize, Debug)] 
struct SevenSummits {
    node_ids: Vec<NodeId>
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct NodeUpdate {
    id: NodeId, 
    title: Option<String>, 
    notes: Option<String>, 
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct NodeRemoval {
    id: NodeId
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct FlowUpdate {
    from_id: NodeId, 
    into_id: NodeId,
    notes: Option<String>, 
    share: Option<f32>
}

#[skip_serializing_none]
#[derive(Serialize, Debug)] 
struct FlowRemoval {
    from_id: NodeId, 
    into_id: NodeId
}

/// Chat server sends this messages to session
#[derive(Message)]
#[rtype(result = "()")]
pub struct Message(pub String);

/// Message for chat server communications

/// New chat session is created
#[derive(Message)]
#[rtype(usize)]
pub struct Connect {
    pub addr: Recipient<Message>,
}

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub session_id: usize,
}

/// Desubscribe from node updates
#[derive(Message)]
#[rtype(result = "()")]
pub struct SubscribeToNode {
    pub session_id: usize, 
    pub node_id: String
}

/// Join room, if room does not exists create new one.
#[derive(Message)]
#[rtype(result = "()")]
pub struct DesubscribeFromNode {
    pub session_id: usize,
    pub node_id: String,
}

/// session. implementation is super primitive
pub struct SummitsServer {
    sessions: HashMap<usize, Recipient<Message>>,
    by_nodes: HashMap<NodeId, HashSet<usize>>, 
    by_sessions: HashMap<usize, HashSet<NodeId>>, 
    rng: ThreadRng,
}

impl SummitsServer {
    pub fn new() -> SummitsServer {
        SummitsServer {
            sessions: HashMap::new(),
            by_nodes: HashMap::new(), 
            by_sessions: HashMap::new(), 
            rng: rand::thread_rng()
        }
    }
}

impl SummitsServer {
    // /// Send message to all users in the room
    // fn send_message(&self, room: &str, message: &str, skip_id: usize) {
    //     if let Some(sessions) = self.rooms.get(room) {
    //         for id in sessions {
    //             if *id != skip_id {
    //                 if let Some(addr) = self.sessions.get(id) {
    //                     let _ = addr.do_send(Message(message.to_owned()));
    //                 }
    //             }
    //         }
    //     }
    // }
    //
    pub fn send_seven_summits(&self) {
    }

    pub fn distribute_node_update(&self) {
    }

    pub fn distribute_node_deletion(&self) {
    }

    pub fn distribute_flow_update(&self) {
    }

    pub fn distribute_flow_deletion(&self) {
    }
}

/// Make actor from `ChatServer`
impl Actor for SummitsServer {
    /// We are going to use simple Context, we just need ability to communicate
    /// with other actors.
    type Context = Context<Self>;
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<Connect> for SummitsServer {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        // register session with random id
        let id = self.rng.gen::<usize>();
        self.sessions.insert(id, msg.addr);

        self.send_seven_summits();

        // send id back
        id
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for SummitsServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        // remove address
        if self.sessions.remove(&msg.session_id).is_some() {
            self.by_sessions.remove(&msg.session_id); 
            for (_, sessions) in &mut self.by_nodes {
                sessions.remove(&msg.session_id); 
            }
        }
    }
}

/// Join room, send disconnect message to old room
/// send join message to new room
impl Handler<SubscribeToNode> for SummitsServer {
    type Result = ();

    fn handle(&mut self, msg: SubscribeToNode, _: &mut Context<Self>) {
        let SubscribeToNode { session_id, node_id} = msg;

        self.by_nodes.entry(node_id.clone())
            .or_insert_with(HashSet::new)
            .insert(session_id.clone()); 

        self.by_sessions.entry(session_id.clone()) 
            .or_insert_with(HashSet::new)
            .insert(node_id.clone()); 
    }
}

/// Handler for Message message.
impl Handler<DesubscribeFromNode> for SummitsServer {
    type Result = ();

    fn handle(&mut self, msg: DesubscribeFromNode, _: &mut Context<Self>) {
        let DesubscribeFromNode { session_id, node_id } = msg;
        if let Some(sessions) = self.by_nodes.get_mut(&node_id) {
            sessions.remove(&session_id); 
        }
        if let Some(nodes) = self.by_sessions.get_mut(&session_id) {
            nodes.remove(&node_id);
        }
    }
}

//     async fn send_seven_summits(
//         &self
//     ) {
//         use schema::nodes::dsl::*; 
// 
//         match self.db_pool.get() {
//             Ok(connection) => {
//                 let result = nodes.limit(7).load::<Node>(&connection); 
// 
//                 match result {
//                     Ok(rows) => {
//                         let msg = messages::Outgoing::SevenSummits(SevenSummits {
//                             node_ids: rows.iter().map(|n| n.id).collect()
//                         }); 
//                         let txt = serde_json::to_string(&msg).unwrap(); 
//                         // sender.send(Message::text(txt)).await.unwrap();
//                     }, 
//                     Err(err) => {
//                         println!("faild to get 7 summits from db:{}", err) 
//                     }
//                 }
//             }, 
//             Err(err) => {
//                 println!("could not get db connection from pool. {}", err)
//             }
//         }
//     }
//
//     async fn initial_update(
//         &self, 
//         node_id: NodeId, 
//     ) {
//         use schema::nodes::dsl::*; 
// 
//         match self.db_pool.get() {
//             Ok(connection) => {
//                 let result = nodes.find(node_id)
//                     .first::<Node>(&connection);
//                 match result {
//                     Ok(node) => {
//                         self.send_initial_updates(
//                             node
//                         ).await; 
//                     }, 
//                     Err(err) => {
//                         println!("could not find node {} in db: {}", node_id, err); 
//                     }
//                 }
//             }, 
//             Err(err) => {
//                 println!("could not get db connection from pool. {}", err)
//             }
//         }
//     }
// 
//     async fn send_initial_updates(
//         &self, 
//         node: Node, 
//     ) {
//         use schema::flows::dsl::*; 
// 
//         if let Ok(connection) = self.db_pool.get() {
// 
//             println!("sending initial update for node {}", node.title); 
//             let flows_into = flows.filter(from_id.eq(node.id))
//                 .load::<Flow>(&connection).unwrap(); 
//             let flows_from = flows.filter(into_id.eq(node.id))
//                 .load::<Flow>(&connection).unwrap(); 
//             
//             
//             let flow_updates = flows_from.iter().chain(flows_into.iter()); 
// 
//             let msg = messages::Outgoing::NodeUpdate(messages::NodeUpdate{
//                 id: node.id, 
//                 title: Some(node.title), 
//                 notes: Some(node.notes),
//             });
//             let json = serde_json::to_string(&msg).unwrap();
// 
//             c.lock().await.send(Message::text(json)).await.unwrap();
// 
//             for flow in flow_updates {
//                 let msg = messages::Outgoing::FlowUpdate(messages::FlowUpdate{
//                     from_id: flow.from_id, 
//                     into_id: flow.into_id, 
//                     notes: Some(flow.notes.clone()), 
//                     share: Some(flow.share)
//                 }); 
//                 let txt = serde_json::to_string(&msg).unwrap();
//                 c.lock().await.send(Message::text(txt)).await.unwrap();
//             }
// 
//         }
//     }
// 
//     async fn send_seven_summits(
//         &self
//     ) {
//         use schema::nodes::dsl::*; 
// 
//         match self.db_pool.get() {
//             Ok(connection) => {
//                 let result = nodes.limit(7).load::<Node>(&connection); 
// 
//                 match result {
//                     Ok(rows) => {
//                         let msg = messages::Outgoing::SevenSummits(SevenSummits {
//                             node_ids: rows.iter().map(|n| n.id).collect()
//                         }); 
//                         let txt = serde_json::to_string(&msg).unwrap(); 
//                         // sender.send(Message::text(txt)).await.unwrap();
//                     }, 
//                     Err(err) => {
//                         println!("faild to get 7 summits from db:{}", err) 
//                     }
//                 }
//             }, 
//             Err(err) => {
//                 println!("could not get db connection from pool. {}", err)
//             }
//         }
//     }
