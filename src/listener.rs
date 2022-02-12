use std::sync::Arc;

use actix::prelude::*;

use tracing::info;

use crate::wsserver;

use tokio::sync::mpsc;


pub async fn listen (
    mut stream: mpsc::Receiver<near_indexer::StreamerMessage>, 
    server_addr: Arc<Addr<wsserver::SummitsServer>>,
    debug_level: u8, 
) {
    while let Some(streamer_message) = stream.recv().await {
        // // send message to summit server
        // server_addr.do_send(wsserver::ClientMessage {
        //     id: self.id,
        //     msg,
        //     room: self.room.clone(),
        // })

        if debug_level > 3 {
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
                streamer_message.shards.iter().map(
                    |shard| shard.receipt_execution_outcomes.len()
                ).sum::<usize>(),
            );
        }
    }
}

//    // Nodes
//
//    async fn create_node(
//        &self
//    ) {
//        use schema::nodes::dsl::*; 
//
//        let new_node = (
//            id.eq(node_creation.id),
//            title.eq(node_creation.title.clone()),
//            notes.eq(node_creation.notes.clone())
//        ); 
//
//        match self.db_pool.get() {
//            Ok(connection) => {
//                diesel::insert_into(nodes)
//                    .values(&new_node)
//                    .execute(&connection).unwrap();
//
//                let subs_w = subs.read().await; 
//
//                let clients = subs_w.get_clients_by_node_id(node_creation.id); 
//
//                for client in clients {
//                    let msg = messages::Outgoing::NodeUpdate(NodeUpdate{ 
//                        id: node_creation.id, 
//                        title: Some(node_creation.title.clone()), 
//                        notes: Some(node_creation.notes.clone()), 
//                    });
//                    let json = serde_json::to_string(&msg).unwrap();
//
//                    client.lock().await.send(Message::text(json)).await.unwrap();
//                }
//            }, 
//            Err(err) => {
//                println!("could not get db connection from pool. {}", err)
//            }
//        }
//    }
//
//    async fn update_node(
//        &self
//    ) {
//        // TBD
//    }
//
//    async fn remove_node(
//        &self, 
//        node_removal: messages::NodeRemoval,
//        subs: Arc<RwLock<Subs>>,
//        db_pool: DbPool,
//    ) {
//        use schema::flows::dsl::*; 
//        use schema::nodes::dsl::*; 
//
//        match db_pool.get() {
//            Ok(connection) => {
//                diesel::delete(flows)
//                    .filter(from_id.eq(node_removal.node_id).or(
//                            into_id.eq(node_removal.node_id)))
//                    .execute(&connection)
//                    .unwrap();
//
//                let result = diesel::delete(nodes.find(node_removal.node_id))
//                    .execute(&connection);
//
//                match result {
//                    Ok(..) => {
//                        {
//                            let subs_r = subs.read().await; 
//
//                            let mut futures = vec![]; 
//
//                            for client in subs_r.get_clients_by_node_id(node_removal.node_id) {
//                                let future = send_remove_node_message(
//                                    node_removal.clone(), 
//                                    client.into()
//                                ); 
//                                futures.push(future); 
//                            }
//                            future::join_all(futures).await;
//                        }
//                        {
//                            let mut subs_w = self.subs.write().await; 
//                            subs_w.unsubscribe_all_from_node(node_removal.node_id); 
//                        }
//                    }, 
//                    Err(err) => {
//                        println!("could not delete node, {}", err) 
//                    }
//                }
//            }, 
//            Err(err) => {
//                println!("error getting connection from pool. {}", err)
//            }
//        }
//
//    }
//
//    async fn send_remove_node_message(
//        &self, 
//        node_removal: messages::NodeRemoval, 
//        client: Arc<Mutex<Client>>
//    ) -> Result<(), warp::Error> {
//        let msg = messages::Outgoing::NodeRemoval(node_removal); 
//        println!("sending node removal to client") ;
//        let json = serde_json::to_string(&msg).unwrap();
//        client.lock().await.send(Message::text(json)).await
//    }
//
//    // Flows
//    async fn create_flow(
//        &self, 
//        flow_creation: messages::FlowCreation,
//        subs: Arc<RwLock<Subs>>,
//        db_pool: DbPool,
//    ) {
//        use schema::flows::dsl::*; 
//
//        let new_flow = (
//            from_id.eq(flow_creation.from_id),
//            into_id.eq(flow_creation.into_id),
//            notes.eq(flow_creation.notes.clone()), 
//            share.eq(flow_creation.share)
//        ); 
//
//        match db_pool.get() {
//            Ok(connection) => {
//                let result = diesel::insert_into(flows)
//                    .values(&new_flow)
//                    .execute(&connection); 
//
//                match result {
//                    Ok(..) => {
//                        send_create_flow_updates(
//                            flow_creation, 
//                            subs, 
//                        ).await
//                    }, 
//                    Err(err) => {
//                        println!("DB error when creating flow {}", err)
//                    }
//                };
//            }, 
//            Err(err) => {
//                println!("error getting connection from pool. {}", err)
//            }
//        }
//    }
//
//    async fn send_create_flow_updates(
//        &self, 
//        flow_update: messages::FlowUpdate
//    ) {
//        let subs_w = subs.read().await; 
//        
//        let from_node_client_ids = subs_w.get_client_ids_by_node_id(flow_creation.from_id); 
//        let into_node_client_ids = subs_w.get_client_ids_by_node_id(flow_creation.into_id); 
//
//        let flow_client_ids = from_node_client_ids.into_iter().filter(|client_id| {
//            for i in into_node_client_ids.iter() {
//                if i.eq(client_id) {
//                    return false
//                } 
//            }
//            true
//        }).chain(into_node_client_ids.clone().into_iter()).collect();
//
//        let flow_clients = subs_w.get_clients_by_ids(flow_client_ids); 
//
//        for client in flow_clients {
//            let msg = messages::Outgoing::FlowUpdate(flow_update);
//            let json = serde_json::to_string(&msg).unwrap();
//
//            match client.lock().await.send(Message::text(json)).await {
//                Ok(..) => (), 
//                Err(..) => {
//                    // remove this client, its connection is probably closed
//                    // add this client to a list of to be cleaned clients
//                    ()
//                }
//            }
//        }
//    }
//
//    async fn update_flow(&self) {
//        // TBD
//    }
//
//    async fn remove_flow(&self) {
//        // TBD
//    }
