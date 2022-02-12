use std::time::{Duration, Instant};

use actix::prelude::*; 

use crate::actix_web_actors::ws;
use crate::wsserver; 
use crate::types::NodeId; 

use serde::Deserialize; 

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5); 
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30); 


#[derive(Deserialize, Debug)]
enum IncomingMessage {
    NodeSubscription(NodeSubscription), 
    NodeDesubscription(NodeDesubscription), 
}

#[derive(Deserialize, Debug)]
struct NodeSubscription {
    node_id: NodeId
}

#[derive(Deserialize, Debug)]
struct NodeDesubscription {
    node_id: NodeId
}

pub struct SummitsSession {
    /// unique session id
    pub id: usize,
    /// otherwise we drop connection.
    pub hb: Instant,
    /// summits server
    pub addr: Addr<wsserver::SummitsServer>
}

impl Actor for SummitsSession {

    type Context = ws::WebsocketContext<Self>;

    /// Method is called on actor start.
    /// We register ws session with SummitsServer 
    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);

        // register self in summits server. `AsyncContext::wait` register
        // future within context, but context waits until this future resolves
        // before processing any other events.
        // HttpContext::state() is instance of WsChatSessionState, state is shared
        // across all routes within application
        let addr = ctx.address();
        self.addr
            .send(wsserver::Connect {
                addr: addr.recipient(),
            })
            .into_actor(self)
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    // something is wrong with chat server
                    _ => ctx.stop(),
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify chat server
        self.addr.do_send(wsserver::Disconnect { session_id: self.id });
        Running::Stop
    }
}

/// Handle messages from chat server, we simply send it to peer websocket
impl Handler<wsserver::Message> for SummitsSession {
    type Result = ();

    fn handle(&mut self, msg: wsserver::Message, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

/// WebSocket message handler
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for SummitsSession {
    fn handle(
        &mut self,
        msg: Result<ws::Message, ws::ProtocolError>,
        ctx: &mut Self::Context,
    ) {
        let msg = match msg {
            Err(_) => {
                ctx.stop();
                return;
            }
            Ok(msg) => msg,
        };

        println!("websocket message: {:?}", msg);
        match msg {
            ws::Message::Ping(msg) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            ws::Message::Pong(_) => {
                self.hb = Instant::now();
            }
            ws::Message::Text(text) => {
                match serde_json::from_str(&text) {
                    Ok(incoming_message) => { 
                        match incoming_message { 
                            IncomingMessage::NodeSubscription(sub) => {
                                self.addr
                                    .do_send(wsserver::SubscribeToNode {
                                        session_id: self.id, 
                                        node_id: sub.node_id
                                    });
                            }, 
                            IncomingMessage::NodeDesubscription(desub) => {
                                self.addr
                                    .do_send(wsserver::DesubscribeFromNode {
                                        session_id: self.id, 
                                        node_id: desub.node_id
                                    });
                            }
                        }
                    }, 
                    Err(err) => {
                        println!("text isn't valid json of an incoming message. {} {}", text, err)
                    }
                }
            }
            ws::Message::Binary(_) => println!("Unexpected binary"),
            ws::Message::Close(reason) => {
                ctx.close(reason);
                ctx.stop();
            }
            ws::Message::Continuation(_) => {
                ctx.stop();
            }
            ws::Message::Nop => (),
        }
    }
}

impl SummitsSession {
    /// send ping to client every second.
    /// also check heartbeats from client. 
    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                println!("Websocket Client heartbeat failed, disconnecting!");

                // notify chat server
                act.addr.do_send(wsserver::Disconnect { session_id: act.id });

                // stop actor
                ctx.stop();

                // don't try to send a ping
                return;
            }

            ctx.ping(b"");
        });
    }
}
