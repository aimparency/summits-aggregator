/** This is a server for summits. 
 * It allows clients to subscribe to node updates. 
 * And it will also aggregate state from the near blockchain. 
 */

use std::sync::Arc; 

#[macro_use]
extern crate diesel; 
use dotenv; 

use diesel::pg::PgConnection;

use diesel::r2d2::{self, ConnectionManager}; 

use std::env; 

use actix::prelude::*;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};

mod actix_web_actors;
use actix_web_actors::ws;

use std::time::Instant;

// NEAR indexer node deps
use anyhow::Result;
use clap::Clap; 

use configs::{init_logging, Opts, SubCommand};

mod configs;

// local 
mod wssession; 
mod wsserver; 
mod listener; 
mod schema; 
mod types; 
mod models; 

type DbPool = Arc<r2d2::Pool<ConnectionManager<PgConnection>>>;

async fn ws_index(
    r: HttpRequest,
    stream: web::Payload, 
    server: web::Data<Addr<wsserver::SummitsServer>>,
) -> Result<HttpResponse, Error> {
    println!("{:?}", r);
    let res = ws::start(
        wssession::SummitsSession {
            id: 0, 
            hb: Instant::now(), 
            addr: server.get_ref().clone(),
        }, 
        &r, 
        stream
    );
    println!("{:?}", res);
    res
}


fn main() {
    dotenv::dotenv().ok(); 

    openssl_probe::init_ssl_cert_env_vars();
    init_logging();

    let opts: Opts = Opts::parse();

    let home_dir =
        opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));

    match opts.subcmd {
        SubCommand::Run(args) => {
            println!("debug level {}", args.indexer_debug_level); 

            // prepare wss 
            println!("establishing connection to db"); 
            let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");
            let db_manager = ConnectionManager::<PgConnection>::new(&db_url);
            let db_pool = Arc::new(r2d2::Pool::builder().build(db_manager).unwrap()); 

            // prepare indexer
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::LatestSynced,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
            };


            // run in parallel 
            let sys = actix::System::new(); 
            sys.block_on(async move {
                // run wss 
                let sserver = Arc::new(wsserver::SummitsServer::new().start()); 
                let sserver_clone = sserver.clone();

                HttpServer::new(move || {
                    App::new()
                        .app_data(sserver_clone.clone())
                        .service(web::resource("/v1").route(web::get().to(ws_index)))
                })
                .bind("127.0.0.1:3031").unwrap()
                .run();

                // let routes = warp::path("v1")
                //     .and(warp::ws()) 
                //     .map(move |ws: warp::ws::Ws| {
                //         let subs_clone = subs.clone();
                //         let pool_clone = db_pool.clone();
                //         ws.on_upgrade(move |socket| handle_ws_client(socket, subs_clone, pool_clone)) 
                //     });
                // println!("about to serve on port {}", PORT); 
                // actix::spawn(warp::serve(routes.boxed()).run(([127, 0, 0, 1], PORT)));

                // run indexer
                println!("running indexer"); 
                let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
                let stream = indexer.streamer(); 

                actix::spawn(
                    listener::listen(
                        stream, 
                        sserver, 
                        args.indexer_debug_level
                    )
                )
            });

            sys.run().unwrap(); 
        }, 
        SubCommand::Init(config) => {
            near_indexer::indexer_init_configs(&home_dir, config.into()).unwrap(); 
        }
    }
}

