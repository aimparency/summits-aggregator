/** This is a server for summits. 
 * It allows clients to subscribe to node updates. 
 * And it will also aggregate state from the near blockchain. 
 */

use std::sync::Arc; 

#[macro_use]
extern crate diesel; 
use dotenv; 

use env_logger; 

use diesel::pg::PgConnection;

use diesel::r2d2::{self, ConnectionManager}; 

use std::env; 

use actix::prelude::*;
use actix_web::{middleware, web, App, Error, HttpRequest, HttpResponse, HttpServer};

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
    ws::start(
        wssession::SummitsSession {
            id: 0, 
            hb: Instant::now(), 
            addr: server.get_ref().clone(),
        }, 
        &r, 
        stream
    )
}


fn main() {
    env_logger::init();
    dotenv::dotenv().ok(); 

    // openssl_probe::init_ssl_cert_env_vars();
    //
    // init_logging();

    let opts: Opts = Opts::parse();

    let home_dir =
        opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));

    match opts.subcmd {
        SubCommand::Run(args) => {
            // prepare wss 
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
                        .wrap(middleware::Logger::default())
                        .wrap(middleware::Logger::new("%a %{User-Agent}i"))
                        .service(web::resource("/v1/").to(ws_index))
                })
                .bind("127.0.0.1:3031").unwrap()
                .run();


                // run indexer
                // println!("running indexer"); 
                // let indexer = near_indexer::Indexer::new(indexer_config).expect("Indexer::new()");
                // let stream = indexer.streamer(); 

                // actix::spawn(
                //     listener::listen(
                //         stream, 
                //         sserver, 
                //         args.indexer_debug_level
                //     )
                // )
            });

            sys.run().unwrap(); 
        }, 
        SubCommand::Init(config) => {
            near_indexer::indexer_init_configs(&home_dir, config.into()).unwrap(); 
        }
    }
}

