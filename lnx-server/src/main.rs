mod auth;
mod error;
mod helpers;
mod responders;
mod routes;
mod state;
mod utils;

#[macro_use]
extern crate log;

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Result;
use engine::structures::IndexDeclaration;
use engine::{Engine, StorageBackend};
use fern::colors::{Color, ColoredLevelConfig};
use log::LevelFilter;
use poem::{Endpoint, EndpointExt, IntoResponse, Request, Response, Route, Server};
use poem::http::Method;
use poem::listener::TcpListener;
use poem::middleware::Cors;
use poem_openapi::{LicenseObject, OpenApiService};
use structopt::StructOpt;

use crate::auth::AuthManager;
use crate::state::State;

static STORAGE_PATH: &str = "./index/engine-storage";
static INDEX_KEYSPACE: &str = "persistent_indexes";

#[derive(Debug, StructOpt)]
#[structopt(name = "lnx", about = "A ultra-fast, adaptable search engine.")]
struct Settings {
    /// The log level filter, any logs that are above this level won't
    /// be displayed.
    #[structopt(long, default_value = "info", env)]
    log_level: LevelFilter,

    /// An optional bool to use ASNI colours for log levels.
    /// You probably want to disable this if using file-based logging.
    #[structopt(long, env)]
    pretty_logs: Option<bool>,

    /// The host to bind to (normally: '127.0.0.1' or '0.0.0.0'.)
    #[structopt(long, short, default_value = "127.0.0.1", env)]
    host: String,

    /// The port to bind the server to.
    #[structopt(long, short, default_value = "8000", env)]
    port: u16,

    /// Optional CORS allowed origins.
    ///
    /// Multiple origins can be defined by separating with a ','
    /// e.g. http://127.0.0.1:3000,http://foo.com
    #[structopt(long, default_value = "*", env)]
    cors_origins: String,

    /// Optional CORS allowed methods.
    ///
    /// Each method should be seperated by a ','
    /// e.g. GET,POST
    #[structopt(long, default_value = "*", env)]
    cors_methods: String,

    /// The super user key.
    ///
    /// If specified this will enable auth mode and require a token
    /// bearer on every endpoint.
    ///
    /// The super user key is used to make tokens with given permissions.
    #[structopt(long, short = "auth", env, hide_env_values = true)]
    super_user_key: Option<String>,

    /// The number of threads to use for the tokio runtime.
    ///
    /// If this is not set, the number of logical cores on the machine is used.
    #[structopt(long, short = "threads", env)]
    runtime_threads: Option<usize>,

    /// A optional file to send persistent logs.
    #[structopt(long, env)]
    log_file: Option<String>,

    /// If true this will stop logging each search request.
    #[structopt(long, env)]
    silent_search: bool,
}

fn main() {
    let settings = match setup() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error during server setup: {:?}", e);
            return;
        },
    };

    let threads = settings.runtime_threads.unwrap_or_else(|| num_cpus::get());
    info!("starting runtime with {} threads", threads);
    let maybe_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build();

    let result = match maybe_runtime {
        Ok(runtime) => runtime.block_on(start(settings)),
        Err(e) => {
            error!("error during runtime creation: {:?}", e);
            return;
        },
    };

    if let Err(e) = result {
        error!("error during server runtime: {:?}", e);
    }
}

fn setup_logger(
    level: LevelFilter,
    log_file: &Option<String>,
    pretty: bool,
) -> Result<()> {
    let mut colours = ColoredLevelConfig::new();

    if pretty {
        colours = colours
            .info(Color::Green)
            .warn(Color::Yellow)
            .error(Color::BrightRed)
            .debug(Color::Magenta)
            .trace(Color::Cyan);
    }

    let mut builder = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{} | {} | {:<5} - {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                colours.color(record.level()),
                message,
            ))
        })
        .level(level)
        .chain(std::io::stdout());

    if let Some(file) = log_file {
        builder = builder.chain(fern::log_file(file)?);
    }

    builder.apply()?;

    Ok(())
}

/// Parses the config and sets up logging
fn setup() -> Result<Settings> {
    let config: Settings = Settings::from_args();
    setup_logger(
        config.log_level,
        &config.log_file,
        config.pretty_logs.unwrap_or(true),
    )?;
    Ok(config)
}

async fn start(settings: Settings) -> Result<()> {
    let state = create_state(&settings).await?;

    let api_service = OpenApiService::new(
        (
        ),
        "Lnx API",
        env!("CARGO_PKG_VERSION")
        )
        .description(env!("CARGO_PKG_DESCRIPTION"))
        .server(format!("http://{}:{}", &settings.host, settings.port));

    let ui = api_service.redoc();
    let spec = api_service.spec();

    let mut cors = Cors::new();

    if settings.cors_origins != "*" {
        let origins = settings.cors_origins.split(",");
        let origins: Vec<String> = origins.map(String::from).collect();
        cors = cors.allow_origins(origins)
    }

    if settings.cors_methods != "*" {
        let methods = settings.cors_methods.split(",");
        let methods: Vec<Method> = methods
            .filter_map(|v| Method::from_str(v).ok())
            .collect();
        cors = cors.allow_methods(methods)
    }

    let app = Route::new()
        .nest("/", api_service)
        .nest("/ui", ui)
        .at("/spec", poem::endpoint::make_sync(move |_| spec.clone()))
        .with(cors.allow_credentials(true))
        .around(log)
        .data(state);

    Server::new(TcpListener::bind("127.0.0.1:8000"))
        .run_with_graceful_shutdown(
            app,
            async move {
                let _ = tokio::signal::ctrl_c().await;
            },
            Some(Duration::from_secs(2)),
        )
        .await?;

    Ok(())
}

async fn create_state(settings: &Settings) -> Result<State> {
    let storage = StorageBackend::connect(Some(STORAGE_PATH.to_string()))?;
    let engine = {
        info!("loading existing indexes...");
        let existing_indexes: Vec<IndexDeclaration>;
        if let Some(buff) = storage.load_structure(INDEX_KEYSPACE)? {
            let buffer: Vec<u8> = bincode::deserialize(&buff)?;
            existing_indexes = serde_json::from_slice(&buffer)?;
        } else {
            existing_indexes = vec![];
        }

        info!(
            " {} existing indexes discovered, recreating state...",
            existing_indexes.len()
        );

        let engine = Engine::new();
        for index in existing_indexes {
            engine.add_index(index, true).await?;
        }

        engine
    };

    let (enabled, key) = if let Some(ref key) = settings.super_user_key {
        (true, key.to_string())
    } else {
        (false, String::new())
    };

    let auth = AuthManager::new(enabled, key);

    Ok(State::new(engine, storage, auth, !settings.silent_search))
}


/// Logs any requests and their relevant responses.
async fn log<E: Endpoint>(next: E, req: Request) -> poem::Result<Response> {
    let method = req.method().clone();
    let path = req.uri().clone();

    let start = Instant::now();
    let res = next.call(req).await;
    let elapsed = start.elapsed();

    match res {
        Ok(r) => {
            let resp = r.into_response();

            info!(
                "{} -> {} {} [ {:?} ] - {:?}",
                method.as_str(),
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or(""),
                elapsed,
                path.path(),
            );

            Ok(resp)
        },
        Err(e) => {

            let resp = e.as_response();

            if resp.status().as_u16() >= 500 {
                error!("{}", &e);
            }

            info!(
                "{} -> {} {} [ {:?} ] - {:?}",
                method.as_str(),
                resp.status().as_u16(),
                resp.status().canonical_reason().unwrap_or(""),
                elapsed,
                path.path(),
            );

            Err(e)
        }
    }
}

