use chainhook_postgres::tokio_postgres::{self, Client, NoTls};
use chainhook_sdk::utils::Context;
use refinery::embed_migrations;

use crate::{config::Config, try_error, try_info};

embed_migrations!("../../migrations/ordinals");

async fn pg_run_migrations(pg_client: &mut Client, ctx: &Context) {
    try_info!(ctx, "Running postgres migrations");
    match migrations::runner()
        .set_migration_table_name("pgmigrations")
        .run_async(pg_client)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            try_error!(ctx, "Error running pg migrations: {}", e.to_string());
            std::process::exit(1);
        }
    };
    try_info!(ctx, "Postgres migrations complete");
}

pub async fn pg_connect(config: &Config, run_migrations: bool, ctx: &Context) -> Client {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .dbname(&config.ordinals_db.database)
        .host(&config.ordinals_db.host)
        .port(config.ordinals_db.port)
        .user(&config.ordinals_db.username);
    if let Some(password) = config.ordinals_db.password.as_ref() {
        pg_config.password(password);
    }

    try_info!(
        ctx,
        "Connecting to postgres at {}:{}",
        config.ordinals_db.host,
        config.ordinals_db.port
    );
    let mut pg_client: Client;
    loop {
        match pg_config.connect(NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("Postgres connection error: {}", e.to_string());
                        std::process::exit(1);
                    }
                });
                pg_client = client;
                break;
            }
            Err(e) => {
                try_error!(ctx, "Error connecting to postgres: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
    if run_migrations {
        pg_run_migrations(&mut pg_client, ctx).await;
    }
    pg_client
}
