pub mod blocks;
pub mod cursor;
pub mod models;
pub mod ordinals_pg;

use chainhook_postgres::pg_connect_with_retry;

use chainhook_sdk::utils::Context;

use crate::{config::Config, core::meta_protocols::brc20::brc20_pg, try_info};

pub async fn migrate_dbs(config: &Config, ctx: &Context) -> Result<(), String> {
    {
        try_info!(ctx, "Running ordinals DB migrations");
        let mut pg_client = pg_connect_with_retry(&config.ordinals_db).await;
        ordinals_pg::migrate(&mut pg_client).await?;
    }
    if let (Some(brc20_db), true) = (&config.brc20_db, config.meta_protocols.brc20) {
        try_info!(ctx, "Running brc20 DB migrations");
        let mut pg_client = pg_connect_with_retry(&brc20_db).await;
        brc20_pg::migrate(&mut pg_client).await?;
    }
    Ok(())
}

#[cfg(test)]
pub fn pg_test_config() -> chainhook_postgres::PgConnectionConfig {
    chainhook_postgres::PgConnectionConfig {
        dbname: "postgres".to_string(),
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        search_path: None,
    }
}

#[cfg(test)]
pub fn pg_test_connection_pool() -> chainhook_postgres::deadpool_postgres::Pool {
    chainhook_postgres::pg_pool(&pg_test_config()).unwrap()
}

#[cfg(test)]
pub async fn pg_test_connection() -> chainhook_postgres::tokio_postgres::Client {
    chainhook_postgres::pg_connect(&pg_test_config()).await.unwrap()
}

#[cfg(test)]
pub async fn pg_test_clear_db(pg_client: &mut chainhook_postgres::tokio_postgres::Client) {
    match pg_client
        .batch_execute(
            "
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT typname FROM pg_type WHERE typtype = 'e' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())) LOOP
                    EXECUTE 'DROP TYPE IF EXISTS ' || quote_ident(r.typname) || ' CASCADE';
                END LOOP;
            END $$;",
        )
        .await {
            Ok(rows) => rows,
            Err(e) => {
                println!(
                    "error rolling back test migrations: {}",
                    e.to_string()
                );
                std::process::exit(1);
            }
        };
}

/// Drops DB files in a test environment.
#[cfg(test)]
pub fn drop_all_dbs(config: &Config) {
    let dir_path = &config.expected_cache_path();
    if dir_path.exists() {
        std::fs::remove_dir_all(dir_path).unwrap();
    }
}
