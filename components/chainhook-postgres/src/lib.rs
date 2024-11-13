pub mod pg_bigint_u32;
pub mod pg_numeric_u128;
pub mod pg_numeric_u64;
pub mod pg_smallint_u8;

use std::future::Future;

use chainhook_sdk::utils::Context;
pub use tokio_postgres;

use tokio_postgres::{Client, Config, NoTls, Transaction};

pub struct PgConnectionConfig {
    pub dbname: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
}

/// Creates a short-lived connection and passes it into the given closure. Use this when you want to make sure the connection gets
/// closed once your work is complete.
pub async fn with_pg_connection<F, Fut, T>(
    config: &PgConnectionConfig,
    ctx: &Context,
    f: F,
) -> Result<T, String>
where
    F: FnOnce(Client) -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let client = pg_connect(config, ctx).await?;
    let result = f(client).await?;
    Ok(result)
}

/// Creates a new connection, opens a transaction, and passes it into the given closure. Use this when you want to run queries
/// within the confines of a single well defined transaction.
pub async fn with_pg_transaction<'a, F, Fut, T>(
    config: &PgConnectionConfig,
    ctx: &Context,
    f: F,
) -> Result<T, String>
where
    F: FnOnce(&'a Transaction<'a>) -> Fut,
    Fut: Future<Output = Result<T, String>> + 'a,
{
    let mut client = pg_connect(config, ctx).await?;
    let transaction = client
        .transaction()
        .await
        .map_err(|e| format!("unable to begin pg transaction: {e}"))?;
    {
        let transaction_ref: &'a Transaction<'a> = unsafe { std::mem::transmute(&transaction) };
        match f(transaction_ref).await {
            Ok(result) => {
                transaction
                    .commit()
                    .await
                    .map_err(|e| format!("unable to commit pg transaction: {e}"))?;
                Ok(result)
            }
            Err(e) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|e| format!("unable to rollback pg transaction: {e}"))?;
                Err(e)
            }
        }
    }
}

/// Connects to postgres and returns an open client.
pub async fn pg_connect(config: &PgConnectionConfig, ctx: &Context) -> Result<Client, String> {
    let mut pg_config = Config::new();
    pg_config
        .dbname(&config.dbname)
        .host(&config.host)
        .port(config.port)
        .user(&config.user);
    if let Some(password) = &config.password {
        pg_config.password(password);
    }
    match pg_config.connect(NoTls).await {
        Ok((client, connection)) => {
            let moved_ctx = ctx.clone();
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    moved_ctx.try_log(|l| slog::error!(l, "postgres connection error: {e}"));
                }
            });
            Ok(client)
        }
        Err(e) => Err(format!("error connecting to postgres: {e}")),
    }
}

/// Connects to postgres with infinite retries and returns an open client.
pub async fn pg_connect_with_retry(config: &PgConnectionConfig, ctx: &Context) -> Client {
    loop {
        match pg_connect(config, ctx).await {
            Ok(client) => return client,
            Err(e) => {
                ctx.try_log(|l| slog::error!(l, "error connecting to postgres: {e}"));
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
}

#[cfg(test)]
pub async fn pg_test_client() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres",
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("test connection error: {}", e);
        }
    });
    client
}

#[cfg(test)]
pub async fn pg_test_roll_back_migrations(pg_client: &mut tokio_postgres::Client) {
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
            Err(_) => unreachable!()
        };
}
