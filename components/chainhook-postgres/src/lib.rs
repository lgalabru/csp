pub mod types;
pub mod utils;

use std::future::Future;

pub use deadpool_postgres;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Transaction};
pub use tokio_postgres;

use tokio_postgres::{Client, Config, NoTls, Row};

/// A Postgres configuration for a single database.
#[derive(Clone, Debug)]
pub struct PgConnectionConfig {
    pub dbname: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub search_path: Option<String>,
}

/// Creates a Postgres connection pool based on a single database config. You can then use this pool to create ad-hoc clients and
/// transactions for interacting with the database.
pub fn new_pg_connection_pool(config: &PgConnectionConfig) -> Result<Pool, String> {
    let mut pg_config = Config::new();
    pg_config
        .dbname(&config.dbname)
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .options(format!(
            "-csearch_path={}",
            config.search_path.as_ref().unwrap_or(&"public".to_string())
        ));
    if let Some(password) = &config.password {
        pg_config.password(password);
    }
    let manager = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Ok(Pool::builder(manager)
        .max_size(16)
        .build()
        .map_err(|e| format!("unable to build pg connection pool: {e}"))?)
}

/// Creates a short-lived client and passes it into the given closure. Takes the connection out of the given pool, so it may or
/// may not close once the given closure is complete.
pub async fn with_pg_client<F, Fut, T>(pool: &Pool, f: F) -> Result<T, String>
where
    F: FnOnce(Object) -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let client = pool
        .get()
        .await
        .map_err(|e| format!("unable to get pg client: {e}"))?;
    let result = f(client).await?;
    Ok(result)
}

/// Takes a connection from the given pool, opens a transaction, and passes it into the given closure. Use this when you want to
/// run queries within the confines of a single well defined transaction.
pub async fn with_pg_transaction<'a, F, Fut, T>(pool: &Pool, f: F) -> Result<T, String>
where
    F: FnOnce(&'a Transaction<'a>) -> Fut,
    Fut: Future<Output = Result<T, String>> + 'a,
{
    let mut client = pool
        .get()
        .await
        .map_err(|e| format!("unable to get pg client: {e}"))?;
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
pub async fn pg_connect(config: &PgConnectionConfig) -> Result<Client, String> {
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
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    println!("postgres connection error: {e}");
                }
            });
            Ok(client)
        }
        Err(e) => Err(format!("error connecting to postgres: {e}")),
    }
}

/// Connects to postgres with infinite retries and returns an open client.
pub async fn pg_connect_with_retry(config: &PgConnectionConfig) -> Client {
    loop {
        match pg_connect(config).await {
            Ok(client) => return client,
            Err(e) => {
                println!("error connecting to postgres: {e}");
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
}

/// Transforms a Postgres row into a model struct.
pub trait FromPgRow {
    fn from_pg_row(row: &Row) -> Self;
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
