pub mod pg_bigint_u32;
pub mod pg_numeric_u128;
pub mod pg_numeric_u64;
pub mod pg_smallint_u8;

use chainhook_sdk::utils::Context;
pub use tokio_postgres;

use tokio_postgres::{Client, Config, NoTls};

pub async fn pg_connect(
    dbname: &String,
    host: &String,
    port: u16,
    user: &String,
    password: Option<&String>,
    ctx: &Context,
) -> Result<Client, String> {
    let mut pg_config = Config::new();
    pg_config.dbname(dbname).host(host).port(port).user(user);
    if let Some(password) = password {
        pg_config.password(password);
    }
    match pg_config.connect(NoTls).await {
        Ok((client, connection)) => {
            let moved_ctx = ctx.clone();
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    moved_ctx.try_log(|l| slog::error!(l, "Postgres connection error: {e}"));
                }
            });
            Ok(client)
        }
        Err(e) => Err(format!("Error connecting to postgres: {e}"))
    }
}

pub async fn pg_connect_with_retry(
    dbname: &String,
    host: &String,
    port: u16,
    user: &String,
    password: Option<&String>,
    ctx: &Context,
) -> Client {
    loop {
        match pg_connect(dbname, host, port, user, password, ctx).await {
            Ok(client) => return client,
            Err(e) => {
                ctx.try_log(|l| slog::error!(l, "Error connecting to postgres: {e}"));
                std::thread::sleep(std::time::Duration::from_secs(1));
            },
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
