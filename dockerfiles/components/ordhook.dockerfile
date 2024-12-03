FROM rust:bullseye AS build

ARG GIT_COMMIT='0000000'

ENV GIT_COMMIT=${GIT_COMMIT}

WORKDIR /src

RUN apt-get update && apt-get install -y ca-certificates pkg-config libssl-dev libclang-11-dev libunwind-dev libunwind8 curl gnupg
RUN rustup update 1.80.1 && rustup default 1.80.1

RUN mkdir /out
COPY ./Cargo.toml /src/Cargo.toml
COPY ./Cargo.lock /src/Cargo.lock
COPY ./components/chainhook-postgres /src/components/chainhook-postgres
COPY ./components/ordhook-core /src/components/ordhook-core
COPY ./components/ordhook-cli /src/components/ordhook-cli
COPY ./migrations /src/migrations

RUN cargo build --features release --release
RUN cp /src/target/release/ordhook /out

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y ca-certificates libssl-dev libclang-11-dev libunwind-dev libunwind8 sqlite3
COPY --from=build /out/ordhook /bin/ordhook

WORKDIR /workspace

ENTRYPOINT ["ordhook"]
