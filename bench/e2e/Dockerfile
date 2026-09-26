FROM rust:1.85-bookworm AS build
WORKDIR /src
COPY . .
RUN rustup target add wasm32-unknown-unknown \
    && cargo build --release --package kineplex-node --package kineplex-cli \
    && cargo build --manifest-path bench/e2e/wasm_filters/Cargo.toml \
       --release --target wasm32-unknown-unknown --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/kineplex-node /usr/local/bin/kineplex-node
COPY --from=build /src/target/release/kineplex-cli /usr/local/bin/kineplex-cli
ENTRYPOINT ["kineplex-node"]
