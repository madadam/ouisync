FROM rust:slim-trixie AS chef

RUN cargo install cargo-chef --locked

WORKDIR /usr/local/src/ouisync

################################################################################
FROM chef AS planner

COPY --from=source . .
RUN cargo chef prepare --recipe-path recipe.json

################################################################################
FROM chef AS builder

COPY --from=planner /usr/local/src/ouisync/recipe.json recipe.json

RUN cargo chef cook \
        --package ouisync-cli \
        --release \
        --no-default-features \
        --recipe-path recipe.json

COPY --from=source . .

RUN cargo build \
        --package ouisync-cli \
        --release \
        --no-default-features

################################################################################
FROM debian:trixie-slim AS runner

RUN apt-get update && apt-get upgrade -y && \
    apt-get -y install iproute2

COPY --from=builder /usr/local/src/ouisync/target/release/ouisync /usr/local/bin/

COPY run.sh /usr/bin/
RUN chmod +x /usr/bin/run.sh

CMD ["run.sh"]
