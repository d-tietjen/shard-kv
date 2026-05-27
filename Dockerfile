FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

ARG SHARDCACHE_FEATURES=redis-server
ARG RUSTFLAGS=
ENV RUSTFLAGS=${RUSTFLAGS}

COPY . .

RUN cargo build --locked --release -p shardcache --features "${SHARDCACHE_FEATURES}" --bin shardcache

FROM debian:bookworm-slim AS runtime

RUN groupadd --system shardcache \
    && useradd --system --gid shardcache --home-dir /var/lib/shardcache --create-home shardcache

COPY --from=builder /app/target/release/shardcache /usr/local/bin/shardcache

RUN mkdir -p /var/lib/shardcache \
    && chown -R shardcache:shardcache /var/lib/shardcache

USER shardcache

EXPOSE 6380 6501 6502 6503 6504

ENTRYPOINT ["shardcache"]
CMD ["--bind-addr", "0.0.0.0:6380", "--disable-persistence", "--server-mode", "direct"]
