FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

ARG FAST_CACHE_FEATURES=server
ARG RUSTFLAGS=
ENV RUSTFLAGS=${RUSTFLAGS}

COPY . .

RUN cargo build --locked --release -p fast-cache --features "${FAST_CACHE_FEATURES}" --bin fast-cache-server

FROM debian:bookworm-slim AS runtime

RUN groupadd --system fast-cache \
    && useradd --system --gid fast-cache --home-dir /var/lib/fast-cache --create-home fast-cache

COPY --from=builder /app/target/release/fast-cache-server /usr/local/bin/fast-cache-server

RUN mkdir -p /var/lib/fast-cache \
    && chown -R fast-cache:fast-cache /var/lib/fast-cache

USER fast-cache

VOLUME ["/var/lib/fast-cache"]
EXPOSE 6380 6501 6502 6503 6504

ENTRYPOINT ["fast-cache-server"]
CMD ["--bind-addr", "0.0.0.0:6380", "--data-dir", "/var/lib/fast-cache"]
