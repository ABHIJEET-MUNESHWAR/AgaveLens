# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1.89-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p agavelens-node

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 agavelens

WORKDIR /app
COPY --from=builder /app/target/release/agavelens-node /usr/local/bin/agavelens-node

USER agavelens
ENV AGAVELENS_HOST=0.0.0.0 \
    AGAVELENS_PORT=8080 \
    AGAVELENS_LOG_JSON=true \
    RUST_LOG=info

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/agavelens-node"]
# Default to the GraphQL analytics server; override with e.g. `docker run … analyze --slots 50000`.
CMD ["serve"]
