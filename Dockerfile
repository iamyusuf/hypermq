# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin hypermq

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 hypermq \
    && useradd --system --uid 10001 --gid hypermq --no-create-home --shell /usr/sbin/nologin hypermq
COPY --from=build /src/target/release/hypermq /usr/local/bin/hypermq

# The configuration file is not baked into the image. Mount it at runtime, for example:
#   docker run -v "$PWD/config.yaml:/etc/hypermq/config.yaml:ro" -p 8080:8080 hypermq
ENV HYPERMQ_CONFIG=/etc/hypermq/config.yaml \
    HYPERMQ_ROLE=all \
    HYPERMQ_LOG_FORMAT=json
USER 10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/hypermq"]
