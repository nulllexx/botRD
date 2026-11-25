# botRD: one small native binary, nothing else.

FROM rust:1-slim-bookworm AS build
WORKDIR /src
# Dependencies first, against a stub main, so code-only changes reuse this layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src src
RUN touch src/main.rs && cargo build --release --locked

# ---- runtime: glibc and nothing else. No shell, no package manager.
# TLS roots are compiled in (webpki-roots), so no ca-certificates either.
FROM gcr.io/distroless/cc-debian12
COPY --from=build /src/target/release/botrd /usr/local/bin/botrd
ENV DATA_FILE=/data/data.json
VOLUME /data
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/botrd"]
