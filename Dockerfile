# syntax=docker/dockerfile:1
# Freshet image: the `freshet` CLI, the `freshet-serve` daemon and the
# `freshet-state-server` as static musl binaries on `scratch`, plus the CA bundle
# hosted state needs for HTTPS.
#
# Built natively per architecture (the release workflow runs one build on an amd64
# runner and one on an arm64 runner, then merges them into one multi-arch tag), so
# the musl build is never emulated.
#
#   docker build -t freshet .
#   docker run --rm -v "$PWD:/work" freshet plan --project models --state state.json
#   docker run --rm -p 8788:8788 -v "$PWD:/work" -e FRESHET_SERVE_TOKEN=<secret> \
#       --entrypoint freshet-serve freshet --project models --host 0.0.0.0 --watch
#   docker run --rm -p 8790:8790 -v freshet-state:/work/state -e FRESHET_STATE_SERVER_TOKEN=<secret> \
#       --entrypoint freshet-state-server freshet --dir state --host 0.0.0.0
#   # …and its console at http://localhost:8790/console/

# ---- builder -----------------------------------------------------------------
FROM rust:1-bookworm AS builder
# musl-gcc: ring (rustls' crypto, behind hosted state and `submit`) compiles C.
RUN apt-get update \
 && apt-get install -y --no-install-recommends musl-tools \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN set -eux; \
    target="$(uname -m)-unknown-linux-musl"; \
    rustup target add "$target"; \
    cargo build --release --locked --target "$target" \
        -p freshet-cli -p freshet-serve -p freshet-state-server \
        --features freshet-serve/http,freshet-serve/watch; \
    mkdir /out /state; \
    cp "target/$target/release/freshet" "target/$target/release/freshet-serve" \
        "target/$target/release/freshet-state-server" /out/

# ---- runtime -----------------------------------------------------------------
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/freshet /out/freshet-serve /out/freshet-state-server /usr/local/bin/
# The state server's directory, owned by the user it runs as. A named volume mounted
# here starts as a copy of it, ownership included, so a fresh one is writable; without
# it the volume is root's and the server cannot take its lock.
COPY --from=builder --chown=65532:65532 /state /work/state
# The console freshet-state-server serves at /console/: on by default here, and
# FRESHET_STATE_SERVER_CONSOLE= (empty) turns it off. Static files; nothing to build.
COPY apps/freshet-console /usr/share/freshet/console
ENV FRESHET_STATE_SERVER_CONSOLE=/usr/share/freshet/console
USER 65532:65532
WORKDIR /work
EXPOSE 8788 8790
ENTRYPOINT ["/usr/local/bin/freshet"]
CMD ["--help"]
