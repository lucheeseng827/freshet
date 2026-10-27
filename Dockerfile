# syntax=docker/dockerfile:1
# Freshet image: the `freshet` CLI and the `freshet-serve` daemon as static musl
# binaries on `scratch`, plus the CA bundle hosted state needs for HTTPS.
#
# Built natively per architecture (the release workflow runs one build on an amd64
# runner and one on an arm64 runner, then merges them into one multi-arch tag), so
# the musl build is never emulated.
#
#   docker build -t freshet .
#   docker run --rm -v "$PWD:/work" freshet plan --project models --state state.json
#   docker run --rm -p 8788:8788 -v "$PWD:/work" --entrypoint freshet-serve freshet \
#       --project models --host 0.0.0.0

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
        -p freshet-cli -p freshet-serve --features freshet-serve/http; \
    mkdir /out; \
    cp "target/$target/release/freshet" "target/$target/release/freshet-serve" /out/

# ---- runtime -----------------------------------------------------------------
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/freshet /out/freshet-serve /usr/local/bin/
USER 65532:65532
WORKDIR /work
EXPOSE 8788
ENTRYPOINT ["/usr/local/bin/freshet"]
CMD ["--help"]
