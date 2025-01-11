FROM --platform=$TARGETPLATFORM debian:bookworm-slim

WORKDIR /app

COPY ./target/aarch64-unknown-linux-gnu/release/ltbridge ./ltbridge

ENTRYPOINT ["./ltbridge"] 