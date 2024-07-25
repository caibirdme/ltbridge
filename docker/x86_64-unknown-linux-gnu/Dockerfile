FROM debian:bookworm-slim

WORKDIR /app

COPY ./target/x86_64-unknown-linux-gnu/release/ltbridge ./ltbridge

ENTRYPOINT ["./ltbridge"]