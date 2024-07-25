FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY ./target/x86_64-unknown-linux-musl/release/ltbridge ./ltbridge

ENTRYPOINT ["./ltbridge"]