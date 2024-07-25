FROM gcr.io/distroless/static-debian12

ARG TARGET="x86_64-unknown-linux-musl"

WORKDIR /app

COPY ./target/${TARGET}/release/ltbridge ./ltbridge

ENTRYPOINT ["./ltbridge"]