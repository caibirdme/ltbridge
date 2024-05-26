FROM ghcr.io/cross-rs/x86_64-unknown-linux-musl

ARG PB_URL="https://github.com/protocolbuffers/protobuf/releases/download/v25.1/protoc-25.1-linux-x86_64.zip"

RUN apt-get update && apt-get upgrade -y &&\
    apt-get install -y clang llvm libssl-dev curl unzip git &&\
    mkdir temp && cd temp &&\
    curl -LO $PB_URL &&\
    unzip protoc-25.1-linux-x86_64.zip && mv bin/protoc /usr/local/bin/ && protoc --version &&\
    cd .. && rm -rf temp
 