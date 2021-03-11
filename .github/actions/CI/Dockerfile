FROM ubuntu:18.04

RUN DEBIAN_FRONTEND=noninteractive \
    TZ=Europe/Minsk \
    apt-get update && apt-get install -y curl unzip

RUN curl -L https://tarantool.io/CpzRHm/release/2.6/installer.sh | bash

RUN apt-get -y install tarantool

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
