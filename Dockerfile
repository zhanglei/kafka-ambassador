FROM golang:1.12


# install rdkafka
RUN wget http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.0.0_1.0.1t-1+deb8u11_amd64.deb -O libssl.deb \
    && dpkg -i libssl.deb
RUN wget -qO - https://packages.confluent.io/deb/5.2/archive.key | apt-key add -
RUN echo "deb [arch=amd64] http://packages.confluent.io/deb/5.2 stable main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y librdkafka1 librdkafka-dev

ENV BASE_DIR /go/src/github.com/anchorfree/kafka-ambassador

ADD . ${BASE_DIR}
RUN cd ${BASE_DIR} \
    && go test ./... \
    && go build -o /go/bin/kaffka-ambassador

FROM ubuntu:16.04
COPY --from=0 /go/bin/kaffka-ambassador /bin/kafka-ambassador
RUN apt-get update && apt-get install -y wget software-properties-common python-software-properties
#RUN apt-get update && apt-get install -y wget
RUN wget -qO - https://packages.confluent.io/deb/5.2/archive.key | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.2 stable main"
RUN apt-get install -y apt-transport-https
RUN apt-get update
RUN apt install -y librdkafka1 librdkafka-dev
