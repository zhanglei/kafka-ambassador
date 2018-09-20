FROM golang:1.11


# install drkafka
RUN echo "deb http://deb.debian.org/debian sid main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install -y librdkafka1 librdkafka-dev

ENV BASE_DIR /go/src/github.com/anchorfree/kafka-ambassador

ADD . ${BASE_DIR}
RUN cd ${BASE_DIR} && go build -o /go/bin/kaffka-ambassador


FROM ubuntu:16.04
COPY --from=0 /go/bin/kaffka-ambassador /bin/kafka-ambassador
RUN apt-get update && apt-get install -y wget software-properties-common python-software-properties
RUN wget -qO - https://packages.confluent.io/deb/5.0/archive.key | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.0 stable main"
RUN apt-get install -y apt-transport-https
RUN apt-get update
RUN apt install -y librdkafka1 librdkafka-dev
