# kafka-ambassador

Kafka proxy to handle HTTP and gRPC messages into kafka stream

## Why another kafka proxy

There are available kafka proxies, such as:

- [kafka-pixy](https://github.com/mailgun/kafka-pixy)
- [kafka-gateway](https://github.com/moul/kafka-gateway)
- [Kafka REST proxy](https://docs.confluent.io/current/kafka-rest/docs/index.html)

Unfortunately these products, even though quite mature and good, doesn't satisfy our needs:

- High performance (we have around 60k msg/sec on one instance)
- Fallback to disk in case kafka is not available.
- always ready for a switch to Amazon Redshift or any other message queue.

This package doesn't solve following problems:

- Does not respect message order. In case message landed into WAL, we do not control message order at all.
- Support `consume` operations. We use `kafka-ambassador` as one way proxy to Kafka. Only produce actions are supported.

## Architecture

TBD

## Configuration

```yaml
server:
  http:
    listen: "0.0.0.0:18080"
  grpc:
    listen: "0.0.0.0:18282"
  monitoring:
    listen: "0.0.0.0:28080"
producer:
  cb:
    interval: 0
    timeout: "20s"
    fails: 5
    requests: 3
  resend:
    period: "33s"
    rate_limit: 10000
  wal:
    mode: "fallback"
    in_memory: false
    path: /data/wal
    always_topics:
      - always_wal
    disable_topics:
      - never_wal_topics
kafka:
  brokers:
    - "kafka.address.com:9092"
  # possible configuration parameters are:
  # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  "compression.codec": "gzip"
  "batch.num.messages": 100000
  "max.in.flight.requests.per.connection": 20
  "message.timeout.ms": 60000
  "socket.timeout.ms": 10000
```

### configuration reference table

| Parameter                           | Default value         | Description                                                                                                                              |
| ----------------------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| server.http.listen                  | NA                    | address to listen to for HTTP interface                                                                                                  |
| server.grpc.listen                  | NA                    | address to listen to for gRPC interface                                                                                                  |
| server.monitoring.listen            | NA                    | address for Prometheus exporter under /metrics, the same address is used for profiling.                                                  |
| producer.cb.interval                | 0                     | Zero counters of failures and successes every N duration. 0 means disable.                                                               |
| producer.cb.timeout                 | 20s                   | Switch to Half-Open after N seconds in Open state.                                                                                       |
| producer.cb.fails                   | 5                     | Switch to Open from Closed state if there was N consecutive errors.                                                                      |
| producer.cb.requests                | 3                     | Consecutive successes to switch to Closed state.                                                                                         |
| producer.resend.period              | 33s                   | Initiate resend from WAL every N duration. In case current state is Open - resend will be skipped.                                       |
| producer.resend.rate_limit          | 10000                 | Rate limit WAL reads.                                                                                                                    |
| producer.wal.always_topics          | []                    | Topics to use WAL even before sending to Kafka. Keep in mind, it slows down the response latency.                                        |
| producer.wal.disable_topics         | []                    | Topics to skip WAL even for fallback. e.g. you rely on message order.                                                                    |
| producer.wal.path                   | ""                    | Path to store WAL files.                                                                                                                 |
| producer.wal.mode                   | fallback              | Possible options: fallback (write to buffer only in case of failure), always (write to wal for all messages), disable (don't use buffer) |
| producer.wal.in_memory              | false                 | Possible options: false (write WAL data to disk), true (WAL data is stored in memory, in case of crash all data will be lost) |
| producer.old_producer_kill_timeout  | 10m                   | Time before old producer gets hard shutdown no mater there are still messages in queue                                                   |
| kafka.brokers                       | []                    |                                                                                                                                          |
| kafka.*                             | depends on librdkafka | https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md                                                                      |

## Run

### docker compose:

```
---
version: '2'
services:
  kafka-ambassador:
    image: anchorfree/kafka-ambassador:latest
    container_name: kafka-ambassador
    restart: always
    mem_limit: 4G
    stop_grace_period: 2m
    command:
      - /bin/kafka-ambassador
      - -config
      - /etc/kafka-ambassador/config.yaml
    volumes:
      - "/etc/kafka-ambassador:/etc/kafka-ambassador"
      - "/data/wal:/data/wal"
    ports:
      - 18080:18080
      - 19094:19094
      - 28080:28080
```

### kubernetes:

```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ambassador
  labels:
    app: kafka-ambassador
spec:
  replicas: 1
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 10%
  selector:
    matchLabels:
      name: ka
  template:
    metadata:
      labels:
        name: ka
    spec:
      imagePullSecrets:
      - name: dockercfg
      containers:
      - name: kafka-ambassador
        image: anchorfree/kafka-ambassador:latest
        args:
        - /bin/kafka-ambassador
        - -config
        - /etc/kafka-ambassador/config.yaml
        env:
        - name: TZ
          value: US/Pacific
        ports:
        - name: exporter-port
          containerPort: 28080
        - name: grpc-port
          containerPort: 19094
        volumeMounts:
        - name: kafka-ambassador-config
          mountPath: /etc/kafka-ambassador
        - name: ka-ula-wal
          mountPath: /data/wal
      volumes:
      - name: kafka-ambassador-config
        configMap:
          name: kafka-ambassador-config
      - name: ka-ula-wal
        hostPath:
          path: /data/ka-ula/wal
```
