package main

import (
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func sendRequest(client *http.Client, addr string) {
	res, err := client.Get(addr)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		panic("request failed")
	}

	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	err = res.Body.Close()
	if err != nil {
		panic(err)
	}
}

func BenchmarkHTTP(b *testing.B) {
	brokers := []byte("127.0.0.1:9092")
	s := new(Server)
	s.Producer, _ = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	s.Run("127.0.0.1:8080")

	client := &http.Client{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sendRequest(client, "http://127.0.0.1:8080/topics/test/messages")
	}
}
