package httpserver

import (
	"bytes"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anchorfree/kafka-ambassador/pkg/kafka_mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestListTopics(t *testing.T) {
	mp := &kafka_mock.MockedProducer{}
	topics := []string{"test", "another", "extra-topic"}
	mp.On("ListTopics").Return(topics, nil)
	logger, _ := zap.NewProduction()
	s := Server{
		Producer: mp,
		Logger:   logger.Sugar(),
	}
	message := []byte("this is a test message to send")

	respRecorder := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/topics", bytes.NewReader(message))
	router := s.getRouter()
	router.ServeHTTP(respRecorder, req)

	respBytes, err := ioutil.ReadAll(respRecorder.Body)
	assert.Equalf(t, 200, respRecorder.Code, "non-200 code returned for ListTopics")
	assert.NoError(t, err)
	assert.Equal(t, strings.Join(topics, "\n"), string(respBytes))
}
