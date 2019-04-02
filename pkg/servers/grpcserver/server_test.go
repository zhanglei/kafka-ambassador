package grpcserver

import (
	"context"
	"errors"
	"github.com/anchorfree/data-go/pkg/ambassador/pb"
	"github.com/anchorfree/kafka-ambassador/pkg/kafka_mock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListTopics(t *testing.T) {
	mp := &kafka_mock.MockedProducer{}
	topics := []string{"test", "test2", "another-one"}
	mp.On("ListTopics").Return(topics, nil)
	s := Server{
		Producer: mp,
	}
	listResp, err := s.ListTopics(context.Background(), &pb.Empty{})
	assert.NoErrorf(t, err, "ListTopics returned error: %v", err)
	assert.NotNil(t, listResp)
	respTopics := []string{}
	for _, t := range listResp.Topics {
		respTopics = append(respTopics, t)
	}
	assert.Equal(t, topics, respTopics)
}

func TestListTopicsFail(t *testing.T) {
	mp := &kafka_mock.MockedProducer{}
	var respTopics, emptyTopics []string
	mp.On("ListTopics").Return(emptyTopics, errors.New("Could not fetch topics"))
	s := Server{
		Producer: mp,
	}
	listResp, err := s.ListTopics(context.Background(), &pb.Empty{})
	assert.Errorf(t, err, "ListTopics returned error: %v", err)
	for _, t := range listResp.Topics {
		respTopics = append(respTopics, t)
	}
	assert.Empty(t, respTopics)
	mp.AssertExpectations(t)
}
