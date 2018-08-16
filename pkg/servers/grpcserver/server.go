package grpcserver

import (
	"io"
	"net"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	pb "github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Server server.T

func (s *Server) Start(configPath string) {
	c := s.Config.Sub(configPath)
	addr := c.GetString("listen")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer(grpc.MaxMsgSize(s.Config.GetInt(configPath + ".max.request.size")))
	pb.RegisterKafkaAmbassadorServer(grpcSrv, s)

	log.Printf("Listening for GRPC requests on %s...\n", addr)

	go func() {
		if err = grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	s.Wg.Add(1)
}

func (s *Server) Produce(stream pb.KafkaAmbassador_ProduceServer) error {
	var res *pb.ProdRs
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		s.Producer.Send(&req.Topic, req.Message)
		res = &pb.ProdRs{StreamOffset: req.StreamOffset}
		err = stream.Send(res)
		if err != nil {
			log.Errorf("Could not stream (GRPC) to the client: %s", err)
			return err
		}
	}
}

func (s *Server) Stop() {

}
