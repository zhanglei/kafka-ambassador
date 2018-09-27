package servers

import (
	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/grpcserver"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/httpserver"
	"github.com/anchorfree/kafka-ambassador/pkg/servers/monitoring"
)

const (
	grpcPath       = "server.grpc"
	httpPath       = "server.http"
	monitoringPath = "server.monitoring"
)

type T struct {
	server.T
	Servers []server.I
}

func (s *T) Start() {
	if s.Config.IsSet(monitoringPath + ".listen") {
		monitSrv := &monitoring.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Logger:     s.Logger,
			Wg:         s.Wg,
		}
		go monitSrv.Start(monitoringPath)
		s.Servers = append(s.Servers, monitSrv)
	}
	if s.Config.IsSet(httpPath + ".listen") {
		httpSrv := &httpserver.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Logger:     s.Logger,
			Wg:         s.Wg,
		}
		go httpSrv.Start(httpPath)
		s.Servers = append(s.Servers, httpSrv)
	}
	if s.Config.IsSet(grpcPath + ".listen") {
		grpcSrv := &grpcserver.Server{
			Producer:   s.Producer,
			Config:     s.Config,
			Prometheus: s.Prometheus,
			Logger:     s.Logger,
			Wg:         s.Wg,
		}
		go grpcSrv.Start(grpcPath)
		s.Servers = append(s.Servers, grpcSrv)
	}
}

func (s *T) Stop() {
	for i, _ := range s.Servers {
		s.Servers[i].Stop()
	}
}
