package monitoring

import (
	"net/http"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server server.T

func (s *Server) Start(configPath string) {

	monitServer := &http.Server{
		Handler: promhttp.HandlerFor(s.Prometheus, promhttp.HandlerOpts{}),
		Addr:    s.Config.GetString(configPath + ".listen"),
	}

	// Start your http server for prometheus.
	if err := monitServer.ListenAndServe(); err != nil {
		s.Logger.Errorf("Unable to start a monitoring http server: %v", err)
	}
}

func (s *Server) Stop() {
	s.Logger.Info("Stopping monitoring server")
}
