package monitoring

import (
	"net/http"
	"net/http/pprof"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server server.T

func (s *Server) Start(configPath string) {
	promHandler := promhttp.HandlerFor(s.Prometheus, promhttp.HandlerOpts{})

	r := mux.NewRouter()
	r.Handle("/metrics", promHandler)

	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.PathPrefix("/debug/pprof").HandlerFunc(pprof.Index)

	monitServer := &http.Server{
		Handler: r,
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
