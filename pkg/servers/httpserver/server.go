package httpserver

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/anchorfree/kafka-ambassador/pkg/server"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	// HTTP headers used by the API.
	hdrContentLength = "Content-Length"
	hdrContentType   = "Content-Type"
	hdrKafkaPrefix   = "X-Kafka-"

	prmTopic = "topic"
)

type Server server.T

func (s *Server) Start(configPath string) {
	r := mux.NewRouter()
	r.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), s.messageHandler)

	c := s.Config.Sub(configPath)
	addr := c.GetString("listen")
	httpServer := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			s.Logger.Errorf("Unable to start http server: %v", err)
		}
	}()

	s.Wg.Add(1)
	s.Logger.Info("Registered HTTP server in servers pool")
	s.Wg.Wait()
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	httpServer.Shutdown(ctx)
	close(s.Done)
}

func (s *Server) messageHandler(w http.ResponseWriter, r *http.Request) {
	var msg []byte
	var err error

	topic := mux.Vars(r)[prmTopic]
	if msg, err = readMsg(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.Producer.Send(topic, msg)
}

func readMsg(r *http.Request) ([]byte, error) {
	contentType := r.Header.Get(hdrContentType)
	if contentType == "text/plain" || contentType == "application/json" {
		if _, ok := r.Header[hdrContentLength]; !ok {
			return nil, errors.Errorf("missing %s header", hdrContentLength)
		}
		messageSizeStr := r.Header.Get(hdrContentLength)
		msgSize, err := strconv.Atoi(messageSizeStr)
		if err != nil {
			return nil, errors.Errorf("invalid %s header: %s", hdrContentLength, messageSizeStr)
		}
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read message")
		}
		if len(msg) != msgSize {
			return nil, errors.Errorf("message size does not match %s: expected=%v, actual=%v",
				hdrContentLength, msgSize, len(msg))
		}
		return msg, nil
	}
	return nil, errors.Errorf("unsupported content type %s", contentType)
}

func (s *Server) Stop() {
	s.Logger.Info("Stopping HTTP server")
	s.Wg.Done()
	<-s.Done
	s.Logger.Info("Stopped HTTP server")
}
