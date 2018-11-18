package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"
)

func (p *T) readyToTrip(c gobreaker.Counts) bool {
	p.Logger.Debugf("failures: %v, success: %v, requests: %v", c.ConsecutiveFailures, c.ConsecutiveSuccesses, c.Requests)
	return c.ConsecutiveFailures > p.Config.CBMaxFailures
}

func (p *T) setCBState(name string, from, to gobreaker.State) {
	switch to {
	case gobreaker.StateClosed:
		cbState.With(prometheus.Labels{"name": name, "state": "closed"}).Inc()
	case gobreaker.StateHalfOpen:
		cbState.With(prometheus.Labels{"name": name, "state": "half"}).Inc()
	case gobreaker.StateOpen:
		cbState.With(prometheus.Labels{"name": name, "state": "open"}).Inc()
	}
	p.Logger.Infof("%s CB state changed from: [%s] to: [%s]", name, from, to)
}

func (p *T) trackCBState(period time.Duration) {
	var ticker *time.Ticker
	if p.Config.ResendPeriod != 0 {
		ticker = time.NewTicker(p.Config.ResendPeriod)

		for range ticker.C {
			switch s := p.cb.State(); s {
			case gobreaker.StateClosed:
				cbCurrentState.Set(0)
			case gobreaker.StateHalfOpen:
				cbCurrentState.Set(0.5)
			case gobreaker.StateOpen:
				cbCurrentState.Set(1)
			}
		}
	}
}

func (p *T) cbClose() {
	switch p.cb.State() {
	case gobreaker.StateOpen:
		time.Sleep(time.Second * p.Config.CBTimeout)
		for i := uint32(0); i < p.Config.CBMaxRequests; i++ {
			success, err := p.cb.Allow()
			if err == nil {
				success(true)
			}
		}
	case gobreaker.StateHalfOpen:
		for i := uint32(0); i < p.Config.CBMaxRequests; i++ {
			success, err := p.cb.Allow()
			if err == nil {
				success(true)
			}
		}
	default:
		return
	}
}

func (p *T) cbHalf() {
	switch p.cb.State() {
	case gobreaker.StateOpen:
		time.Sleep(p.Config.CBTimeout)
	case gobreaker.StateClosed:
		p.cbOpen()
		time.Sleep(p.Config.CBTimeout)
	default:
		return
	}
}

func (p *T) cbOpen() {
	switch p.cb.State() {
	case gobreaker.StateHalfOpen, gobreaker.StateClosed:
		for i := uint32(0); i < p.Config.CBMaxFailures+1; i++ {
			success, err := p.cb.Allow()
			if err != nil {
				return
			}
			success(false)
		}
	default:
		return
	}
}
