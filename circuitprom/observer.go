// SPDX-License-Identifier: MIT
//
// Copyright 2022 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package circuitprom provides a Prometheus collector for circuit breakers.
package circuitprom

import (
	"bursavich.dev/circuit"
	"github.com/prometheus/client_golang/prometheus"
)

// An Observer is a Prometheus collector for circuit breakers.
type Observer interface {
	circuit.Observer
	prometheus.Collector
}

type observer struct {
	closed *prometheus.GaugeVec
}

// NewObserver returns an observer for circuit breakers.
func NewObserver() Observer {
	return &observer{
		closed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "circuit_breaker_closed",
				Help: "Describes whether the named circuit breaker is currently closed",
			},
			[]string{"name"},
		),
	}
}

func (o *observer) Collect(ch chan<- prometheus.Metric) {
	o.closed.Collect(ch)
}

func (o *observer) Describe(ch chan<- *prometheus.Desc) {
	o.closed.Describe(ch)
}

func (o *observer) Init(name string) {
	o.closed.WithLabelValues(name).Set(1)
}

func (o *observer) ObserveStateChange(name string, state circuit.State) {
	v := 1.0
	if state != circuit.Closed {
		v = 0
	}
	o.closed.WithLabelValues(name).Set(v)
}
