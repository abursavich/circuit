// SPDX-License-Identifier: MIT
//
// Copyright 2022 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package circuitprom provides a Prometheus collector for circuit breakers.
package circuitprom

import (
	"sync/atomic"

	"bursavich.dev/circuit"
	"github.com/prometheus/client_golang/prometheus"
)

// An Observer is a Prometheus collector for a named circuit breaker.
type Observer struct {
	desc  *prometheus.Desc
	state atomic.Int64
}

// NewObserver returns an observer for the named circuit breaker.
func NewObserver(name string) *Observer {
	const (
		fqName = "circuit_breaker_closed"
		help   = "Describes whether the named circuit breaker is currently closed"
	)
	return &Observer{
		desc: prometheus.NewDesc(fqName, help, nil, prometheus.Labels{"name": name}),
	}
}

func (o *Observer) ObserveStateChange(state circuit.State) { o.state.Store(int64(state)) }

func (o *Observer) Describe(ch chan<- *prometheus.Desc) { ch <- o.desc }

func (o *Observer) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(o.desc, prometheus.GaugeValue, o.closedValue())
}

func (o *Observer) closedValue() float64 {
	if o.state.Load() == int64(circuit.Closed) {
		return 1
	}
	return 0
}
