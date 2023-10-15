// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package circuitprom provides a Prometheus collector for circuit breakers.
package circuitprom

import (
	"sync/atomic"

	"bursavich.dev/circuit"
	"github.com/prometheus/client_golang/prometheus"
)

// An Option provides optional configuration for an Observer.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (fn optionFunc) apply(c *config) { fn(c) }

type config struct {
	namespace   string
	constLabels prometheus.Labels
}

// WithNamespace returns an Option that sets the namespace for all metrics.
// The default is empty.
func WithNamespace(namespace string) Option {
	return optionFunc(func(c *config) {
		c.namespace = namespace
	})
}

// WithConstLabels returns an Option that adds constant labels to all metrics.
func WithConstLabels(labels prometheus.Labels) Option {
	return optionFunc(func(c *config) {
		if c.constLabels == nil && len(labels) > 0 {
			c.constLabels = make(prometheus.Labels, len(labels))
		}
		for k, v := range labels {
			c.constLabels[k] = v
		}
	})
}

// An Observer is a Prometheus collector for a circuit breaker.
type Observer interface {
	circuit.Observer
	prometheus.Collector
}

type observer struct {
	state atomic.Int32
	desc  *prometheus.Desc
}

// NewObserver returns an observer for a circuit breaker with the given options.
func NewObserver(options ...Option) Observer {
	var cfg config
	for _, o := range options {
		o.apply(&cfg)
	}
	return &observer{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(cfg.namespace, "circuit_breaker", "status"),
			`Describes the current status of the circuit breaker ("Closed", "HalfOpen", or "Open")`,
			[]string{"status"},
			cfg.constLabels,
		),
	}
}

func (o *observer) Collect(ch chan<- prometheus.Metric) {
	state := circuit.State(o.state.Load())
	ch <- prometheus.MustNewConstMetric(o.desc, prometheus.GaugeValue, eq(state, circuit.Closed), circuit.Closed.String())
	ch <- prometheus.MustNewConstMetric(o.desc, prometheus.GaugeValue, eq(state, circuit.HalfOpen), circuit.HalfOpen.String())
	ch <- prometheus.MustNewConstMetric(o.desc, prometheus.GaugeValue, eq(state, circuit.Open), circuit.Open.String())
}

func (o *observer) Describe(ch chan<- *prometheus.Desc) {
	ch <- o.desc
}

func (o *observer) ObserveStateChange(state circuit.State) {
	o.state.Store(int32(state))
}

func eq[T comparable](a, b T) float64 {
	if a == b {
		return 1
	}
	return 0
}
