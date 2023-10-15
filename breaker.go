// SPDX-License-Identifier: MIT
//
// Copyright 2023 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package circuit provides a circuit breaker implementation.
package circuit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"bursavich.dev/fastrand"
	"github.com/go-logr/logr"
)

// Default values.
const (
	DefaultWindow       = 10 * time.Second
	DefaultCooldown     = 5 * time.Second
	DefaultSpacing      = 2 * time.Second
	DefaultJitterFactor = 0.25
	DefaultMinSuccess   = 3
	DefaultMinResults   = 5
	DefaultFailureRatio = 0.5
)

// State is the state of a circuit breaker.
type State int

//go:generate stringer -type=State

const (
	Closed   State = iota // Closed allows all operations.
	HalfOpen              // HalfOpen allows a probing operation.
	Open                  // Open disallows all operations.
)

// An Observer observes changes to circuit breakers.
type Observer interface {
	// ObserverStateChange is a called in a critical section
	// when the state of the circuit breaker changes.
	ObserveStateChange(state State)
}

// A NotifyFunc is a function that is called in a critical section
// when the state of a circuit breaker changes.
type NotifyFunc func(state State)

type notifyFunc func(state State)

func (fn notifyFunc) ObserveStateChange(state State) { fn(state) }

// An Option provides an override to defaults.
type Option func(*Breaker) error

// WithLogger returns an Option that specifies the logger.
func WithLogger(log logr.Logger) Option {
	return func(b *Breaker) error {
		b.log = log
		return nil
	}
}

// WithObserver returns an Option that adds an Observer.
func WithObserver(observer Observer) Option {
	return func(b *Breaker) error {
		b.observers = append(b.observers, observer)
		return nil
	}
}

// WithNotifyFunc returns an Option that adds a function to be called
// in a critical section when the State of the circuit breaker changes.
func WithNotifyFunc(notify NotifyFunc) Option {
	return func(b *Breaker) error {
		b.observers = append(b.observers, notifyFunc(notify))
		return nil
	}
}

// WithWindow returns an Option that sets the window of time after
// which results are reset when the circuit breaker is Closed.
// The default is 10s.
func WithWindow(window time.Duration) Option {
	return func(b *Breaker) error {
		if window <= 0 {
			return fmt.Errorf("invalid window: %v", window)
		}
		b.window = window
		return nil
	}
}

// WithMinResults returns an Option that sets the minimum number of results
// in a window required to switch the circuit breaker from Closed to Open.
// The default is 5.
func WithMinResults(min int) Option {
	return func(b *Breaker) error {
		if min <= 0 {
			return fmt.Errorf("invalid minimum results: %v", min)
		}
		b.minResults = min
		return nil
	}
}

// WithFailureRatio returns an Option that sets the minimum failure ratio
// in a window required to switch the circuit breaker from Closed to Open.
// The default is 50%.
func WithFailureRatio(ratio float64) Option {
	return func(b *Breaker) error {
		if ratio <= 0 || ratio > 1 {
			return fmt.Errorf("invalid failure ratio: %v", ratio)
		}
		b.failRatio = ratio
		return nil
	}
}

// WithCooldown returns an Option that sets the cooldown time before
// probes will be allowed when the circuit breaker is Open.
// The default is 5s.
func WithCooldown(cooldown time.Duration) Option {
	return func(b *Breaker) error {
		if cooldown <= 0 {
			return fmt.Errorf("invalid cooldown: %v", cooldown)
		}
		b.cooldown = cooldown
		return nil
	}
}

// WithSpacing returns an Option that sets the spacing time between
// allowed probes when the circuit breaker is HalfOpen.
// The default is 2s.
func WithSpacing(spacing time.Duration) Option {
	return func(b *Breaker) error {
		if spacing <= 0 {
			return fmt.Errorf("invalid spacing: %v", spacing)
		}
		b.spacing = spacing
		return nil
	}
}

// WithJitterFactor returns an Option that sets the random
// jitter factor applied to cooldown and spacing delays.
// The default is 25%.
func WithJitterFactor(jitter float64) Option {
	return func(b *Breaker) error {
		if jitter < 0 {
			return fmt.Errorf("invalid jitter: %v", jitter)
		}
		b.jitter = jitter
		return nil
	}
}

// WithMinSuccess returns an Option that sets the minimum number of consecutive
// successful probes required to switch the circuit breaker from HalfOpen to Closed.
// The default is 3.
func WithMinSuccess(min int) Option {
	return func(b *Breaker) error {
		if min <= 0 {
			return fmt.Errorf("invalid minimum success: %v", min)
		}
		b.minSuccess = min
		return nil
	}
}

// WithFilter returns an Option that specifies a function to filter
// expected error results that should not be counted as a failure
// (e.g. Canceled, InvalidArgument, NotFound, etc.).
func WithFilter(filter func(error) error) Option {
	return func(b *Breaker) error {
		b.filter = filter
		return nil
	}
}

// A Breaker is a circuit breaker.
type Breaker struct {
	log logr.Logger

	window     time.Duration
	cooldown   time.Duration
	spacing    time.Duration
	jitter     float64
	minResults int
	minSuccess int
	failRatio  float64
	filter     func(error) error

	mu        sync.Mutex
	state     atomic.Int64
	deadline  atomic.Pointer[time.Time]
	success   atomic.Int64
	failure   atomic.Int64
	observers []Observer

	timeNow func() time.Time
}

// NewBreaker returns a circuit breaker with the given options.
func NewBreaker(options ...Option) (*Breaker, error) {
	b := &Breaker{
		log:        logr.Discard(),
		window:     DefaultWindow,
		cooldown:   DefaultCooldown,
		spacing:    DefaultSpacing,
		jitter:     DefaultJitterFactor,
		minResults: DefaultMinResults,
		minSuccess: DefaultMinSuccess,
		failRatio:  DefaultFailureRatio,
		timeNow:    time.Now,
	}
	for _, fn := range options {
		if err := fn(b); err != nil {
			return nil, err
		}
	}
	window := b.timeNow().Add(b.window)
	b.deadline.Store(&window)
	return b, nil
}

// State returns the current state of the circuit breaker.
func (b *Breaker) State() State { return State(b.state.Load()) }

func (b *Breaker) lockedStoreState(state State) {
	b.log.Info("Circuit breaker state changed", "state", state)
	b.state.Store(int64(state))
	for _, o := range b.observers {
		o.ObserveStateChange(state)
	}
}

// AddNotifyFunc adds the notify function to be called in a critical section
// when the State of the circuit breaker changes.
func (b *Breaker) AddNotifyFunc(notify NotifyFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.observers = append(b.observers, notifyFunc(notify))
}

// Allow returns a value indicating if an operation is allowed.
// If the operation is allowed, its result MUST be recorded.
func (b *Breaker) Allow() bool {
	if b.State() == Closed {
		return true
	}
	return b.shouldProbe()
}

func (b *Breaker) shouldProbe() bool {
	now := b.timeNow()
	deadline := b.deadline.Load()
	if !deadline.Before(now) {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	delay := b.spacing
	state := b.State()
	if state == Open {
		delay = b.cooldown
	}
	then := now.Add(fastrand.Jitter(delay, b.jitter))
	if !b.deadline.CompareAndSwap(deadline, &then) {
		return false
	}
	if state == Open {
		b.failure.Store(0)
		b.success.Store(0)
		b.lockedStoreState(HalfOpen)
	}
	return true
}

// Record records the error result of an allowed operation.
func (b *Breaker) Record(err error) {
	if err != nil && b.filter != nil {
		err = b.filter(err)
	}
	switch b.State() {
	case Closed:
		b.recordClosed(err)
	case HalfOpen:
		b.recordHalfOpen(err)
	case Open:
		// TODO: Record latent success?
	}
}

func (b *Breaker) recordHalfOpen(err error) {
	b.mu.Lock()
	if b.State() != HalfOpen {
		b.mu.Unlock()
		b.Record(err)
	}
	defer b.mu.Unlock()

	// Reopen
	if err != nil {
		cooldown := b.deadline.Load().Add(fastrand.Jitter(b.cooldown, b.jitter))
		if now := b.timeNow(); cooldown.Before(now) {
			cooldown = now
		}
		b.deadline.Store(&cooldown)
		b.lockedStoreState(Open)
		return
	}

	// Stay HalfOpen
	if b.success.Add(1) < int64(b.minSuccess) {
		return
	}

	// Close
	window := b.timeNow().Add(b.window)
	b.deadline.Store(&window)
	b.failure.Store(0)
	b.success.Store(0)
	b.lockedStoreState(Closed)
}

func (b *Breaker) recordClosed(err error) {
	now := b.timeNow()

	// Rotate window.
	if b.deadline.Load().Before(now) {
		b.mu.Lock()
		if b.State() != Closed {
			b.mu.Unlock()
			b.Record(err)
			return
		}
		total := b.success.Load() + b.failure.Load()
		if b.deadline.Load().Before(now) && total >= int64(b.minResults) {
			b.failure.Store(0)
			b.success.Store(0)
			window := now.Add(b.window)
			b.deadline.Store(&window)
		}
		b.mu.Unlock()
	}

	// Increment counters and check status.
	var failure, success int64
	if err != nil {
		failure = b.failure.Add(1)
		success = b.success.Load()
	} else {
		success = b.success.Add(1)
		failure = b.failure.Load()
	}
	total := failure + success
	ratio := float64(failure) / float64(total)
	if total < int64(b.minResults) || ratio < b.failRatio {
		return
	}

	// Open circuit.
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.State() != Closed {
		return
	}
	cooldown := now.Add(fastrand.Jitter(b.cooldown, b.jitter))
	b.deadline.Store(&cooldown)
	b.lockedStoreState(Open)
}
