// SPDX-License-Identifier: MIT
//
// Copyright 2022 Andrew Bursavich. All rights reserved.
// Use of this source code is governed by The MIT License
// which can be found in the LICENSE file.

// Package circuit provides a circuit breaker implementation.
package circuit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Default values.
const (
	DefaultWindow       = 10 * time.Second
	DefaultCooldown     = 5 * time.Second
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

// An Option provides an override to defaults.
type Option func(*Breaker) error

// WithWindow returns an Option that sets the Closed window.
// The default is 5s.
func WithWindow(window time.Duration) Option {
	return func(b *Breaker) error {
		if window <= 0 {
			return fmt.Errorf("invalid window: %v", window)
		}
		b.window = window
		return nil
	}
}

// WithCooldown returns an Option that sets the Open cooldown.
// The default is 10s.
func WithCooldown(cooldown time.Duration) Option {
	return func(b *Breaker) error {
		if cooldown <= 0 {
			return fmt.Errorf("invalid cooldown: %v", cooldown)
		}
		b.cooldown = cooldown
		return nil
	}
}

// WithMinResults returns an Option that sets the minimum number of results
// required in a Closed window before the circuit breaker may be Opened.
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
// required in a Closed window before the circuit breaker may be Opened.
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

// WithNotifyFunc returns an Option that specifies a function to be called
// in a critical section when the State of the circuit breaker changes.
func WithNotifyFunc(notify func(State)) Option {
	return func(b *Breaker) error {
		b.notifyFns = append(b.notifyFns, notify)
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
	window     time.Duration
	cooldown   time.Duration
	minResults int
	failRatio  float64
	filter     func(error) error

	mu        sync.Mutex
	state     atomic.Int64
	deadline  atomic.Pointer[time.Time]
	success   atomic.Int64
	failure   atomic.Int64
	notifyFns []func(State)

	timeNow func() time.Time
}

// NewBreaker returns a circuit breaker with the given options.
func NewBreaker(options ...Option) (*Breaker, error) {
	b := &Breaker{
		window:     DefaultWindow,
		cooldown:   DefaultCooldown,
		minResults: DefaultMinResults,
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
	b.state.Store(int64(state))
	for _, notify := range b.notifyFns {
		notify(state)
	}
}

// AddNotifyFunc adds the notify function to be called in a critical section
// when the State of the circuit breaker changes.
func (b *Breaker) AddNotifyFunc(notify func(State)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.notifyFns = append(b.notifyFns, notify)
}

// Allow returns a value indicating if an operation is allowed.
// If the operation is allowed, its result MUST be recorded.
func (b *Breaker) Allow() bool {
	switch State(b.state.Load()) {
	case Closed:
		return true
	case HalfOpen:
		// Allowing an occasional probe here can prevent the breaker
		// from being stuck in a bad state due to misbehaving callers
		// that fail to report the result of the original probe.
		return b.shouldProbe()
	case Open:
		return b.shouldProbe()
	default:
		panic("invalid state")
	}
}

func (b *Breaker) shouldProbe() bool {
	now := b.timeNow()
	deadline := b.deadline.Load()
	if !deadline.Before(now) {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cooldown := now.Add(b.cooldown)
	if !b.deadline.CompareAndSwap(deadline, &cooldown) {
		return false
	}
	if b.State() == Open {
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
	case Open:
		// TODO: Close on latent success?
	case Closed:
		b.recordClosed(err)
	case HalfOpen:
		b.recordHalfOpen(err)
	}
}

func (b *Breaker) recordHalfOpen(err error) {
	b.mu.Lock()
	if b.State() != HalfOpen {
		b.mu.Unlock()
		b.Record(err)
	}
	defer b.mu.Unlock()

	now := b.timeNow()

	// Reopen
	if err != nil {
		cooldown := b.deadline.Load().Add(b.cooldown)
		if cooldown.Before(now) {
			cooldown = now
		}
		b.deadline.Store(&cooldown)
		b.lockedStoreState(Open)
		return
	}

	// Close
	window := now.Add(b.window)
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
	cooldown := now.Add(b.cooldown)
	b.deadline.Store(&cooldown)
	b.lockedStoreState(Open)
}
