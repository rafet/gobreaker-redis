// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32

	CB *CircuitBreaker
}

func (c *Counts) onRequest() {
	key := fmt.Sprintf("%s:%s:cb:counts", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Incr(key)
	// c.Requests++
}

func (c *Counts) onSuccess() {
	key := fmt.Sprintf("%s:%s:cb:successes", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Incr(key)

	key = fmt.Sprintf("%s:%s:cb:consecutive_successes", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Incr(key)

	key = fmt.Sprintf("%s:%s:cb:consecutive_failures", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Set(key, 0, 0)
	//c.TotalSuccesses++
	//c.ConsecutiveSuccesses++
	//c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	key := fmt.Sprintf("%s:%s:cb:failures", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Incr(key)

	key = fmt.Sprintf("%s:%s:cb:consecutive_failures", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Incr(key)

	key = fmt.Sprintf("%s:%s:cb:consecutive_successes", c.CB.redisKeyPrefix, c.CB.name)
	c.CB.redisClient.Set(key, 0, 0)

	//c.TotalFailures++
	//c.ConsecutiveFailures++
	//c.ConsecutiveSuccesses = 0
}

func (c *Counts) clear() {
	keys := []string{
		fmt.Sprintf("%s:%s:cb:counts", c.CB.redisKeyPrefix, c.CB.name),
		fmt.Sprintf("%s:%s:cb:successes", c.CB.redisKeyPrefix, c.CB.name),
		fmt.Sprintf("%s:%s:cb:failures", c.CB.redisKeyPrefix, c.CB.name),
		fmt.Sprintf("%s:%s:cb:consecutive_successes", c.CB.redisKeyPrefix, c.CB.name),
		fmt.Sprintf("%s:%s:cb:consecutive_failures", c.CB.redisKeyPrefix, c.CB.name),
	}
	c.CB.redisClient.Del(keys...)

	//c.Requests = 0
	//c.TotalSuccesses = 0
	//c.TotalFailures = 0
	//c.ConsecutiveSuccesses = 0
	//c.ConsecutiveFailures = 0
}

func (c *Counts) GetConsecutiveSuccesses() uint32 {
	key := fmt.Sprintf("%s:%s:cb:consecutive_successes", c.CB.redisKeyPrefix, c.CB.name)
	val, err := c.CB.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return uint32(val)
}

func (c *Counts) GetConsecutiveFailures() uint32 {
	key := fmt.Sprintf("%s:%s:cb:consecutive_failures", c.CB.redisKeyPrefix, c.CB.name)
	val, err := c.CB.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return uint32(val)
}

func (c *Counts) GetSuccesses() uint32 {
	key := fmt.Sprintf("%s:%s:cb:successes", c.CB.redisKeyPrefix, c.CB.name)
	val, err := c.CB.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return uint32(val)
}

func (c *Counts) GetFailures() uint32 {
	key := fmt.Sprintf("%s:%s:cb:failures", c.CB.redisKeyPrefix, c.CB.name)
	val, err := c.CB.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return uint32(val)
}

func (c *Counts) GetRequests() uint32 {
	key := fmt.Sprintf("%s:%s:cb:counts", c.CB.redisKeyPrefix, c.CB.name)
	val, err := c.CB.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return uint32(val)
}

func (c *Counts) LoadFromRedis() {
	c.Requests = c.GetRequests()
	c.TotalSuccesses = c.GetSuccesses()
	c.TotalFailures = c.GetFailures()
	c.ConsecutiveSuccesses = c.GetConsecutiveSuccesses()
	c.ConsecutiveFailures = c.GetConsecutiveFailures()
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from a request.
// If IsSuccessful returns true, the error is counted as a success.
// Otherwise the error is counted as a failure.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
type Settings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts Counts) bool
	OnStateChange func(name string, from State, to State)
	IsSuccessful  func(err error) bool

	RedisClient    *redis.Client
	RedisKeyPrefix string
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts Counts) bool
	isSuccessful  func(err error) bool
	onStateChange func(name string, from State, to State)

	mutex  sync.Mutex
	state  State
	counts Counts

	redisClient    *redis.Client
	redisKeyPrefix string
}

func (cb *CircuitBreaker) getState() State {
	key := fmt.Sprintf("%s:%s:cb:state", cb.redisKeyPrefix, cb.name)
	val, err := cb.redisClient.Get(key).Int64()
	if err != nil {
		return 0
	}
	return State(val)
}

func (cb *CircuitBreaker) _setState(state State) {
	key := fmt.Sprintf("%s:%s:cb:state", cb.redisKeyPrefix, cb.name)
	cb.redisClient.Set(key, state, 0)
}

func (cb *CircuitBreaker) getGeneration() uint64 {
	key := fmt.Sprintf("%s:%s:cb:generation", cb.redisKeyPrefix, cb.name)
	val, err := cb.redisClient.Get(key).Uint64()
	if err != nil {
		return 0
	}
	return val
}

func (cb *CircuitBreaker) incrGeneration() {
	key := fmt.Sprintf("%s:%s:cb:generation", cb.redisKeyPrefix, cb.name)
	cb.redisClient.Incr(key)
}

func (cb *CircuitBreaker) getExpiry() time.Time {
	key := fmt.Sprintf("%s:%s:cb:time:open", cb.redisKeyPrefix, cb.name)
	val, err := cb.redisClient.Get(key).Int64()
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, val)
}

func (cb *CircuitBreaker) setExpiry(expiry time.Time) {
	key := fmt.Sprintf("%s:%s:cb:time:open", cb.redisKeyPrefix, cb.name)
	if expiry.IsZero() {
		cb.redisClient.Del(key)
		return
	}

	cb.redisClient.Set(key, expiry.UnixNano(), 0)
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	if st.RedisClient == nil {
		panic("redis client is nil")
	} else {
		cb.redisClient = st.RedisClient
	}

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange
	cb.redisKeyPrefix = st.RedisKeyPrefix

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.counts.CB = cb
	cb.counts.LoadFromRedis()

	cb.toNewGeneration(time.Now())

	return cb
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultInterval = time.Duration(0) * time.Second
const defaultTimeout = time.Duration(60) * time.Second

func defaultReadyToTrip(counts Counts) bool {
	return counts.GetConsecutiveFailures() > 5
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Counts returns internal counters
func (tscb *TwoStepCircuitBreaker) Counts() Counts {
	return tscb.cb.Counts()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrOpenState
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}

	cb.counts.onRequest()
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onSuccess()
	case StateHalfOpen:
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}
	default:
	}
}

func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onFailure()
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	default:
	}
}

func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	expiry := cb.getExpiry()
	switch cb.getState() {
	case StateClosed:
		if !expiry.IsZero() && expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	default:
	}
	return cb.getState(), cb.getGeneration()
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	cbState := cb.getState()
	if cbState == state {
		return
	}

	prev := cbState
	cb._setState(state)

	cb.toNewGeneration(now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	//cb.generation++
	cb.incrGeneration()
	cb.counts.clear()

	var zero time.Time
	switch cb.getState() {
	case StateClosed:
		if cb.interval == 0 {
			cb.setExpiry(zero)
		} else {
			cb.setExpiry(now.Add(cb.interval))
		}
	case StateOpen:
		cb.setExpiry(now.Add(cb.timeout))
	default: // StateHalfOpen
		cb.setExpiry(zero)
	}
}
