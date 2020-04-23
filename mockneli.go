package goneli

import (
	"context"
	"fmt"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/obsidiandynamics/libstdgo/arity"
	"github.com/obsidiandynamics/libstdgo/concurrent"
)

// mockLeaderStatus represents the leader status of the mock.
type mockLeaderStatus int

const (
	mockLeaderStatusAcquired mockLeaderStatus = iota
	mockLeaderStatusRevoked
	mockLeaderStatusFenced
)

type mockNeli struct {
	pollDeadline  concurrent.Deadline
	barrier       Barrier
	runState      concurrent.AtomicReference
	currentStatus concurrent.AtomicReference
	targetStatus  concurrent.AtomicReference
	pulseError    concurrent.AtomicReference
}

// MockNeli is a mock of Neli, producing the same behaviour as the real thing, but without contending for leadership.
// Instead, leader status is assigned/revoked via the Transition method.
type MockNeli interface {
	Neli
	PulseError(err error)
	AcquireLeader()
	RevokeLeader()
	FenceLeader()
}

// MockConfig encapsulates the configuration for MockNeli.
type MockConfig struct {
	MinPollInterval *time.Duration
}

// SetDefaults assigns the default values to optional fields.
func (c *MockConfig) SetDefaults() {
	defaultDuration(&c.MinPollInterval, DefaultMinPollInterval)
}

// Validate the MockConfig, returning an error if invalid.
func (c MockConfig) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.MinPollInterval, validation.Required, validation.Min(1*time.Millisecond)),
	)
}

// Obtains a textual representation of the configuration.
func (c MockConfig) String() string {
	return fmt.Sprint("MockConfig[MinPollInterval=", c.MinPollInterval, "]")
}

// NewMock creates a MockNeli instance for the given config and optional barrier. If unspecified, a no-op barrier
// is used.
func NewMock(config MockConfig, barrier ...Barrier) (MockNeli, error) {
	barrierArg := arity.SoleUntyped(NopBarrier(), barrier).(Barrier)

	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &mockNeli{
		pollDeadline:  concurrent.NewDeadline(*config.MinPollInterval),
		barrier:       barrierArg,
		runState:      concurrent.NewAtomicReference(Live),
		currentStatus: concurrent.NewAtomicReference(mockLeaderStatusRevoked),
		targetStatus:  concurrent.NewAtomicReference(mockLeaderStatusRevoked),
		pulseError:    concurrent.NewAtomicReference(),
	}, nil
}

func (m *mockNeli) getCurrentStatus() mockLeaderStatus {
	return m.currentStatus.Get().(mockLeaderStatus)
}

func (m *mockNeli) getTargetStatus() mockLeaderStatus {
	return m.targetStatus.Get().(mockLeaderStatus)
}

// IsLeader returns true if this MockNeli instance is currently the elected leader.
func (m *mockNeli) IsLeader() bool {
	return m.getCurrentStatus() == mockLeaderStatusAcquired
}

// Pulse the MockNeli instance.
func (m *mockNeli) Pulse(timeout time.Duration) (bool, error) {
	ctx, cancel := concurrent.Timeout(context.Background(), timeout)
	defer cancel()
	return m.PulseCtx(ctx)
}

// Pulse the MockNeli instance.
func (m *mockNeli) PulseCtx(ctx context.Context) (bool, error) {
	for {
		leader := m.tryPulse()

		if m.pulseError.Get() != nil {
			defer m.pulseError.Set(nil)
			return m.IsLeader(), m.pulseError.Get().(error)
		}

		if m.State() != Live {
			return leader, ErrNonLivePulse
		}

		if leader {
			return true, nil
		}

		timer := time.NewTimer(m.pollDeadline.Remaining())
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, nil
		case <-timer.C:
			continue
		}
	}
}

func (m *mockNeli) tryPulse() bool {
	m.pollDeadline.TryRun(func() {
		fmt.Println("Transition ", m.getTargetStatus(), " ", m.getCurrentStatus())
		if m.getCurrentStatus() != m.getTargetStatus() {
			m.currentStatus.Set(m.targetStatus.Get())
			switch m.getTargetStatus() {
			case mockLeaderStatusAcquired:
				m.barrier(LeaderAcquired{})
			case mockLeaderStatusRevoked:
				m.barrier(LeaderRevoked{})
			case mockLeaderStatusFenced:
				m.barrier(LeaderFenced{})
			}
		}
	})
	return m.IsLeader()
}

// Deadline returns the underlying poll deadline object, concerning the minimum (lower bound) poll interval.
func (m *mockNeli) Deadline() concurrent.Deadline {
	return m.pollDeadline
}

// Close the MockNeli instance, terminating the underlying Kafka producer and consumer clients.
func (m *mockNeli) Close() error {
	m.runState.Set(Closed)
	return nil
}

// Await the closing of this MockNeli instance.
func (m *mockNeli) Await() {
	m.runState.Await(concurrent.RefEqual(Closed), concurrent.Indefinitely)
}

// State returns the current state of this MockNeli instance.
func (m *mockNeli) State() State {
	return m.runState.Get().(State)
}

// Background will place the given LeaderTask for conditional execution in a newly-spawned background Goroutine,
// managed by the returned Pulser instance.
func (m *mockNeli) Background(task LeaderTask) (Pulser, error) {
	return pulse(m, task)
}

// PulseError simulates a one-off error on Pulse/PulseCtx.
func (m *mockNeli) PulseError(err error) {
	m.pulseError.Set(err)
}

func (m *mockNeli) transition(targetStatus mockLeaderStatus) {
	m.targetStatus.Set(targetStatus)
}

// AcquireLeader initiates leader acquisition, which will be observed by the client upon its next call to Pulse().
func (m *mockNeli) AcquireLeader() {
	m.transition(mockLeaderStatusAcquired)
}

// RevokeLeader initiates leader revocation, which will be observed by the client upon its next call to Pulse().
func (m *mockNeli) RevokeLeader() {
	m.transition(mockLeaderStatusRevoked)
}

// FenceLeader initiates leader fencing, which will be observed by the client upon its next call to Pulse().
func (m *mockNeli) FenceLeader() {
	m.transition(mockLeaderStatusFenced)
}
