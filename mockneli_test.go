package goneli

import (
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransition(t *testing.T) {
	event := concurrent.NewAtomicReference()
	barrier := func(e Event) {
		event.Set(e)
	}
	m, err := NewMock(MockConfig{}, barrier)
	require.Nil(t, err)
	require.NotNil(t, m)
	require.Equal(t, Live, m.State())

	// Starts off with no events fired and a non-leader state.
	require.Nil(t, event.Get())
	require.False(t, m.IsLeader())
	require.Equal(t, time.Unix(0, 0), m.Deadline().Last())

	// Transitioning will not change state until the next pulse.
	m.AcquireLeader()
	require.Nil(t, event.Get())
	require.False(t, m.IsLeader())

	// Transition kicks in after pulsing.
	isLeader, err := m.Pulse(10 * time.Second)
	require.Nil(t, err)
	require.True(t, isLeader)
	require.Equal(t, LeaderAcquired{}, event.Get())
	require.True(t, m.IsLeader())
	require.NotEqual(t, time.Unix(0, 0), m.Deadline().Last())

	// Transition away from leader state.
	m.RevokeLeader()
	require.True(t, m.IsLeader())
	require.Equal(t, LeaderAcquired{}, event.Get())

	m.Deadline().Move(time.Unix(0, 0))
	isLeader, err = m.Pulse(1 * time.Millisecond)
	require.Nil(t, err)
	require.False(t, isLeader)
	require.Equal(t, LeaderRevoked{}, event.Get())
	require.False(t, m.IsLeader())

	// Fenced state.
	m.FenceLeader()
	m.Deadline().Move(time.Unix(0, 0))
	isLeader, err = m.Pulse(1 * time.Millisecond)
	require.Nil(t, err)
	require.False(t, isLeader)
	require.Equal(t, LeaderFenced{}, event.Get())
	require.False(t, m.IsLeader())

	require.Nil(t, m.Close())
	require.Equal(t, Closed, m.State())
	m.Await()
}

func TestMockConfigValidationError(t *testing.T) {
	m, err := NewMock(MockConfig{
		MinPollInterval: Duration(-1),
	})
	require.Nil(t, m)
	require.NotNil(t, err)
}

func TestMockConfigString(t *testing.T) {
	require.Contains(t, MockConfig{}.String(), "MockConfig[")
}

func TestMockBackground(t *testing.T) {
	event := concurrent.NewAtomicReference()
	barrier := func(e Event) {
		event.Set(e)
	}
	m, err := NewMock(MockConfig{}, barrier)
	require.Nil(t, err)
	require.NotNil(t, m)
	require.Equal(t, Live, m.State())

	liveCount := concurrent.NewAtomicCounter()
	p, err := m.Background(func() {
		liveCount.Inc()
	})
	require.Nil(t, err)
	require.NotNil(t, p)

	// Starts off with no events fired and a non-leader state.
	require.Nil(t, event.Get())
	require.False(t, m.IsLeader())
	require.Equal(t, 0, liveCount.GetInt())

	m.AcquireLeader()
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.Equal(t, LeaderAcquired{}, event.Get())
		assert.GreaterOrEqual(t, liveCount.GetInt(), 1)
	})

	require.Nil(t, m.Close())
	require.Equal(t, Closed, m.State())
	m.Await()
	assertNoError(t, p.Await)
}

func TestMockPulseTimeout(t *testing.T) {
	m, err := NewMock(MockConfig{})
	require.Nil(t, err)
	require.NotNil(t, m)

	isLeader, err := m.Pulse(10 * time.Millisecond)
	require.Nil(t, err)
	require.False(t, isLeader)

	require.Nil(t, m.Close())
	require.Equal(t, Closed, m.State())
	m.Await()
}

func TestMockPulseEventuallyLeader(t *testing.T) {
	var m MockNeli
	barrier := func(e Event) {
		switch e.(type) {
		case LeaderFenced:
			m.AcquireLeader()
		}
	}
	m, err := NewMock(MockConfig{}, barrier)
	require.Nil(t, err)
	require.NotNil(t, m)

	m.FenceLeader()
	isLeader, err := m.Pulse(10 * time.Second)
	require.Nil(t, err)
	require.True(t, isLeader)

	require.Nil(t, m.Close())
	require.Equal(t, Closed, m.State())
	m.Await()
}

func TestMockPulseError(t *testing.T) {
	m, _ := NewMock(MockConfig{})
	m.PulseError(check.ErrSimulated)
	isLeader, err := m.Pulse(1 * time.Millisecond)
	require.False(t, isLeader)
	require.Equal(t, err, check.ErrSimulated)
}
