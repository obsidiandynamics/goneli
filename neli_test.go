package goneli

import (
	"sync"
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func wait(t check.Tester) check.Timesert {
	return check.Wait(t, 10*time.Second)
}

type fixtures struct{}

func (fixtureOpts fixtures) create() (scribe.MockScribe, *consMock, Config, *testBarrier) {
	m := scribe.NewMock()

	cons := &consMock{}
	cons.fillDefaults()

	config := Config{
		KafkaConsumerProvider: mockKafkaConsumerProvider(cons),
		MinPollInterval:       Duration(1 * time.Millisecond),
		PollDuration:          Duration(1 * time.Millisecond),
		Scribe:                scribe.New(m.Factories()),
	}
	config.Scribe.SetEnabled(scribe.All)

	return m, cons, config, &testBarrier{}
}

type testBarrier struct {
	mutex  sync.Mutex
	events []Event
}

func (c *testBarrier) barrier() Barrier {
	return func(e Event) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.events = append(c.events, e)
	}
}

func (c *testBarrier) list() []Event {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	eventsCopy := make([]Event, len(c.events))
	copy(eventsCopy, c.events)
	return eventsCopy
}

func (c *testBarrier) length() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.events)
}

func TestCorrectInitialisation(t *testing.T) {
	_, cons, config, b := fixtures{}.create()

	var givenConsumerConfig *KafkaConfigMap
	var givenLeaderTopic string

	cons.f.Subscribe = func(m *consMock, topic string, rebalanceCb kafka.RebalanceCb) error {
		givenLeaderTopic = topic
		return nil
	}
	config.KafkaConsumerProvider = func(conf *KafkaConfigMap) (KafkaConsumer, error) {
		givenConsumerConfig = conf
		return cons, nil
	}
	config.LeaderTopic = "test leader topic"
	config.LeaderGroupID = "test leader group ID"
	config.KafkaConfig = KafkaConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	n, err := New(config, b.barrier())
	require.Nil(t, err)
	assert.Equal(t, Live, n.State())

	assert.Equal(t, config.LeaderTopic, givenLeaderTopic)
	assert.Contains(t, *givenConsumerConfig, "bootstrap.servers")
	assert.Equal(t, 1, cons.c.Subscribe.GetInt())

	assertNoError(t, n.Close)
	n.Await()
	assert.Equal(t, Closed, n.State())

	assert.Equal(t, 1, cons.c.Close.GetInt())
}

func TestConfigError(t *testing.T) {
	n, err := New(Config{
		MinPollInterval: Duration(0),
	}, (&testBarrier{}).barrier())
	assert.Nil(t, n)
	assert.NotNil(t, err)
}

func TestErrorDuringConsumerInitialisation(t *testing.T) {
	_, _, config, b := fixtures{}.create()

	config.KafkaConsumerProvider = func(conf *KafkaConfigMap) (KafkaConsumer, error) {
		return nil, check.ErrSimulated
	}
	n, err := New(config, b.barrier())
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "simulated")
	assert.Nil(t, n)
}

func TestErrorDuringConsumerConfiguration(t *testing.T) {
	_, _, config, b := fixtures{}.create()

	config.KafkaConfig = KafkaConfigMap{
		"group.id": "overridden_group",
	}
	n, err := New(config, b.barrier())
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot override configuration 'group.id'")
	assert.Nil(t, n)
}

func TestErrorDuringSubscribe(t *testing.T) {
	_, cons, config, b := fixtures{}.create()

	cons.f.Subscribe = func(m *consMock, topic string, rebalanceCb kafka.RebalanceCb) error {
		return check.ErrSimulated
	}

	n, err := New(config, b.barrier())
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "simulated")
	assert.Nil(t, n)
	assert.Equal(t, 1, cons.c.Close.GetInt())
}

func TestPulseNotLeader(t *testing.T) {
	_, _, config, _ := fixtures{}.create()

	n, err := New(config)
	require.Nil(t, err)

	isLeader, err := n.Pulse(1 * time.Millisecond)
	assert.False(t, isLeader)
	assert.Nil(t, err)

	assertNoError(t, n.Close)
	n.Await()
}

func TestPulseAfterClose(t *testing.T) {
	_, _, config, _ := fixtures{}.create()

	n, err := New(config)
	require.Nil(t, err)

	assertNoError(t, n.Close)
	n.Await()

	isLeader, err := n.Pulse(1 * time.Millisecond)
	assert.False(t, isLeader)
	assert.Equal(t, err, ErrNonLivePulse)
}

func TestDeadline(t *testing.T) {
	_, _, config, _ := fixtures{}.create()

	n, err := New(config)
	require.Nil(t, err)

	assert.Equal(t, n.Deadline().Last(), time.Unix(0, 0))
	isLeader, err := n.Pulse(1 * time.Millisecond)
	assert.False(t, isLeader)
	assert.Nil(t, err)
	assert.NotEqual(t, n.Deadline().Last(), time.Unix(0, 0))

	assertNoError(t, n.Close)
	n.Await()
}

func TestBasicLeaderElectionAndRevocation(t *testing.T) {
	m, cons, config, b := fixtures{}.create()

	n, err := New(config, b.barrier())
	require.Nil(t, err)

	onLeaderCnt := concurrent.NewAtomicCounter()
	p, err := n.Background(func() {
		onLeaderCnt.Inc()
	})
	require.Nil(t, err)

	// Starts off in a non-leader state
	assert.Equal(t, false, n.IsLeader())

	// Assign leadership via the rebalance listener and wait for the assignment to take effect
	cons.rebalanceEvents <- assignedPartitions(0, 1, 2)
	wait(t).UntilAsserted(isTrue(n.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Elected as leader")).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 1, b.length()) {
			_ = b.list()[0].(*LeaderElected)
		}
	})
	wait(t).UntilAsserted(atLeast(1, onLeaderCnt.GetInt))

	// Revoke some partitions, but not containing leader partition (0)... check that still leader
	cons.rebalanceEvents <- revokedPartitions(1)
	wait(t).UntilAsserted(
		m.ContainsEntries().
			Having(scribe.LogLevel(scribe.Trace)).
			Having(scribe.MessageContaining("Revoked partitions")).
			Passes(scribe.Count(1)))
	m.Reset()
	assert.True(t, n.IsLeader())
	assert.Equal(t, 1, b.length())

	// Revoke leadership via the rebalance listener and await its effect
	cons.rebalanceEvents <- revokedPartitions(0, 2)
	wait(t).UntilAsserted(isFalse(n.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Lost leader status")).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 2, b.length()) {
			_ = b.list()[1].(*LeaderRevoked)
		}
	})

	// Assign a partition, but not the leader one... check that still not a leader
	cons.rebalanceEvents <- assignedPartitions(1)
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Trace)).
		Having(scribe.MessageContaining("Assigned partitions")).
		Passes(scribe.Count(1)))
	m.Reset()
	assert.False(t, n.IsLeader())
	assert.Equal(t, 2, b.length())

	assertNoError(t, n.Close)
	n.Await()
	assertNoError(t, p.Await)
}

func TestLeaderElectionAndRevocation_nopBarrier(t *testing.T) {
	m, cons, config, _ := fixtures{}.create()

	n, err := New(config)
	require.Nil(t, err)

	onLeaderCnt := concurrent.NewAtomicCounter()
	p, err := n.Background(func() {
		onLeaderCnt.Inc()
	})
	require.Nil(t, err)

	// Starts off in a non-leader state
	assert.Equal(t, false, n.IsLeader())

	// Assign leadership via the rebalance listener and wait for the assignment to take effect
	cons.rebalanceEvents <- assignedPartitions(0, 1, 2)
	wait(t).UntilAsserted(isTrue(n.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Elected as leader")).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(atLeast(1, onLeaderCnt.GetInt))

	assertNoError(t, n.Close)
	n.Await()
	assertNoError(t, p.Await)
}

func TestNonFatalErrorInReadMessage(t *testing.T) {
	m, cons, config, _ := fixtures{}.create()

	cons.f.ReadMessage = func(m *consMock, timeout time.Duration) (*kafka.Message, error) {
		return nil, kafka.NewError(kafka.ErrAllBrokersDown, "simulated", false)
	}

	n, err := New(config)
	require.Nil(t, err)

	onLeaderCnt := concurrent.NewAtomicCounter()
	p, err := n.Background(func() {
		onLeaderCnt.Inc()
	})
	require.Nil(t, err)

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Recoverable error during poll")).
		Passes(scribe.CountAtLeast(1)))
	assert.Equal(t, Live, n.State())

	assertNoError(t, n.Close)
	n.Await()

	assertNoError(t, p.Await)
	assertNoError(t, p.Error)
}

func TestFatalErrorInReadMessage(t *testing.T) {
	m, cons, config, _ := fixtures{}.create()

	cons.f.ReadMessage = func(m *consMock, timeout time.Duration) (*kafka.Message, error) {
		return nil, kafka.NewError(kafka.ErrFatal, "simulated", true)
	}

	n, err := New(config)
	require.Nil(t, err)

	onLeaderCnt := concurrent.NewAtomicCounter()
	p, err := n.Background(func() {
		onLeaderCnt.Inc()
	})
	require.Nil(t, err)

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Error)).
		Having(scribe.MessageContaining("Fatal error during poll")).
		Passes(scribe.CountAtLeast(1)))
	assert.Equal(t, Live, n.State())

	assertNoError(t, n.Close)
	n.Await()

	err = p.Await()
	require.NotNil(t, err)
	assert.Equal(t, "Fatal error: simulated", err.Error())

	err = p.Error()
	require.NotNil(t, err)
	assert.Equal(t, "Fatal error: simulated", err.Error())
}

func TestClosePulser(t *testing.T) {
	_, _, config, _ := fixtures{}.create()

	n, err := New(config)
	require.Nil(t, err)

	onLeaderCnt := concurrent.NewAtomicCounter()
	p, err := n.Background(func() {
		onLeaderCnt.Inc()
	})
	require.Nil(t, err)

	p.Close()
	assertNoError(t, p.Await)
	assertNoError(t, p.Error)

	assertNoError(t, n.Close)
	n.Await()
}

func TestPulserWithError(t *testing.T) {
	p, err := pulse(nil, nil)
	assert.Nil(t, p)
	assert.NotNil(t, err)
}

func intEqual(expected int, intSupplier func() int) func(t check.Tester) {
	return func(t check.Tester) {
		assert.Equal(t, expected, intSupplier())
	}
}

func atLeast(min int, f func() int) check.Assertion {
	return func(t check.Tester) {
		assert.GreaterOrEqual(t, f(), min)
	}
}

func isTrue(f func() bool) check.Assertion {
	return func(t check.Tester) {
		assert.True(t, f())
	}
}

func isFalse(f func() bool) check.Assertion {
	return func(t check.Tester) {
		assert.False(t, f())
	}
}

func isNotNil(f func() interface{}) check.Assertion {
	return func(t check.Tester) {
		assert.NotNil(t, f())
	}
}

func assertNoError(t *testing.T, f func() error) {
	err := f()
	require.Nil(t, err)
}

func generatePartitions(indexes ...int32) []kafka.TopicPartition {
	parts := make([]kafka.TopicPartition, len(indexes))
	for i, index := range indexes {
		parts[i] = kafka.TopicPartition{Partition: index}
	}
	return parts
}

func assignedPartitions(indexes ...int32) kafka.AssignedPartitions {
	return kafka.AssignedPartitions{Partitions: generatePartitions(indexes...)}
}

func revokedPartitions(indexes ...int32) kafka.RevokedPartitions {
	return kafka.RevokedPartitions{Partitions: generatePartitions(indexes...)}
}
