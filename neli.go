/*
Package goneli implements the (Non-)Exclusive Leader Induction protocol (NELI), published
in https://github.com/obsidiandynamics/NELI.

This implementation is for the 'simplified' variation of the protocol, running in exclusive mode over a
single NELI group.

This implementation is thread-safe.
*/
package goneli

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/obsidiandynamics/libstdgo/arity"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Neli is a curator for leader election.
type Neli interface {
	IsLeader() bool
	Pulse(timeout time.Duration) (bool, error)
	PulseCtx(ctx context.Context) (bool, error)
	Deadline() concurrent.Deadline
	Close() error
	Await()
	State() State
	Background(task LeaderTask) (Pulser, error)
}

type neli struct {
	config       Config
	scribe       scribe.Scribe
	consumer     KafkaConsumer
	pollDeadline concurrent.Deadline
	isLeader     concurrent.AtomicCounter
	barrier      Barrier
	state        concurrent.AtomicReference
	stateMutex   sync.Mutex
}

// State of the Neli instance.
type State int

const (
	// Live — currently operational, with a live Kafka consumer subscription.
	Live State = iota

	// Closing — in the process of closing the underlying resources (such as the Kafka consumer client).
	Closing

	// Closed — has been completely disposed of.
	Closed
)

// ErrNonLivePulse is returned by Pulse() if the Neli instance has been closed.
var ErrNonLivePulse = fmt.Errorf("cannot pulse in non-live state")

// New creates a Neli instance for the given config and optional barrier. If unspecified, a no-op barrier
// is used.
func New(config Config, barrier ...Barrier) (Neli, error) {
	barrierArg := arity.SoleUntyped(NopBarrier(), barrier).(Barrier)

	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	n := &neli{
		config:       config,
		scribe:       config.Scribe,
		isLeader:     concurrent.NewAtomicCounter(),
		barrier:      barrierArg,
		pollDeadline: concurrent.NewDeadline(*config.MinPollInterval),
		state:        concurrent.NewAtomicReference(Live),
	}

	consumerConfigs := copyKafkaConfig(n.config.KafkaConfig)
	err := setKafkaConfigs(consumerConfigs, KafkaConfigMap{
		"group.id":           n.config.LeaderGroupID,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	success := false
	defer n.cleanupFailedStart(&success)

	n.logger().T()("Creating Kafka consumer with config %v", &consumerConfigs)
	c, err := n.config.KafkaConsumerProvider(&consumerConfigs)
	if err != nil {
		return nil, err
	}
	n.consumer = c

	err = c.Subscribe(n.config.LeaderTopic, func(_ *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			onAssigned(n, e)
		case kafka.RevokedPartitions:
			onRevoked(n, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	success = true
	return n, nil
}

func (n *neli) logger() scribe.StdLogAPI {
	return n.scribe.Capture(n.scene())
}

func (n *neli) scene() scribe.Scene {
	return scribe.Scene{Fields: scribe.Fields{"name": n.config.Name}}
}

func (n *neli) cleanupFailedStart(success *bool) {
	if *success {
		return
	}

	if n.consumer != nil {
		n.consumer.Close()
	}
}

// IsLeader returns true if this Neli instance is currently the elected leader.
func (n *neli) IsLeader() bool {
	return n.isLeader.GetInt() == 1
}

// Pulse the Neli instance. This will occasionally poll the underlying Kafka consumer, thereby asserting the
// client's health to the Kafka cluster. The timeout specifies how long this method should block if the
// client is not the current leader. If leader status is acquired, this method will return true. Otherwise,
// if leader status could not be acquired within the timeout period, false is returned.
//
// If a fatal error is encountered upon attempting to poll Kafka, it is returned along with the present leader status.
//
// Either Pulse() or PulseCtx() should be called routinely by the application to indicate liveness. If this method is
// not called within the period specified by the max.poll.interval.ms Kafka property, the client risks having
// its leader status silently revoked, creating a potentially hazardous situation. Calling this method is 'cheap'; it
// will only result in network I/O approximately once every Config.MinPollInterval. As such, it should be called as
// frequently as possible — ideally from a tight loop.
func (n *neli) Pulse(timeout time.Duration) (isLeader bool, err error) {
	ctx, cancel := concurrent.Timeout(context.Background(), timeout)
	defer cancel()
	return n.PulseCtx(ctx)
}

// Pulse the Neli instance. This will occasionally poll the underlying Kafka consumer, thereby asserting the
// client's health to the Kafka cluster. The ctx argument allows this method to be cancelled. If leader status is
// acquired prior to cancellation, this method will return true. Otherwise, if leader status could not be acquired
// before the invocation was cancelled, false is returned.
//
// If a fatal error is encountered upon attempting to poll Kafka, it is returned along with the present leader status.
//
// Either Pulse() or PulseCtx() should be called routinely by the application to indicate liveness. If this method is
// not called within the period specified by the max.poll.interval.ms Kafka property, the client risks having
// its leader status silently revoked, creating a potentially hazardous situation. Calling this method is 'cheap'; it
// will only result in network I/O approximately once every Config.MinPollInterval. As such, it should be called as
// frequently as possible — ideally from a tight loop.
func (n *neli) PulseCtx(ctx context.Context) (isLeader bool, err error) {
	for {
		leader, err := n.tryPulse()
		if leader || err != nil {
			return leader, err
		}

		timeRemaining := *n.config.MinPollInterval - n.pollDeadline.Elapsed()
		timer := time.NewTimer(timeRemaining)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, nil
		case <-timer.C:
			continue
		}
	}
}

func (n *neli) tryPulse() (bool, error) {
	var error error
	n.pollDeadline.TryRun(func() {
		n.stateMutex.Lock()
		defer n.stateMutex.Unlock()

		if n.State() != Live {
			error = ErrNonLivePulse
			return
		}

		// Polling is just to indicate consumer liveness; we don't actually care about the messages consumed.
		n.logger().T()("Polling... (is leader: %v)", n.IsLeader())
		_, err := n.consumer.ReadMessage(*n.config.PollDuration)
		if err != nil {
			if isFatalError(err) {
				error = err
				n.logger().E()("Fatal error during poll: %v", err)
			} else if !isTimedOutError(err) {
				n.logger().W()("Recoverable error during poll: %v", err)
			}
		}
	})
	return n.IsLeader(), error
}

// Deadline returns the underlying poll deadline object, concerning the minimum (lower bound) poll interval. The
// returned Deadline can be used to determine when the last Kafka poll was made and when the next one is expected
// to occur.
func (n *neli) Deadline() concurrent.Deadline {
	return n.pollDeadline
}

func onAssigned(n *neli, assigned kafka.AssignedPartitions) {
	n.logger().T()("Assigned partitions: %s", assigned)
	if containsPartition(assigned.Partitions, 0) {
		n.isLeader.Set(1)
		n.logger().I()("Elected as leader")
		n.barrier(&LeaderElected{})
	}
}

func onRevoked(n *neli, revoked kafka.RevokedPartitions) {
	n.logger().T()("Revoked partitions: %s", revoked)
	if containsPartition(revoked.Partitions, 0) {
		n.logger().I()("Lost leader status")
		n.isLeader.Set(0)
		n.barrier(&LeaderRevoked{})
	}
}

// State returns the current state of this Neli instance.
func (n *neli) State() State {
	return n.state.Get().(State)
}

// Close the Neli instance, terminating the underlying Kafka consumer client.
func (n *neli) Close() error {
	n.stateMutex.Lock()
	defer n.stateMutex.Unlock()
	defer n.state.Set(Closed)

	n.state.Set(Closing)
	return n.consumer.Close()
}

// Await the closing of this Neli instance.
func (n *neli) Await() {
	n.state.Await(concurrent.RefEqual(Closed), concurrent.Indefinitely)
}

// Background will place the given LeaderTask for conditional execution in a newly-spawned background goroutine,
// managed by the returned Pulser instance. The background goroutine will continuously invoke PulseCtx(),
// followed by the given task if leader status is held.
//
// The task should perform a bite-sized amount of work, such that it does not block for longer than necessary
// (max.poll.interval.ms in the worst-case). Ideally, the task should perform one atomic unit of work and
// return immediately. The task mays schedule work on a separate goroutine so as to avoid blocking; however,
// it must then employ a Barrier to detect impending leader revocation and wrap up any in-flight work.
func (n *neli) Background(task LeaderTask) (Pulser, error) {
	return pulse(n, task)
}
