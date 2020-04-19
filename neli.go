package goneli

import (
	"context"
	"fmt"
	"sync"
	"time"

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
}

type neli struct {
	config       Config
	scribe       scribe.Scribe
	consumer     KafkaConsumer
	pollDeadline concurrent.Deadline
	isLeader     concurrent.AtomicCounter
	eventHandler EventHandler
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

func New(config Config, eventHandler EventHandler) (Neli, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	n := &neli{
		config:       config,
		scribe:       config.Scribe,
		isLeader:     concurrent.NewAtomicCounter(),
		eventHandler: eventHandler,
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

func (n *neli) IsLeader() bool {
	return n.isLeader.GetInt() == 1
}

func (n *neli) Pulse(timeout time.Duration) (bool, error) {
	ctx, cancel := concurrent.Timeout(context.Background(), timeout)
	defer cancel()
	return n.PulseCtx(ctx)
}

func (n *neli) PulseCtx(ctx context.Context) (bool, error) {
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
			error = fmt.Errorf("cannot pulse in non-live state")
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

func (n *neli) Deadline() concurrent.Deadline {
	return n.pollDeadline
}

func onAssigned(n *neli, assigned kafka.AssignedPartitions) {
	n.logger().T()("Assigned partitions: %s", assigned)
	if containsPartition(assigned.Partitions, 0) {
		n.isLeader.Set(1)
		n.logger().I()("Elected as leader")
		n.eventHandler(&LeaderElected{})
	}
}

func onRevoked(n *neli, revoked kafka.RevokedPartitions) {
	n.logger().T()("Revoked partitions: %s", revoked)
	if containsPartition(revoked.Partitions, 0) {
		n.logger().I()("Lost leader status")
		n.isLeader.Set(0)
		n.eventHandler(&LeaderRevoked{})
	}
}

func (n *neli) State() State {
	return n.state.Get().(State)
}

func (n *neli) Close() error {
	n.stateMutex.Lock()
	defer n.stateMutex.Unlock()
	defer n.state.Set(Closed)

	n.state.Set(Closing)
	return n.consumer.Close()
}

func (n *neli) Await() {
	n.state.Await(concurrent.RefEqual(Closed), concurrent.Indefinitely)
}
