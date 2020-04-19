package goneli

import (
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Neli is a curator for leader election.
type Neli interface {
	Close() error
	IsLeader() bool
	Pulse() (bool, error)
	Deadline() concurrent.Deadline
}

type neli struct {
	config       Config
	scribe       scribe.Scribe
	consumer     KafkaConsumer
	pollDeadline concurrent.Deadline
	isLeader     concurrent.AtomicCounter
	eventHandler EventHandler
}

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

func (n *neli) Pulse() (bool, error) {
	var error error
	n.pollDeadline.TryRun(func() {
		// Polling is just to indicate consumer liveness; we don't actually care about the messages consumed.
		n.logger().T()("Polling... (is leader: %v)", n.IsLeader())
		_, err := n.consumer.ReadMessage(*n.config.PollDuration)
		if err != nil {
			if isFatalError(err) {
				error = err
				n.logger().E()("Fatal error during poll: %v", err)
			} else if !isTimedOutError(err) {
				n.logger().W()("Retryable error during poll: %v", err)
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

func (n *neli) Close() error {
	return n.consumer.Close()
}
