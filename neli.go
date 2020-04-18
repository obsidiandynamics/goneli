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
}

type neli struct {
	config       Config
	scribe       scribe.Scribe
	consumer     KafkaConsumer
	pollDeadline concurrent.Deadline
	isLeader     concurrent.AtomicCounter
}

func New(config Config) (Neli, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	n := &neli{
		config:   config,
		scribe:   config.Scribe,
		isLeader: concurrent.NewAtomicCounter(),
	}

	consumerConfigs := copyKafkaConfig(n.config.KafkaConfig)
	err := setKafkaConfigs(consumerConfigs, KafkaConfigMap{
		"group.id":           n.config.LeaderGroupID,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	defer n.cleanupFailedStart()

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

	return n, nil
}

func (h *neli) logger() scribe.StdLogAPI {
	return h.scribe.Capture(h.scene())
}

func (h *neli) scene() scribe.Scene {
	return scribe.Scene{Fields: scribe.Fields{"name": h.config.Name}}
}

func (h *neli) cleanupFailedStart() {
	if h.consumer != nil {
		h.consumer.Close()
	}
}

func (h *neli) IsLeader() bool {
	return h.isLeader.GetInt() == 1
}

func onAssigned(h *neli, assigned kafka.AssignedPartitions) {
	h.logger().T()("Assigned partitions: %s", assigned)
	if containsPartition(assigned.Partitions, 0) {
		h.isLeader.Set(1)
		h.logger().I()("Elected as leader")
		// h.eventHandler(&LeaderElected{newLeaderID})
	}
}

func onRevoked(h *neli, revoked kafka.RevokedPartitions) {
	h.logger().T()("Revoked partitions: %s", revoked)
	if containsPartition(revoked.Partitions, 0) {
		h.logger().I()("Lost leader status")
		h.isLeader.Set(1)
		// h.eventHandler(&LeaderRevoked{})
	}
}

func (h *neli) Close() error {
	return h.consumer.Close()
}
