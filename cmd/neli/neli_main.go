package main

import (
	"flag"
	"log"
	"time"

	"github.com/obsidiandynamics/goneli"
)

func main() {
	var leaderGroupID, leaderTopic, kafkaBoostrapServers string
	flag.StringVar(&leaderGroupID, "group", "goneli.group", "Leader group ID")
	flag.StringVar(&leaderTopic, "topic", "goneli.topic", "Leader topic")
	flag.StringVar(&kafkaBoostrapServers, "kafka", "localhost:9092", "Kafka bootstrap servers")
	flag.Parse()
	log.Printf("group: %s", leaderGroupID)
	log.Printf("topic: %s", leaderTopic)
	log.Printf("kafka: %s", kafkaBoostrapServers)

	// Create a new Neli curator.
	neli, err := goneli.New(goneli.Config{
		KafkaConfig: goneli.KafkaConfigMap{
			"bootstrap.servers":    kafkaBoostrapServers,
			"max.poll.interval.ms": 60000,
		},
		LeaderGroupID:   leaderGroupID,
		LeaderTopic:     leaderTopic,
		MinPollInterval: goneli.Duration(500 * time.Millisecond),
	})
	if err != nil {
		panic(err)
	}

	// Starts a pulser Goroutine in the background, which will automatically terminate when Neli is closed.
	p, _ := neli.Background(func() {
		// An activity performed by the client application if it is the elected leader. This task should
		// perform a small amount of work that is exclusively attributable to a leader, and return immediately. For as
		// long as the associated Neli instance is the leader, this task will be invoked repeatedly; therefore, it should
		// break down any long-running work into bite-sized chunks that can be safely performed without causing excessive
		// blocking.
		log.Printf("Do important leader stuff")
		time.Sleep(500 * time.Millisecond)
	})

	// Blocks until Neli is closed or an unrecoverable error occurs.
	panic(p.Await())
}
