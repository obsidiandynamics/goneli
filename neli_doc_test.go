package goneli

import (
	"log"
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	logrus "github.com/sirupsen/logrus"
)

func Example() {
	// Configure Neli.
	config := Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
	}

	// Create a new Neli curator.
	neli, err := New(config)
	if err != nil {
		panic(err)
	}

	// Starts a goroutine in the background, which will automatically terminate when Neli is closed.
	neli.Background(func() {
		log.Printf("Do important leader stuff")
		time.Sleep(100 * time.Millisecond)
	})

	// Blocks until Neli is closed.
	neli.Await()
}

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}

func Example_lowLevel() {
	// A custom logger.
	log := logrus.StandardLogger()
	log.SetLevel(logrus.TraceLevel)

	// Configure Neli.
	config := Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		Scribe: scribe.New(scribelogrus.Bind()),
	}

	// Blocking handler of leader status updates. Used to initialise state upon leader acquisition, and to wrap up
	// in-flight work before relinquishing leader status.
	eventHandler := func(e Event) {
		switch e.(type) {
		case *LeaderElected:
			log.Infof("Received event: leader elected")
		case *LeaderRevoked:
			log.Infof("Received event: leader revoked")
		}
	}

	// Create a new Neli curator.
	neli, err := New(config, eventHandler)
	if err != nil {
		panic(err)
	}

	// Pulsing is done in a separate goroutine. (We don't have to, but it's often practical to do so.)
	go func() {
		defer neli.Close()

		for {
			// Pulse our presence, allowing for some time to acquire leader status.
			// Will return instantly if already leader.
			isLeader, err := neli.Pulse(10 * time.Millisecond)
			if err != nil {
				panic(err)
			}

			// We hold leader status... let's act as one.
			if isLeader {
				log.Infof("Do important leader stuff")
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Blocks until Neli is closed.
	neli.Await()
}

func TestExample_lowLevel(t *testing.T) {
	check.RunTargetted(t, Example_lowLevel)
}
