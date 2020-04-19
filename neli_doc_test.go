package goneli

import (
	"log"
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
)

func Example() {
	// Configure NELI.
	config := Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
	}

	// Create a new NELI curator.
	neli, err := New(config, func(e Event) {
		switch e.(type) {
		case *LeaderElected:
			log.Printf("Received event: leader elected")
		case *LeaderRevoked:
			log.Printf("Received event: leader revoked")
		}
	})
	if err != nil {
		panic(err)
	}

	// Pulsing may be done in a separate goroutine.
	go func() {
		for {
			isLeader, err := neli.Pulse()
			if err != nil {
				panic(err)
			}

			if isLeader {
				log.Printf("Do leader stuff")
			}
		}
	}()

	time.Sleep(1 * time.Hour)
	neli.Close()
}

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}
