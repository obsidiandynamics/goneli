package goneli

import (
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	logrus "github.com/sirupsen/logrus"
)

func Example() {
	// A logger.
	log := logrus.StandardLogger()
	log.SetLevel(logrus.TraceLevel)

	// Configure NELI.
	config := Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		Scribe: scribe.New(scribelogrus.Bind()),
	}

	// Create a new Neli curator.
	neli, err := New(config, func(e Event) {
		switch e.(type) {
		case *LeaderElected:
			log.Infof("Received event: leader elected")
		case *LeaderRevoked:
			log.Infof("Received event: leader revoked")
		}
	})
	if err != nil {
		panic(err)
	}

	// Pulsing is done in a separate goroutine.
	go func() {
		for {
			isLeader, err := neli.Pulse()
			if err != nil {
				panic(err)
			}

			if isLeader {
				log.Infof("Do leader stuff")
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Blocks until Neli is closed.
	neli.Await()
}

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}
