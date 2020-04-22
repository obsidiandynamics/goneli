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
	// Create a new Neli curator.
	neli, err := New(Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		LeaderGroupID: "my-app-name.group",
		LeaderTopic:   "my-app-name.topic",
	})
	if err != nil {
		panic(err)
	}

	// Starts a pulser Goroutine in the background, which will automatically terminate when Neli is closed.
	p, _ := neli.Background(func() {
		// An activity performed by the client application if it is the elected leader. This task should
		// perform a small amount of work that is exclusively attributable to a leader, and return immediately.
		// For as long as the associated Neli instance is the leader, this task will be invoked repeatedly;
		// therefore, it should break down any long-running work into bite-sized chunks that can be safely
		// performed without causing excessive blocking.
		log.Printf("Do important leader stuff")
		time.Sleep(100 * time.Millisecond)
	})

	// Blocks until Neli is closed or an unrecoverable error occurs.
	panic(p.Await())
}

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}

func Example_secureBroker() {
	// Connects to a secure broker over TLS, using SASL authentication.
	neli, err := New(Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
			"security.protocol": "sasl_ssl",
			"ssl.ca.location":   "ca-cert.pem",
			"sasl.mechanism":    "SCRAM-SHA-256",
			"sasl.username":     "user",
			"sasl.password":     "secret",
		},
		LeaderGroupID: "my-app-name.group",
		LeaderTopic:   "my-app-name.topic",
	})
	if err != nil {
		panic(err)
	}

	p, _ := neli.Background(func() {
		log.Printf("Do important leader stuff")
		time.Sleep(100 * time.Millisecond)
	})

	panic(p.Await())
}

func TestExample_secureBroker(t *testing.T) {
	check.RunTargetted(t, Example_secureBroker)
}

func Example_lowLevel() {
	// Bootstrap a custom logger.
	log := logrus.StandardLogger()
	log.SetLevel(logrus.TraceLevel)

	// Configure Neli.
	config := Config{
		KafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		LeaderGroupID: "my-app-name.group",
		LeaderTopic:   "my-app-name.topic",
		Scribe:        scribe.New(scribelogrus.Bind()),
	}

	// Handler of leader status updates. Used to initialise state upon leader acquisition, and to
	// wrap up in-flight work upon loss of leader status.
	barrier := func(e Event) {
		switch e.(type) {
		case *LeaderAcquired:
			// The application may initialise any state necessary to perform work as a leader.
			log.Infof("Received event: leader elected")
		case *LeaderRevoked:
			// The application may block the Barrier callback until it wraps up any in-flight
			// activity. Only upon returning from the callback, will a new leader be elected.
			log.Infof("Received event: leader revoked")
		case *LeaderFenced:
			// The application must immediately terminate any ongoing activity, on the assumption
			// that another leader may be imminently elected. Unlike the handling of LeaderRevoked,
			// blocking in the Barrier callback will not prevent a new leader from being elected.
			log.Infof("Received event: leader fenced")
		}
	}

	// Create a new Neli curator, supplying the barrier as an optional argument.
	neli, err := New(config, barrier)
	if err != nil {
		panic(err)
	}

	// Pulsing is done in a separate Goroutine. (We don't have to, but it's often practical to do so.)
	go func() {
		defer neli.Close()

		for {
			// Pulse our presence, allowing for some time to acquire leader status.
			// Will return instantly if already leader.
			isLeader, err := neli.Pulse(10 * time.Millisecond)
			if err != nil {
				// Only fatal errors are returned from Pulse().
				panic(err)
			}

			if isLeader {
				// We hold leader status... can safely do some work.
				// Avoid blocking for too long, otherwise we may miss a poll and lose leader status.
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
