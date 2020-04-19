package int

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/obsidiandynamics/goneli"
	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/diags"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/obsidiandynamics/libstdgo/scribe/overlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testKafkaNamespace = "neli_test"
	testTopic          = testKafkaNamespace + ".topic"
	testGroupID        = testKafkaNamespace + ".group"
)

var testBootstrapServers = getEnv("GONELI_KAFKA_URL", "localhost:9092")

func getEnv(key string, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

const waitTimeout = 90 * time.Second

var logger = overlog.New(overlog.StandardFormat())
var scr = scribe.New(overlog.Bind(logger))

func wait(t check.Tester) check.Timesert {
	return check.Wait(t, waitTimeout)
}

func TestOneNode(t *testing.T) {
	test(t, 1, 5*time.Second)
}

func TestFourNodes(t *testing.T) {
	test(t, 4, 5*time.Second)
}

func test(t *testing.T, numNodes int, spawnInterval time.Duration) {
	check.RequireLabel(t, "int")
	installSigQuitHandler()

	ns := make([]Neli, numNodes)
	nsMutex := &sync.Mutex{}
	pulsers := make([]Pulser, numNodes)
	// Start nodes at a set interval.
	for i := 0; i < numNodes; i++ {
		config := Config{
			Name:   fmt.Sprintf("neli-#%d", i+1),
			Scribe: scribe.New(overlog.Bind(logger)),
			KafkaConfig: KafkaConfigMap{
				"bootstrap.servers": testBootstrapServers,
			},
			LeaderTopic:     testTopic,
			LeaderGroupID:   testGroupID,
			MinPollInterval: Duration(100 * time.Millisecond),
		}
		config.Scribe.SetEnabled(scribe.Trace)

		scr.I()("Starting Neli %d/%d", i+1, numNodes)
		n, err := New(config, func(e Event) {
			switch e.(type) {
			case *LeaderAcquired:
				scr.I()("Elected leader %s", config.Name)
				assert.Equal(t, 1, countLeaders(ns, nsMutex))
			case *LeaderRevoked:
				scr.I()("Revoked leader %s", config.Name)
				assert.Equal(t, 0, countLeaders(ns, nsMutex))
			case *LeaderFenced:
				scr.I()("Fenced leader %s", config.Name)
				assert.Equal(t, 0, countLeaders(ns, nsMutex))
			default:
				scr.E()("Unexpected event %v (%T)", e)
				assert.Fail(t, "Unexpected event")
			}
		})
		require.Nil(t, err)

		// The ns slice is also accessed from the barrier callback to perform a leader count assertion.
		nsMutex.Lock()
		ns[i] = n
		nsMutex.Unlock()

		pulser, err := n.Background(func() {})
		require.Nil(t, err)
		pulsers[i] = pulser

		scr.I()("Sleeping")
		sleepWithDeadline(spawnInterval)
	}

	// Wait for a leader to have formed.
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.Equal(t, 1, countLeaders(ns, nsMutex))
	})

	// Stop nodes in the order they were started.
	for i := 0; i < numNodes; i++ {
		scr.I()("Stopping Neli %d/%d", i+1, numNodes)
		err := ns[i].Close()
		require.Nil(t, err)
		if i != numNodes-1 {
			sleepWithDeadline(spawnInterval)
		}
	}

	// Await pulsers.
	for i, h := range pulsers {
		scr.I()("Awaiting pulser %d/%d", i+1, numNodes)
		assert.Nil(t, h.Await())
	}

	scr.I()("Done")
}

func countLeaders(ns []Neli, nsMutex *sync.Mutex) int {
	nsMutex.Lock()
	defer nsMutex.Unlock()
	leaders := 0
	for _, n := range ns {
		if n != nil && n.State() == Live && n.IsLeader() {
			leaders++
		}
	}
	return leaders
}

func sleepWithDeadline(duration time.Duration) {
	beforeSleep := time.Now()
	time.Sleep(duration)
	if elapsed := time.Now().Sub(beforeSleep); elapsed > 2*duration {
		scr.W()("Sleep deadline exceeded; expected %v but slept for %v", duration, elapsed)
	}
}

func installSigQuitHandler() {
	sig := make(chan os.Signal, 1)
	go func() {
		signal.Notify(sig, syscall.SIGQUIT)
		select {
		case <-sig:
			scr.I()("Stack\n%s", diags.DumpAllStacks())
		}
	}()
}
