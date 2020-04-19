<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goneli/images/goneli-logo.png" width="90px" alt="logo"/> goNELI
===
![Go version](https://img.shields.io/github/go-mod/go-version/obsidiandynamics/goneli)
[![Build](https://travis-ci.org/obsidiandynamics/goneli.svg?branch=master) ](https://travis-ci.org/obsidiandynamics/goneli#)
![Release](https://img.shields.io/github/v/release/obsidiandynamics/goneli?color=ff69b4)
[![Codecov](https://codecov.io/gh/obsidiandynamics/goneli/branch/master/graph/badge.svg)](https://codecov.io/gh/obsidiandynamics/goneli)
[![Go Report Card](https://goreportcard.com/badge/github.com/obsidiandynamics/goneli)](https://goreportcard.com/report/github.com/obsidiandynamics/goneli)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/obsidiandynamics/goneli.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/obsidiandynamics/goneli/alerts/)
[![GoDoc Reference](https://img.shields.io/badge/docs-GoDoc-blue.svg)](https://pkg.go.dev/github.com/obsidiandynamics/goneli?tab=doc)

Implementation of the [NELI](https://github.com/obsidiandynamics/neli) leader election protocol for Go and Kafka. goNELI encapsulates the 'simplified' variation of the protocol, running in exclusive mode over a group of contending processes.

# Getting started
## Add the dependency
```sh
go get -u github.com/obsidiandynamics/goneli
```

## Go import
```go
import "github.com/obsidiandynamics/goneli"
```

## Basic leader election
This is the easiest way of getting started with leader election. A task will be continuously invoked in the background while the `Neli` instance is the leader of its group.

```go
// Create a new Neli curator.
neli, err := New(Config{
  KafkaConfig: KafkaConfigMap{
    "bootstrap.servers": "localhost:9092",
  },
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
  time.Sleep(100 * time.Millisecond)
})

// Blocks until Neli is closed or an unrecoverable error occurs.
panic(p.Await())
```

## Full control
Sometimes more control is needed. For example —

* Configuring a custom logger, based on your application's needs.
* Setting up a barrier to synchronize leadership transition, so that the new leader does not step in until the outgoing leader has completed all of its backlogged work.
* Pulsing of the `Neli` instance from your own Goroutine.

```go
// Additional imports for the logger and Scribe bindings.
import (
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	logrus "github.com/sirupsen/logrus"
)
```

```go
// Bootstrap a custom logger.
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
// in-flight work before relinquishing leader status. Kafka will suspend rebalancing for as long as the barrier
// is blocked.
barrier := func(e Event) {
  switch e.(type) {
  case *LeaderElected:
    // Initialise state.
    log.Infof("Received event: leader elected")
  case *LeaderRevoked:
    // Clean up any pending work.
    log.Infof("Received event: leader revoked")
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
```