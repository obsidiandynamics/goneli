package goneli

import (
	"fmt"

	"github.com/obsidiandynamics/libstdgo/concurrent"
)

// Barrier is a callback function for handling Neli events during group rebalancing.
type Barrier func(e Event)

// NopBarrier returns a no-op barrier implementation.
func NopBarrier() Barrier {
	return func(e Event) {
		concurrent.Nop()
	}
}

// Event encapsulates a Neli event.
type Event interface {
	fmt.Stringer
}

// LeaderAcquired is emitted upon successful acquisition of leader status, either through explicit partition
// assignment, or when a heartbeat is eventually received following a previously fenced state.
//
// An application responding to a LeaderAcquired may initialise any state necessary to perform work as a leader.
type LeaderAcquired struct{}

// String obtains a textual representation of the LeaderAcquired event.
func (e LeaderAcquired) String() string {
	return fmt.Sprint("LeaderAcquired[]")
}

// LeaderRevoked is emitted when the leader status has been revoked.
//
// An application reacting to a LeaderRevoked event may block the Barrier callback until it wraps up any in-flight
// activity. Only upon returning from the callback, will a new leader be elected.
type LeaderRevoked struct{}

// String obtains a textual representation of the LeaderRevoked event.
func (e LeaderRevoked) String() string {
	return fmt.Sprint("LeaderRevoked[]")
}

// LeaderFenced is emitted when a suspected network partition occurs, and the leader voluntarily fences itself.
//
// An application reacting to a LeaderFenced event must immediately terminate any ongoing activity, on the assumption
// that another leader may be imminently elected. Unlike the handling of LeaderRevoked, blocking in the Barrier callback
// will not prevent a new leader from being elected.
type LeaderFenced struct{}

// String obtains a textual representation of the LeaderFenced event.
func (e LeaderFenced) String() string {
	return fmt.Sprint("LeaderFenced[]")
}
