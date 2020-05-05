package goneli

import (
	"errors"
	"time"

	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/scribe"
)

var errPerformWithNoError = errors.New("")

func void(f func()) func() error {
	return func() error {
		f()
		return nil
	}
}

func performTimed(logger scribe.Logger, opName string, op func() error, timeout time.Duration) (bool, error) {
	errorRef := concurrent.NewAtomicReference()
	go func() {
		err := op()
		if err != nil {
			errorRef.Set(err)
		} else {
			errorRef.Set(errPerformWithNoError)
		}
	}()

	res := errorRef.Await(concurrent.RefNot(concurrent.RefNil()), timeout)
	if res == nil {
		logger("Operation '%s' failed to complete within %v", opName, timeout)
		return false, nil
	}
	if err := res.(error); err != errPerformWithNoError {
		return true, err
	}
	return true, nil
}
