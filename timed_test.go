package goneli

import (
	"sync"
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/stretchr/testify/require"
)

func TestPerformTimed_noError(t *testing.T) {
	done, err := performTimed(void(func() {}), 10*time.Second)
	require.True(t, done)
	require.Nil(t, err)
}

func TestPerformTimed_withError(t *testing.T) {
	done, err := performTimed(func() error {
		return check.ErrSimulated
	}, 10*time.Second)
	require.True(t, done)
	require.Equal(t, check.ErrSimulated, err)
}

func TestPerformTimed_withTimeout(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	done, err := performTimed(func() error {
		wg.Wait()
		return nil
	}, 1*time.Millisecond)
	require.False(t, done)
	require.Nil(t, err)

	wg.Done()
}
