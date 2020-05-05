package goneli

import (
	"sync"
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/require"
)

func TestPerformTimed_noError(t *testing.T) {
	m := scribe.NewMock()
	scr := scribe.New(m.Factories())
	done, err := performTimed(scr.W(), "some-op", void(func() {}), 10*time.Second)
	require.True(t, done)
	require.Nil(t, err)
	m.Entries().Assert(t, scribe.Count(0))
}

func TestPerformTimed_withError(t *testing.T) {
	m := scribe.NewMock()
	scr := scribe.New(m.Factories())
	done, err := performTimed(scr.W(), "some-op", func() error {
		return check.ErrSimulated
	}, 10*time.Second)
	require.True(t, done)
	require.Equal(t, check.ErrSimulated, err)
	m.Entries().Assert(t, scribe.Count(0))
}

func TestPerformTimed_withTimeout(t *testing.T) {
	m := scribe.NewMock()
	scr := scribe.New(m.Factories())
	wg := sync.WaitGroup{}
	wg.Add(1)

	done, err := performTimed(scr.W(), "some-op", func() error {
		wg.Wait()
		return nil
	}, 1*time.Millisecond)
	require.False(t, done)
	require.Nil(t, err)
	m.Entries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageEqual("Operation 'some-op' failed to complete within 1ms")).
		Assert(t, scribe.Count(1))

	wg.Done()
}
