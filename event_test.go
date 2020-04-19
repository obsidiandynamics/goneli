package goneli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeaderAcquired_string(t *testing.T) {
	assert.Equal(t, "LeaderAcquired[]", LeaderAcquired{}.String())
}

func TestLeaderRevoked_string(t *testing.T) {
	assert.Equal(t, "LeaderRevoked[]", LeaderRevoked{}.String())
}

func TestLeaderFenced_string(t *testing.T) {
	assert.Equal(t, "LeaderFenced[]", LeaderFenced{}.String())
}
