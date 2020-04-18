package goneli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeaderElected_string(t *testing.T) {
	assert.Equal(t, "LeaderElected[]", LeaderElected{}.String())
}

func TestLeaderRevoked_string(t *testing.T) {
	assert.Equal(t, "LeaderRevoked[]", LeaderRevoked{}.String())
}
