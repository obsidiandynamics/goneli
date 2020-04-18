package goneli

import (
	"testing"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/assert"
)

func TestDefaultKafkaConsumerProvider(t *testing.T) {
	c := Config{}
	c.SetDefaults()

	cons, err := c.KafkaConsumerProvider(&KafkaConfigMap{})
	assert.Nil(t, cons)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "Required property")
	}
}

func TestGetString(t *testing.T) {
	assert.Equal(t, "some-default", getString("some-default", func() (string, error) { return "", check.ErrSimulated }))
	assert.Equal(t, "some-string", getString("some-default", func() (string, error) { return "some-string", nil }))
}

func TestConfigString(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	assert.Contains(t, cfg.String(), "Config[")
}

func TestValidateConfig_valid(t *testing.T) {
	cfg := Config{
		KafkaConfig:           KafkaConfigMap{},
		LeaderTopic:           "leader-topic",
		LeaderGroupID:         "leader-group-d",
		KafkaConsumerProvider: StandardKafkaConsumerProvider(),
		Scribe:                scribe.New(scribe.StandardBinding()),
		Name:                  "name",
	}
	cfg.SetDefaults()
	assert.Nil(t, cfg.Validate())
}

func TestValidateConfig_invalidLimits(t *testing.T) {
	cfg := Config{
		KafkaConfig:           KafkaConfigMap{},
		LeaderTopic:           "leader-topic",
		LeaderGroupID:         "leader-group-id",
		KafkaConsumerProvider: StandardKafkaConsumerProvider(),
		Scribe:                scribe.New(scribe.StandardBinding()),
		Name:                  "name",
		MinPollInterval:       Duration(0),
	}
	cfg.SetDefaults()
	assert.NotNil(t, cfg.Validate())
}

func TestValidateConfig_default(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	assert.Nil(t, cfg.Validate())
}
