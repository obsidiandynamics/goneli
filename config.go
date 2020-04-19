package goneli

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/obsidiandynamics/libstdgo/scribe"
)

// Duration is a convenience for deriving a pointer from a given Duration argument.
func Duration(d time.Duration) *time.Duration {
	return &d
}

func defaultDuration(d **time.Duration, def time.Duration) {
	if *d == nil {
		*d = &def
	}
}

// KafkaConfigMap represents the Kafka key-value configuration.
type KafkaConfigMap map[string]interface{}

// Config encapsulates configuration for Neli.
type Config struct {
	KafkaConfig           KafkaConfigMap
	LeaderTopic           string
	LeaderGroupID         string
	KafkaConsumerProvider KafkaConsumerProvider
	KafkaProducerProvider KafkaProducerProvider
	Scribe                scribe.Scribe
	Name                  string
	PollDuration          *time.Duration
	MinPollInterval       *time.Duration
}

// Validate the Config, returning an error if invalid.
func (c Config) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.KafkaConfig, validation.NotNil),
		validation.Field(&c.LeaderTopic, validation.Required),
		validation.Field(&c.LeaderGroupID, validation.Required),
		validation.Field(&c.KafkaConsumerProvider, validation.NotNil),
		validation.Field(&c.KafkaProducerProvider, validation.NotNil),
		validation.Field(&c.Scribe, validation.NotNil),
		validation.Field(&c.Name, validation.Required),
		validation.Field(&c.PollDuration, validation.Required, validation.Min(1*time.Millisecond)),
		validation.Field(&c.MinPollInterval, validation.Required, validation.Min(1*time.Millisecond)),
	)
}

// Obtains a textual representation of the configuration.
func (c Config) String() string {
	return fmt.Sprint(
		"Config[KafkaConfig=", c.KafkaConfig,
		", LeaderTopic=", c.LeaderTopic,
		", LeaderGroupID=", c.LeaderGroupID,
		", KafkaConsumerProvider=", c.KafkaConsumerProvider,
		", KafkaProducerProvider=", c.KafkaProducerProvider,
		", Scribe=", c.Scribe,
		", Name=", c.Name,
		", PollDuration=", c.PollDuration,
		", MinPollInterval=", c.MinPollInterval, "]")
}

// SetDefaults assigns the default values to optional fields.
func (c *Config) SetDefaults() {
	if c.KafkaConfig == nil {
		c.KafkaConfig = KafkaConfigMap{}
	}
	if _, ok := c.KafkaConfig["bootstrap.servers"]; !ok {
		c.KafkaConfig["bootstrap.servers"] = "localhost:9092"
	}
	if c.LeaderGroupID == "" {
		c.LeaderGroupID = filepath.Base(os.Args[0])
	}
	if c.KafkaConsumerProvider == nil {
		c.KafkaConsumerProvider = StandardKafkaConsumerProvider()
	}
	if c.KafkaProducerProvider == nil {
		c.KafkaProducerProvider = StandardKafkaProducerProvider()
	}
	if c.Scribe == nil {
		c.Scribe = scribe.New(scribe.StandardBinding())
	}
	if c.Name == "" {
		c.Name = fmt.Sprintf("%s~%d~%d", getString("localhost", os.Hostname), os.Getpid(), time.Now().Unix())
	}

	defaultDuration(&c.PollDuration, 1*time.Millisecond)
	defaultDuration(&c.MinPollInterval, 100*time.Millisecond)
}

type stringGetter func() (string, error)

func getString(def string, stringGetter stringGetter) string {
	str, err := stringGetter()
	if err != nil {
		return def
	}
	return str
}
