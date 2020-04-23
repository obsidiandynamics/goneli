package goneli

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	HeartbeatTimeout      *time.Duration
}

// Validate the Config, returning an error if invalid.
func (c Config) Validate() error {
	validName := matchValidKafkaChars()
	return validation.ValidateStruct(&c,
		validation.Field(&c.KafkaConfig, validation.NotNil),
		validation.Field(&c.LeaderTopic, validation.Required, validation.Match(validName)),
		validation.Field(&c.LeaderGroupID, validation.Required, validation.Match(validName)),
		validation.Field(&c.KafkaConsumerProvider, validation.NotNil),
		validation.Field(&c.KafkaProducerProvider, validation.NotNil),
		validation.Field(&c.Scribe, validation.NotNil),
		validation.Field(&c.Name, validation.Required, validation.Match(regexp.MustCompile("^[^%]*$"))),
		validation.Field(&c.PollDuration, validation.Required, validation.Min(1*time.Millisecond)),
		validation.Field(&c.MinPollInterval, validation.Required, validation.Min(1*time.Millisecond)),
		validation.Field(&c.HeartbeatTimeout, validation.Required, validation.Min(1*time.Millisecond)),
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
		", MinPollInterval=", c.MinPollInterval,
		", HeartbeatTimeout=", c.HeartbeatTimeout, "]")
}

const (
	// DefaultPollDuration is the default value of Config.PollDuration
	DefaultPollDuration = 1 * time.Millisecond

	// DefaultMinPollInterval is the default value of Config.MinPollInterval
	DefaultMinPollInterval = 100 * time.Millisecond

	// DefaultHeartbeatTimeout is the default value of Config.HeartbeatTimeout
	DefaultHeartbeatTimeout = 5 * time.Second
)

// SetDefaults assigns the default values to optional fields.
func (c *Config) SetDefaults() {
	if c.KafkaConfig == nil {
		c.KafkaConfig = KafkaConfigMap{}
	}
	if _, ok := c.KafkaConfig["bootstrap.servers"]; !ok {
		c.KafkaConfig["bootstrap.servers"] = "localhost:9092"
	}
	if c.LeaderGroupID == "" {
		c.LeaderGroupID = Sanitise(filepath.Base(os.Args[0]))
	}
	if c.LeaderTopic == "" {
		c.LeaderTopic = c.LeaderGroupID + ".neli"
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
		c.Name = fmt.Sprintf("%s_%d_%d", Sanitise(getString("localhost", os.Hostname)), os.Getpid(), time.Now().Unix())
	}

	defaultDuration(&c.PollDuration, DefaultPollDuration)
	defaultDuration(&c.MinPollInterval, DefaultMinPollInterval)
	defaultDuration(&c.HeartbeatTimeout, DefaultHeartbeatTimeout)
}

const validKafkaNameChars = "a-zA-Z0-9\\._\\-"

// Obtains a regular expression matcher for the set of valid characters
// allowed in Kafka resource names.
func matchValidKafkaChars() *regexp.Regexp {
	return regexp.MustCompile("^[" + validKafkaNameChars + "]*$")
}

// Sanitise cleans up the given name by replacing all characters in the pattern [^a-zA-Z0-9\\._\\-]
// with underscores.
func Sanitise(name string) string {
	return string(regexp.MustCompile("[^"+validKafkaNameChars+"]").ReplaceAll([]byte(name), []byte("_")))
}

type stringGetter func() (string, error)

func getString(def string, stringGetter stringGetter) string {
	str, err := stringGetter()
	if err != nil {
		return def
	}
	return str
}
