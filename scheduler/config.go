package scheduler

import (
	"encoding/json"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
)

// Config represents the configuration of the Mesos scheduler.
type Config struct {
	// Framework tasks user.
	User string `json:"user,omitempty"`
	// Framework name.
	Name string `json:"name,omitempty"`
	// Framework role.
	Role string `json:"role,omitempty"`
	// Mesos URL
	MesosURL string `json:"mesosUrl,omitempty"`
	// Principal to authenticate against Mesos.
	Principal string `json:"principal,omitempty"`
	// Credentials to authenticate against Mesos
	Credentials *credentials `json:"credentials,omitempty"`
	// Authentication method (AuthModeBasic is only supported)
	AuthMode string `json:"authMode,omitempty"`
	// Codec to be used when talking to Mesos.
	Codec codec `json:"codec,omitempty"`
	// Framework connection to Mesos timeout.
	Timeout time.Duration
	// Framework failover timeout.
	FailoverTimeout time.Duration
	// Checkpoint framework tasks.
	Checkpoint bool `json:"checkpoint,omitempty"`
	// Hostname advertised to Mesos?
	Hostname string `json:"hostname,omitempty"`
	// Framework labels?
	Labels Labels `json:"labels,omitempty"`
	// Framework verbosity enabled?
	Verbose bool `json:"verbose,omitempty"`
	// Number of revive messages that may be sent in a burst within revive-wait period.
	ReviveBurst int `json:"reviveBurst,omitempty"`
	// Wait this long to fully recharge revive-burst quota.
	ReviveWait time.Duration
	// URI path to metrics endpoint.
	Metrics *metrics `json:"metrics,omitempty"`
	// Max length of time to refuse future offers.
	MaxRefuseSeconds time.Duration
	// Duration between job (internal service) restarts between failures
	JobRestartDelay time.Duration
}

const AuthModeBasic = "basic"

// DefaultConfig returns the default configuration for the Mesos framework
func DefaultConfig() *Config {
	timeout, _ := time.ParseDuration("20s")
	failoverTimeout, _ := time.ParseDuration("10s")
	reviveWait, _ := time.ParseDuration("1s")
	maxRefuseSeconds, _ := time.ParseDuration("5s")
	jobRestartDelay, _ := time.ParseDuration("5s")

	return &Config{
		MesosURL:         "http://:5050/api/v1/scheduler",
		Name:             "vk_mesos",
		Role:             "*",
		Credentials:      &credentials{},
		Codec:            codec{Codec: codecs.ByMediaType[codecs.MediaTypeProtobuf]},
		Timeout:          timeout,
		FailoverTimeout:  failoverTimeout,
		Checkpoint:       true,
		ReviveBurst:      3,
		ReviveWait:       reviveWait,
		MaxRefuseSeconds: maxRefuseSeconds,
		JobRestartDelay:  jobRestartDelay,
		Metrics: &metrics{
			address: "localhost",
			port:    64009,
			path:    "/metrics",
		},
		User:    "root",
		Verbose: true,
	}
}

type metrics struct {
	address string
	path    string
	port    int
}

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c *Config) UnmarshalJSON(b []byte) error {
	type config2 Config
	if err := json.Unmarshal(b, (*config2)(c)); err != nil {
		return err
	}

	var rawStrings map[string]interface{}
	if err := json.Unmarshal(b, &rawStrings); err != nil {
		return err
	}

	if s, err := rawStrings["failoverTimeoutDuration"]; err {
		d, err := time.ParseDuration(s.(string))
		if err != nil {
			return err
		}
		c.FailoverTimeout = d
	}
	//TODO: add other durations to parse here
	return nil
}
