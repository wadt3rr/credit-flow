package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Postgres PostgresConfig `mapstructure:"postgres"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Outbox   OutboxConfig   `mapstructure:"outbox"`
}

type PostgresConfig struct {
	Host     string `mapstructure:"host"`
	Port     uint16 `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	SSLMode  string `mapstructure:"ssl_mode"`
}

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
}

type OutboxConfig struct {
	PollInterval time.Duration `mapstructure:"poll_interval"`
}

func Load(configPath string) (*Config, error) {
	const op = "config.Load"

	v := viper.New()

	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("%s: read config: %w", op, err)
	}

	v.SetDefault("postgres.ssl_mode", "disable")
	if err := v.BindEnv("postgres.password"); err != nil {
		return nil, fmt.Errorf("%s: bind postgres password: %w", op, err)
	}
	var cfg Config

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("%s: unmarshal config: %w", op, err)
	}

	cfg.Postgres.Password = v.GetString("postgres.password")
	if cfg.Postgres.Password == "" {
		return nil, fmt.Errorf("%s: postgres password is required", op)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: validate config: %w", op, err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Postgres.Host == "" {
		return fmt.Errorf("postgres host is required")
	}
	if c.Postgres.Port == 0 {
		return fmt.Errorf("postgres port is required")
	}
	if c.Postgres.User == "" {
		return fmt.Errorf("postgres user is required")
	}
	if c.Postgres.Database == "" {
		return fmt.Errorf("postgres database is required")
	}
	if c.Postgres.Password == "" {
		return fmt.Errorf("postgres password is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one kafka broker is required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic is required")
	}
	if c.Outbox.PollInterval <= 0 {
		return fmt.Errorf("outbox poll interval must be positive")
	}

	return nil
}
