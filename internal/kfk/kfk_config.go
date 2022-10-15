package kfk

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

const (
	KAFKA_BROKERS   = "KAFKA_BROKERS"
	KAFKA_TOPIC_MS  = "KAFKA_TOPIC_MAILSENDER"
	KAFKA_TOPIC_PRF = "KAFKA_TOPIC_PROFILE"
	KAFKA_GROUPID   = "KAFKA_GROUPID"
)

type Config struct {
	brokers  []string
	topicMS  string
	topicPrf string
	groupId  string
}

func (cfgKfk *Config) GetConfig() error {
	var ok bool

	brk, ok := os.LookupEnv(KAFKA_BROKERS)
	cfgKfk.brokers = []string{brk}

	if !ok {
		return fmt.Errorf("kafka broker not defined")
	}

	if cfgKfk.topicMS, ok = os.LookupEnv(KAFKA_TOPIC_MS); !ok {
		return fmt.Errorf("KAFKA_TOPIC_MS not defined")
	}

	if cfgKfk.topicPrf, ok = os.LookupEnv(KAFKA_TOPIC_PRF); !ok {
		return fmt.Errorf("KAFKA_TOPIC_PRF not defined")
	}

	if cfgKfk.groupId, ok = os.LookupEnv(KAFKA_GROUPID); !ok {
		return fmt.Errorf("KAFKA_GROUPID not defined")
	}

	zap.S().Debugf("kafka Config %v\n", cfgKfk)

	return nil
}
