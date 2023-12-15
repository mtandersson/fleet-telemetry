package nats

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/nats-io/nats.go"
	"github.com/teslamotors/fleet-telemetry/metrics"
	"github.com/teslamotors/fleet-telemetry/telemetry"
)

type Producer struct {
	logger  *logrus.Logger
	nc      *nats.Conn
	channel string
}

type Config struct {
	// Verbose controls if verbose logging is enabled for the socket.
	Verbose  bool   `json:"verbose"`
	Host     string `json:"host`
	User     string `json:"user, omitempty"`
	Password string `json:"password"`
	Channel  string `json:"channel"`
}

// NewProtoLogger initializes the parameters for protobuf payload logging
func NewProducer(_ctx context.Context, config *Config, _metrics metrics.MetricCollector, _namespace string, logger *logrus.Logger) (telemetry.Producer, error) {
	logger.Info("nats producer ", config.Verbose)
	nc, err := nats.Connect(config.Host, nats.UserInfo(config.User, config.Password))
	return &Producer{
		logger:  logger,
		nc:      nc,
		channel: config.Channel,
	}, err

}

// Produce sends the data to the logger
func (p *Producer) Produce(entry *telemetry.Record) {
	if entry.TxType != "V" {
		p.logger.Infof("Received message with type %s %v %s\n", entry.Vin, entry.Metadata(), entry.Payload())
		return
	}

	err := p.nc.Publish(p.channel, entry.Payload())

	if err != nil {
		p.logger.Errorf("nats_publish_error %s %v %s\n", entry.Vin, entry.Metadata(), err.Error())
	} else {
		p.logger.Infof("nats_publish %s %v\n", entry.Vin, entry.Metadata())
	}
}
