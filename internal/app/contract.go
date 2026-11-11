package app

import (
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/contract"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
)

// ContractDeliveryTimeout bounds how long a contract event's send may take.
const ContractDeliveryTimeout = 30 * time.Second

// ContractPublisher publishes on the unified contract's topics through the
// cluster fbs uses (kafka.*), with contract.topic_prefix in place of fbs's
// own topic prefix: content.* are the platform's topics, not fbs's.
func ContractPublisher(cfg *config.Config, db *pgxpool.Pool, version string) (*contract.Publisher, error) {
	kc, err := KafkaClient(cfg, "-contract")
	if err != nil {
		return nil, err
	}
	kc.TopicPrefix = cfg.Contract.TopicPrefix
	// A send that can't be delivered fails within the timeout and is counted
	// (the backfill sends it later): the persister never waits on it for long.
	p, err := kafka.NewProducer(kc, kgo.RecordDeliveryTimeout(ContractDeliveryTimeout))
	if err != nil {
		return nil, err
	}
	return &contract.Publisher{DB: db, Producer: p, Production: cfg.Service.Env == "prod",
		Meta: contract.Meta{Collector: "fbs", CollectorVersion: version,
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT}}, nil
}
