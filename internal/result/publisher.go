package result

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub/v2"
)

// Publisher sends ResultEnvelopes to a Pub/Sub topic.
// Used by Cloud Run Jobs to report results back to the engine.
type Publisher struct {
	client    *pubsub.Client
	publisher *pubsub.Publisher
}

// NewPublisher creates a result publisher. Project and topic are read from
// env vars if not provided (GRANICUS_PUBSUB_PROJECT, GRANICUS_RESULT_TOPIC).
func NewPublisher(ctx context.Context, project, topicName string) (*Publisher, error) {
	if project == "" {
		project = os.Getenv("GRANICUS_PUBSUB_PROJECT")
		if project == "" {
			project = os.Getenv("GRANICUS_FIRESTORE_PROJECT")
		}
	}
	if topicName == "" {
		topicName = os.Getenv("GRANICUS_RESULT_TOPIC")
		if topicName == "" {
			topicName = "granicus-results"
		}
	}

	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("creating Pub/Sub client: %w", err)
	}

	pub := client.Publisher(topicName)
	pub.EnableMessageOrdering = true

	return &Publisher{
		client:    client,
		publisher: pub,
	}, nil
}

// Publish sends a ResultEnvelope to the results topic.
// The ordering key is pipeline:run_id to ensure ordered delivery.
func (p *Publisher) Publish(ctx context.Context, envelope ResultEnvelope) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshaling result: %w", err)
	}

	orderingKey := envelope.Pipeline + ":" + envelope.RunID

	result := p.publisher.Publish(ctx, &pubsub.Message{
		Data:        data,
		OrderingKey: orderingKey,
		Attributes: map[string]string{
			"pipeline": envelope.Pipeline,
			"run_id":   envelope.RunID,
			"asset":    envelope.Asset,
			"status":   envelope.Status,
		},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("publishing result: %w", err)
	}

	return nil
}

// Close releases resources held by the publisher.
func (p *Publisher) Close() error {
	p.publisher.Stop()
	return p.client.Close()
}
