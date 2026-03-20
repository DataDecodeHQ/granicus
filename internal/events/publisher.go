package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

// DualWritePublisher writes events to both the local event store and Pub/Sub.
// Pub/Sub failures are logged but do not block the Firestore write.
type DualWritePublisher struct {
	store     *Store
	publisher *pubsub.Publisher
	client    *pubsub.Client
}

// NewDualWritePublisher creates a publisher that writes to local events and Pub/Sub.
// If Pub/Sub is unavailable, it falls back to local-only.
func NewDualWritePublisher(store *Store) *DualWritePublisher {
	p := &DualWritePublisher{store: store}

	project := os.Getenv("GRANICUS_PUBSUB_PROJECT")
	if project == "" {
		project = os.Getenv("GRANICUS_FIRESTORE_PROJECT")
	}
	if project == "" {
		return p
	}

	topicName := os.Getenv("GRANICUS_EVENTS_TOPIC")
	if topicName == "" {
		topicName = "granicus-events"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		slog.Warn("Pub/Sub client init failed, events will be local-only", "error", err)
		return p
	}

	publisher := client.Publisher(topicName)
	publisher.EnableMessageOrdering = true
	p.client = client
	p.publisher = publisher
	return p
}

// Emit writes an event to both local store and Pub/Sub.
func (p *DualWritePublisher) Emit(event Event) error {
	// Always write locally first
	if err := p.store.Emit(event); err != nil {
		return err
	}

	// Publish to Pub/Sub (non-blocking on failure)
	if p.publisher != nil {
		go p.publishAsync(event)
	}

	return nil
}

func (p *DualWritePublisher) publishAsync(event Event) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data, err := json.Marshal(event)
	if err != nil {
		slog.Warn("Pub/Sub event marshal failed", "error", err)
		return
	}

	orderingKey := event.Pipeline + ":" + event.RunID
	result := p.publisher.Publish(ctx, &pubsub.Message{
		Data:        data,
		OrderingKey: orderingKey,
		Attributes: map[string]string{
			"pipeline":   event.Pipeline,
			"run_id":     event.RunID,
			"event_type": event.EventType,
			"asset":      event.Asset,
		},
	})

	if _, err := result.Get(ctx); err != nil {
		slog.Warn("Pub/Sub publish failed", "event_type", event.EventType, "error", err)
	}
}

// Close releases Pub/Sub resources.
func (p *DualWritePublisher) Close() {
	if p.publisher != nil {
		p.publisher.Stop()
	}
	if p.client != nil {
		p.client.Close()
	}
}
