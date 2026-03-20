package state

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// DeleteDocs deletes all documents from the given iterator in batches of 500.
// Returns the total number of documents deleted.
func DeleteDocs(ctx context.Context, client *firestore.Client, iter *firestore.DocumentIterator) (int, error) {
	batch := client.Batch()
	count := 0
	totalDeleted := 0

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			return totalDeleted, fmt.Errorf("iterating documents: %w", err)
		}

		batch.Delete(doc.Ref)
		count++

		if count >= 500 {
			if _, err := batch.Commit(ctx); err != nil {
				iter.Stop()
				return totalDeleted, fmt.Errorf("batch delete: %w", err)
			}
			totalDeleted += count
			batch = client.Batch()
			count = 0
		}
	}
	iter.Stop()

	// Commit remaining documents
	if count > 0 {
		if _, err := batch.Commit(ctx); err != nil {
			return totalDeleted, fmt.Errorf("batch delete: %w", err)
		}
		totalDeleted += count
	}

	return totalDeleted, nil
}
