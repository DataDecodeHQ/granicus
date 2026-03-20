package state

import (
	"context"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// DeleteDocs deletes all documents from the given iterator using BulkWriter.
// Returns the total number of documents deleted.
func DeleteDocs(ctx context.Context, client *firestore.Client, iter *firestore.DocumentIterator) (int, error) {
	bw := client.BulkWriter(ctx)
	count := 0
	var firstErr error

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			iter.Stop()
			bw.End()
			return count, fmt.Errorf("iterating documents: %w", err)
		}

		if _, err := bw.Delete(doc.Ref); err != nil {
			iter.Stop()
			bw.End()
			return count, fmt.Errorf("enqueuing delete: %w", err)
		}
		count++

		if firstErr != nil {
			iter.Stop()
			bw.End()
			return count, firstErr
		}
	}
	iter.Stop()

	bw.Flush()
	bw.End()

	return count, nil
}
