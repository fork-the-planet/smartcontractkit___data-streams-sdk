package streams

import "testing"

func TestFeedDeduplicator_Accept(t *testing.T) {
	d := NewFeedDeduplicator()
	if v := d.Check("feed-1", 100); v != Accept {
		t.Fatalf("expected Accept, got %d", v)
	}
}

func TestFeedDeduplicator_Duplicate(t *testing.T) {
	d := NewFeedDeduplicator()
	d.Check("feed-1", 100)
	if v := d.Check("feed-1", 100); v != Duplicate {
		t.Fatalf("expected Duplicate, got %d", v)
	}
}

func TestFeedDeduplicator_OutOfOrder(t *testing.T) {
	d := NewFeedDeduplicator()
	d.Check("feed-1", 200)
	if v := d.Check("feed-1", 100); v != OutOfOrder {
		t.Fatalf("expected OutOfOrder, got %d", v)
	}
}

func TestFeedDeduplicator_OutOfOrderNotDuplicate(t *testing.T) {
	d := NewFeedDeduplicator()
	d.Check("feed-1", 200)
	v := d.Check("feed-1", 100)
	if v != OutOfOrder {
		t.Fatalf("expected OutOfOrder for first OOO delivery, got %d", v)
	}
	if v := d.Check("feed-1", 100); v != Duplicate {
		t.Fatalf("expected Duplicate for second OOO delivery, got %d", v)
	}
}

func TestFeedDeduplicator_FIFOEviction(t *testing.T) {
	d := NewFeedDeduplicator()
	for i := int64(1); i <= seenBufferSize; i++ {
		d.Check("feed-1", i)
	}
	d.Check("feed-1", 33)
	// ts=2 (second inserted) should still be in the buffer
	if v := d.Check("feed-1", 2); v != Duplicate {
		t.Fatalf("expected ts=2 still in buffer, got %d", v)
	}
	// ts=1 (first inserted) was evicted by ts=33
	if v := d.Check("feed-1", 1); v == Duplicate {
		t.Fatal("expected ts=1 to be evicted (FIFO), but got Duplicate")
	}
}

func TestFeedDeduplicator_FIFOEvictsOldestInsertedNotSmallest(t *testing.T) {
	d := NewFeedDeduplicator()
	// Insert out of order: 100, 1, 2, 3, ..., 31 (total 32 entries)
	d.Check("feed-1", 100)
	for i := int64(1); i <= seenBufferSize-1; i++ {
		d.Check("feed-1", i)
	}
	// Buffer is full. ts=100 was inserted first (oldest by insertion).
	// Adding ts=999 should evict ts=100, NOT ts=1 (the smallest value).
	d.Check("feed-1", 999)
	// ts=1 should still be present (smallest value, but NOT oldest inserted)
	if v := d.Check("feed-1", 1); v != Duplicate {
		t.Fatalf("expected ts=1 (smallest value, but not oldest inserted) to remain, got %d", v)
	}
	// ts=100 should have been evicted (oldest inserted)
	if v := d.Check("feed-1", 100); v == Duplicate {
		t.Fatal("expected ts=100 (oldest inserted) to be evicted, but got Duplicate")
	}
}

func TestFeedDeduplicator_IndependentFeeds(t *testing.T) {
	d := NewFeedDeduplicator()
	d.Check("feed-a", 100)
	d.Check("feed-b", 100)

	if v := d.Check("feed-a", 100); v != Duplicate {
		t.Fatalf("expected Duplicate for feed-a, got %d", v)
	}
	if v := d.Check("feed-b", 100); v != Duplicate {
		t.Fatalf("expected Duplicate for feed-b, got %d", v)
	}
	// Different feed, same ts is not a duplicate
	if v := d.Check("feed-c", 100); v != Accept {
		t.Fatalf("expected Accept for new feed-c, got %d", v)
	}
}

func TestFeedDeduplicator_WatermarkZeroNotOutOfOrder(t *testing.T) {
	d := NewFeedDeduplicator()
	// First report at ts=0 should be Accept, not OutOfOrder
	if v := d.Check("feed-1", 0); v != Accept {
		t.Fatalf("expected Accept for first report at ts=0, got %d", v)
	}
}

func TestFeedDeduplicator_HADuplicateAfterWatermarkAdvance(t *testing.T) {
	d := NewFeedDeduplicator()
	d.Check("feed-1", 100) // Accept
	d.Check("feed-1", 200) // Accept, watermark -> 200
	// HA duplicate of ts=100 arrives from second connection
	if v := d.Check("feed-1", 100); v != Duplicate {
		t.Fatalf("expected Duplicate for HA retransmit, got %d", v)
	}
}
