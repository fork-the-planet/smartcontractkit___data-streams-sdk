package streams

import "sync"

const seenBufferSize = 32

type Verdict int

const (
	Accept Verdict = iota
	Duplicate
	OutOfOrder
)

type feedState struct {
	watermark int64
	ring      [seenBufferSize]int64
	set       map[int64]struct{}
	cursor    int
	count     int
}

type FeedDeduplicator struct {
	mu    sync.Mutex
	feeds map[string]*feedState
}

func NewFeedDeduplicator() *FeedDeduplicator {
	return &FeedDeduplicator{feeds: make(map[string]*feedState)}
}

func (d *FeedDeduplicator) Check(feedID string, ts int64) Verdict {
	d.mu.Lock()
	defer d.mu.Unlock()

	fs := d.feeds[feedID]
	if fs == nil {
		fs = &feedState{set: make(map[int64]struct{}, seenBufferSize)}
		d.feeds[feedID] = fs
	}

	if _, dup := fs.set[ts]; dup {
		return Duplicate
	}

	if fs.count == seenBufferSize {
		evict := fs.ring[fs.cursor]
		delete(fs.set, evict)
	} else {
		fs.count++
	}
	fs.ring[fs.cursor] = ts
	fs.set[ts] = struct{}{}
	fs.cursor = (fs.cursor + 1) % seenBufferSize

	isOutOfOrder := fs.watermark > 0 && ts < fs.watermark
	if isOutOfOrder {
		return OutOfOrder
	}
	
	if ts > fs.watermark {
		fs.watermark = ts
	}

	return Accept
}
