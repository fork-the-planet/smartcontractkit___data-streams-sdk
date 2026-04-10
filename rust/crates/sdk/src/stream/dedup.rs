use std::collections::{HashMap, HashSet};

const SEEN_BUFFER_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Accept,
    Duplicate,
    OutOfOrder,
}

struct FeedState {
    watermark: u64,
    ring: [u64; SEEN_BUFFER_SIZE],
    set: HashSet<u64>,
    cursor: usize,
    count: usize,
}

impl FeedState {
    fn new() -> Self {
        Self {
            watermark: 0,
            ring: [0; SEEN_BUFFER_SIZE],
            set: HashSet::with_capacity(SEEN_BUFFER_SIZE),
            cursor: 0,
            count: 0,
        }
    }
}

pub(crate) struct FeedDeduplicator {
    feeds: HashMap<String, FeedState>,
}

impl FeedDeduplicator {
    pub fn new() -> Self {
        Self {
            feeds: HashMap::new(),
        }
    }

    pub fn check(&mut self, feed_id: &str, ts: u64) -> Verdict {
        let fs = self
            .feeds
            .entry(feed_id.to_owned())
            .or_insert_with(FeedState::new);

        if fs.set.contains(&ts) {
            return Verdict::Duplicate;
        }

        if fs.count == SEEN_BUFFER_SIZE {
            let evict = fs.ring[fs.cursor];
            fs.set.remove(&evict);
        } else {
            fs.count += 1;
        }
        fs.ring[fs.cursor] = ts;
        fs.set.insert(ts);
        fs.cursor = (fs.cursor + 1) % SEEN_BUFFER_SIZE;

        let is_out_of_order = fs.watermark > 0 && ts < fs.watermark;
        if ts > fs.watermark {
            fs.watermark = ts;
        }

        if is_out_of_order {
            Verdict::OutOfOrder
        } else {
            Verdict::Accept
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept() {
        let mut d = FeedDeduplicator::new();
        assert_eq!(d.check("feed-1", 100), Verdict::Accept);
    }

    #[test]
    fn duplicate() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-1", 100);
        assert_eq!(d.check("feed-1", 100), Verdict::Duplicate);
    }

    #[test]
    fn out_of_order() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-1", 200);
        assert_eq!(d.check("feed-1", 100), Verdict::OutOfOrder);
    }

    #[test]
    fn out_of_order_then_duplicate() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-1", 200);
        assert_eq!(d.check("feed-1", 100), Verdict::OutOfOrder);
        assert_eq!(d.check("feed-1", 100), Verdict::Duplicate);
    }

    #[test]
    fn fifo_eviction() {
        let mut d = FeedDeduplicator::new();
        for i in 1..=SEEN_BUFFER_SIZE as u64 {
            d.check("feed-1", i);
        }
        d.check("feed-1", 33);
        // ts=2 still present
        assert_eq!(d.check("feed-1", 2), Verdict::Duplicate);
        // ts=1 was evicted (FIFO oldest inserted)
        assert_ne!(d.check("feed-1", 1), Verdict::Duplicate);
    }

    #[test]
    fn fifo_evicts_oldest_inserted_not_smallest_value() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-1", 100);
        for i in 1..SEEN_BUFFER_SIZE as u64 {
            d.check("feed-1", i);
        }
        d.check("feed-1", 999);
        // ts=1 (smallest value) should still be present
        assert_eq!(d.check("feed-1", 1), Verdict::Duplicate);
        // ts=100 (oldest inserted) should be evicted
        assert_ne!(d.check("feed-1", 100), Verdict::Duplicate);
    }

    #[test]
    fn independent_feeds() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-a", 100);
        d.check("feed-b", 100);
        assert_eq!(d.check("feed-a", 100), Verdict::Duplicate);
        assert_eq!(d.check("feed-b", 100), Verdict::Duplicate);
        assert_eq!(d.check("feed-c", 100), Verdict::Accept);
    }

    #[test]
    fn watermark_zero_not_out_of_order() {
        let mut d = FeedDeduplicator::new();
        assert_eq!(d.check("feed-1", 0), Verdict::Accept);
    }

    #[test]
    fn ha_duplicate_after_watermark_advance() {
        let mut d = FeedDeduplicator::new();
        d.check("feed-1", 100);
        d.check("feed-1", 200);
        assert_eq!(d.check("feed-1", 100), Verdict::Duplicate);
    }
}
