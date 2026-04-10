import { ReportDeduplicator, ReportMetadata } from "../../../src/stream/deduplication";

describe("ReportDeduplicator", () => {
  let deduplicator: ReportDeduplicator;

  beforeEach(() => {
    deduplicator = new ReportDeduplicator();
  });

  afterEach(() => {
    deduplicator.stop();
  });

  describe("basic deduplication", () => {
    it("should allow first report for a feed", () => {
      const report: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report-data",
        validFromTimestamp: 900,
      };

      const result = deduplicator.processReport(report);
      expect(result.isAccepted).toBe(true);
      expect(result.isDuplicate).toBe(false);
    });

    it("should reject duplicate reports with same timestamp", () => {
      const report: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report-data",
        validFromTimestamp: 900,
      };

      const result1 = deduplicator.processReport(report);
      expect(result1.isAccepted).toBe(true);
      expect(result1.isDuplicate).toBe(false);

      const result2 = deduplicator.processReport(report);
      expect(result2.isAccepted).toBe(false);
      expect(result2.isDuplicate).toBe(true);
      expect(result2.isOutOfOrder).toBe(false);
      expect(result2.reason).toContain("already seen");
    });

    it("should reject reports with older timestamps", () => {
      const newerReport: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 2000,
        fullReport: "newer-report",
        validFromTimestamp: 1900,
      };

      const olderReport: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "older-report",
        validFromTimestamp: 900,
      };

      const result1 = deduplicator.processReport(newerReport);
      expect(result1.isAccepted).toBe(true);

      const result2 = deduplicator.processReport(olderReport);
      expect(result2.isAccepted).toBe(false);
      expect(result2.isOutOfOrder).toBe(true);
    });

    it("should accept reports with newer timestamps", () => {
      const olderReport: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "older-report",
        validFromTimestamp: 900,
      };

      const newerReport: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 2000,
        fullReport: "newer-report",
        validFromTimestamp: 1900,
      };

      const result1 = deduplicator.processReport(olderReport);
      expect(result1.isAccepted).toBe(true);

      const result2 = deduplicator.processReport(newerReport);
      expect(result2.isAccepted).toBe(true);
      expect(result2.isDuplicate).toBe(false);
    });
  });

  describe("out-of-order with allowOutOfOrder", () => {
    it("should accept out-of-order reports when allowOutOfOrder is true", () => {
      const dedup = new ReportDeduplicator({ allowOutOfOrder: true });

      dedup.processReport({
        feedID: "0x123",
        observationsTimestamp: 2000,
        fullReport: "newer",
        validFromTimestamp: 1900,
      });

      const result = dedup.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "older",
        validFromTimestamp: 900,
      });

      expect(result.isAccepted).toBe(true);
      expect(result.isOutOfOrder).toBe(true);
      expect(result.isDuplicate).toBe(false);
      dedup.stop();
    });

    it("should distinguish out-of-order from duplicate", () => {
      const dedup = new ReportDeduplicator({ allowOutOfOrder: true });

      dedup.processReport({
        feedID: "0x123",
        observationsTimestamp: 200,
        fullReport: "r",
        validFromTimestamp: 100,
      });

      const ooo = dedup.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 50,
      });
      expect(ooo.isAccepted).toBe(true);
      expect(ooo.isOutOfOrder).toBe(true);

      const dup = dedup.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 50,
      });
      expect(dup.isAccepted).toBe(false);
      expect(dup.isDuplicate).toBe(true);
      expect(dup.isOutOfOrder).toBe(false);
      dedup.stop();
    });
  });

  describe("multi-feed handling", () => {
    it("should handle multiple feeds independently", () => {
      const report1: ReportMetadata = {
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report1",
        validFromTimestamp: 900,
      };

      const report2: ReportMetadata = {
        feedID: "0x456",
        observationsTimestamp: 1000,
        fullReport: "report2",
        validFromTimestamp: 900,
      };

      const result1 = deduplicator.processReport(report1);
      expect(result1.isAccepted).toBe(true);

      const result2 = deduplicator.processReport(report2);
      expect(result2.isAccepted).toBe(true);

      const result3 = deduplicator.processReport(report1);
      expect(result3.isAccepted).toBe(false);

      const result4 = deduplicator.processReport(report2);
      expect(result4.isAccepted).toBe(false);
    });

    it("should track watermarks per feed independently", () => {
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report1",
        validFromTimestamp: 900,
      });

      deduplicator.processReport({
        feedID: "0x456",
        observationsTimestamp: 2000,
        fullReport: "report2",
        validFromTimestamp: 1900,
      });

      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1500,
        fullReport: "report3",
        validFromTimestamp: 1400,
      });

      expect(deduplicator.getWatermark("0x123")).toBe(1500);
      expect(deduplicator.getWatermark("0x456")).toBe(2000);
    });
  });

  describe("watermark management", () => {
    it("should return undefined for unknown feeds", () => {
      expect(deduplicator.getWatermark("unknown-feed")).toBeUndefined();
    });

    it("should update watermarks correctly", () => {
      expect(deduplicator.getWatermark("0x123")).toBeUndefined();

      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1500,
        fullReport: "report",
        validFromTimestamp: 1400,
      });

      expect(deduplicator.getWatermark("0x123")).toBe(1500);
    });

    it("should not update watermark for out-of-order reports", () => {
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 2000,
        fullReport: "report1",
        validFromTimestamp: 1900,
      });
      expect(deduplicator.getWatermark("0x123")).toBe(2000);

      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report2",
        validFromTimestamp: 900,
      });
      expect(deduplicator.getWatermark("0x123")).toBe(2000);
    });
  });

  describe("FIFO eviction", () => {
    it("should evict oldest-inserted entry when buffer is full", () => {
      for (let i = 1; i <= 32; i++) {
        deduplicator.processReport({
          feedID: "0x123",
          observationsTimestamp: i,
          fullReport: "r",
          validFromTimestamp: i - 1,
        });
      }

      // ts=2 (second inserted) should still be in the buffer
      const stillPresent = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 2,
        fullReport: "r",
        validFromTimestamp: 1,
      });
      expect(stillPresent.isDuplicate).toBe(true);

      // Adding ts=33 evicts ts=1 (oldest inserted)
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 33,
        fullReport: "r",
        validFromTimestamp: 32,
      });

      // ts=1 was evicted, so it's no longer a duplicate
      // (calling processReport re-inserts it, which evicts ts=3 as next oldest)
      const evicted = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1,
        fullReport: "r",
        validFromTimestamp: 0,
      });
      expect(evicted.isDuplicate).toBe(false);
    });

    it("should evict by insertion order, not by smallest value", () => {
      // Insert 100 first, then 1..31 (total 32 entries)
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 99,
      });
      for (let i = 1; i <= 31; i++) {
        deduplicator.processReport({
          feedID: "0x123",
          observationsTimestamp: i,
          fullReport: "r",
          validFromTimestamp: i - 1,
        });
      }

      // Add 999 -> should evict 100 (oldest inserted), NOT 1 (smallest value)
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 999,
        fullReport: "r",
        validFromTimestamp: 998,
      });

      // ts=1 should still be present
      const smallestStillPresent = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1,
        fullReport: "r",
        validFromTimestamp: 0,
      });
      expect(smallestStillPresent.isDuplicate).toBe(true);

      // ts=100 should have been evicted
      const oldestEvicted = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 99,
      });
      expect(oldestEvicted.isDuplicate).toBe(false);
    });
  });

  describe("HA duplicate detection", () => {
    it("should detect HA duplicate after watermark advance", () => {
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 99,
      });

      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 200,
        fullReport: "r",
        validFromTimestamp: 199,
      });

      // HA duplicate of ts=100 from second connection
      const result = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 100,
        fullReport: "r",
        validFromTimestamp: 99,
      });
      expect(result.isDuplicate).toBe(true);
      expect(result.isOutOfOrder).toBe(false);
      expect(result.isAccepted).toBe(false);
    });
  });

  describe("statistics tracking", () => {
    it("should track statistics correctly", () => {
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report1",
        validFromTimestamp: 900,
      });
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report2",
        validFromTimestamp: 900,
      });
      deduplicator.processReport({
        feedID: "0x456",
        observationsTimestamp: 2000,
        fullReport: "report3",
        validFromTimestamp: 1900,
      });

      const stats = deduplicator.getStats();
      expect(stats.accepted).toBe(2);
      expect(stats.deduplicated).toBe(1);
      expect(stats.totalReceived).toBe(3);
      expect(stats.watermarkCount).toBe(2);
    });

    it("should reset statistics", () => {
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report",
        validFromTimestamp: 900,
      });
      deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 1000,
        fullReport: "report",
        validFromTimestamp: 900,
      });

      let stats = deduplicator.getStats();
      expect(stats.accepted).toBe(1);
      expect(stats.deduplicated).toBe(1);

      deduplicator.reset();

      stats = deduplicator.getStats();
      expect(stats.accepted).toBe(0);
      expect(stats.deduplicated).toBe(0);
      expect(stats.totalReceived).toBe(0);
      expect(stats.watermarkCount).toBe(0);
    });
  });

  describe("memory management", () => {
    it("should handle large numbers of feeds efficiently", () => {
      const feedCount = 1000;
      const feeds: string[] = [];

      for (let i = 0; i < feedCount; i++) {
        feeds.push(`0x${i.toString(16).padStart(64, "0")}`);
      }

      feeds.forEach((feedID, index) => {
        const result = deduplicator.processReport({
          feedID,
          observationsTimestamp: index + 1000,
          fullReport: `report-${index}`,
          validFromTimestamp: index + 900,
        });
        expect(result.isAccepted).toBe(true);
      });

      feeds.forEach((feedID, index) => {
        expect(deduplicator.getWatermark(feedID)).toBe(index + 1000);
      });

      const stats = deduplicator.getStats();
      expect(stats.watermarkCount).toBe(feedCount);
    });
  });

  describe("edge cases", () => {
    it("should handle zero timestamp", () => {
      const result = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 0,
        fullReport: "report",
        validFromTimestamp: 0,
      });
      expect(result.isAccepted).toBe(true);
      expect(deduplicator.getWatermark("0x123")).toBe(0);

      const result2 = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: 0,
        fullReport: "report",
        validFromTimestamp: 0,
      });
      expect(result2.isAccepted).toBe(false);
    });

    it("should handle very large timestamps", () => {
      const largeTimestamp = Number.MAX_SAFE_INTEGER;
      const result = deduplicator.processReport({
        feedID: "0x123",
        observationsTimestamp: largeTimestamp,
        fullReport: "report",
        validFromTimestamp: largeTimestamp - 1,
      });
      expect(result.isAccepted).toBe(true);
      expect(deduplicator.getWatermark("0x123")).toBe(largeTimestamp);
    });

    it("should handle empty feed ID", () => {
      const result = deduplicator.processReport({
        feedID: "",
        observationsTimestamp: 1000,
        fullReport: "report",
        validFromTimestamp: 900,
      });
      expect(result.isAccepted).toBe(true);
      expect(deduplicator.getWatermark("")).toBe(1000);
    });

    it("should handle special characters in feed ID", () => {
      const specialFeedId = "0x!@#$%^&*()_+-=[]{}|;:,.<>?";
      const result = deduplicator.processReport({
        feedID: specialFeedId,
        observationsTimestamp: 1000,
        fullReport: "report",
        validFromTimestamp: 900,
      });
      expect(result.isAccepted).toBe(true);
      expect(deduplicator.getWatermark(specialFeedId)).toBe(1000);
    });
  });

  describe("cleanup functionality", () => {
    it("should initialize with cleanup enabled", () => {
      const dedup = new ReportDeduplicator({
        maxWatermarkAge: 1000,
        cleanupIntervalMs: 500,
      });

      expect(dedup).toBeDefined();
      dedup.stop();
    });

    it("should stop cleanup properly", () => {
      const dedup = new ReportDeduplicator();
      dedup.stop();

      // Should not throw when stopped multiple times
      dedup.stop();
    });
  });
});
