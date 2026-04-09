/**
 * Report deduplication using watermark timestamps
 */

export interface ReportMetadata {
  feedID: string;
  observationsTimestamp: number;
  validFromTimestamp: number;
  fullReport: string;
}

export interface DeduplicationResult {
  isAccepted: boolean;
  isDuplicate: boolean;
  isOutOfOrder: boolean;
  reason?: string;
}

export interface DeduplicationStats {
  accepted: number;
  deduplicated: number;
  outOfOrder: number;
  totalReceived: number;
  watermarkCount: number;
}

/**
 * Manages report deduplication using watermark timestamps
 */
export class ReportDeduplicator {
  private waterMark: Map<string, number> = new Map();
  private acceptedCount = 0;
  private deduplicatedCount = 0;
  private outOfOrderCount = 0;
  private cleanupInterval: NodeJS.Timeout | null = null;

  // Configuration
  private readonly maxWatermarkAge: number;
  private readonly cleanupIntervalMs: number;
  private readonly allowOutOfOrder: boolean;

  constructor(
    options: {
      maxWatermarkAge?: number; // How long to keep watermarks (default: 1 hour)
      cleanupIntervalMs?: number; // How often to clean old watermarks (default: 5 minutes)
      allowOutOfOrder?: boolean; // Allow out-of-order reports through (default: false)
    } = {}
  ) {
    this.maxWatermarkAge = options.maxWatermarkAge ?? 60 * 60 * 1000; // 1 hour
    this.cleanupIntervalMs = options.cleanupIntervalMs ?? 5 * 60 * 1000; // 5 minutes
    this.allowOutOfOrder = options.allowOutOfOrder ?? false;

    // Start periodic cleanup
    this.startCleanup();
  }

  /**
   * Process a report and determine if it should be accepted or deduplicated
   */
  processReport(report: ReportMetadata): DeduplicationResult {
    const feedId = report.feedID;
    const observationsTimestamp = report.observationsTimestamp;

    const currentWatermark = this.waterMark.get(feedId);

    if (currentWatermark !== undefined) {
      if (this.allowOutOfOrder) {
        if (observationsTimestamp === currentWatermark) {
          this.deduplicatedCount++;
          return {
            isAccepted: false,
            isDuplicate: true,
            isOutOfOrder: false,
            reason: `Duplicate timestamp ${observationsTimestamp} for feed ${feedId}`,
          };
        }
        if (observationsTimestamp < currentWatermark) {
          this.outOfOrderCount++;
          this.acceptedCount++;
          return { isAccepted: true, isDuplicate: false, isOutOfOrder: true };
        }
        this.waterMark.set(feedId, observationsTimestamp);
      } else {
        if (observationsTimestamp <= currentWatermark) {
          this.deduplicatedCount++;
          const isOOO = observationsTimestamp < currentWatermark;
          if (isOOO) {
            this.outOfOrderCount++;
          }
          return {
            isAccepted: false,
            isDuplicate: true,
            isOutOfOrder: isOOO,
            reason: `Report timestamp ${observationsTimestamp} <= watermark ${currentWatermark} for feed ${feedId}`,
          };
        }
        this.waterMark.set(feedId, observationsTimestamp);
      }
    } else {
      this.waterMark.set(feedId, observationsTimestamp);
    }

    this.acceptedCount++;
    return { isAccepted: true, isDuplicate: false, isOutOfOrder: false };
  }

  /**
   * Get current deduplication statistics
   */
  getStats(): DeduplicationStats {
    return {
      accepted: this.acceptedCount,
      deduplicated: this.deduplicatedCount,
      outOfOrder: this.outOfOrderCount,
      totalReceived: this.acceptedCount + this.deduplicatedCount,
      watermarkCount: this.waterMark.size,
    };
  }

  /**
   * Get watermark for a specific feed ID
   */
  getWatermark(feedId: string): number | undefined {
    return this.waterMark.get(feedId);
  }

  /**
   * Get all current watermarks (for debugging/monitoring)
   */
  getAllWatermarks(): Record<string, number> {
    const watermarks: Record<string, number> = {};
    for (const [feedId, timestamp] of this.waterMark) {
      watermarks[feedId] = timestamp;
    }
    return watermarks;
  }

  /**
   * Manually set watermark for a feed (useful for initialization)
   */
  setWatermark(feedId: string, timestamp: number): void {
    this.waterMark.set(feedId, timestamp);
  }

  /**
   * Clear watermark for a specific feed
   */
  clearWatermark(feedId: string): boolean {
    return this.waterMark.delete(feedId);
  }

  /**
   * Clear all watermarks
   */
  clearAllWatermarks(): void {
    this.waterMark.clear();
  }

  /**
   * Reset all counters and watermarks
   */
  reset(): void {
    this.acceptedCount = 0;
    this.deduplicatedCount = 0;
    this.outOfOrderCount = 0;
    this.waterMark.clear();
  }

  /**
   * Start periodic cleanup of old watermarks
   * This prevents memory leaks for feeds that are no longer active
   */
  private startCleanup(): void {
    this.cleanupInterval = setInterval(() => {
      this.cleanupOldWatermarks();
    }, this.cleanupIntervalMs);
  }

  /**
   * Clean up watermarks that are too old
   * This is a safety mechanism to prevent unbounded memory growth
   */
  private cleanupOldWatermarks(): void {
    const now = Date.now();
    const cutoffTime = now - this.maxWatermarkAge;

    // Convert cutoff time to seconds (like the timestamps in reports)
    const cutoffTimestamp = Math.floor(cutoffTime / 1000);

    let _removedCount = 0;
    for (const [feedId, timestamp] of this.waterMark) {
      if (timestamp < cutoffTimestamp) {
        this.waterMark.delete(feedId);
        _removedCount++;
      }
    }
  }

  /**
   * Stop the deduplicator and clean up resources
   */
  stop(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }

  /**
   * Get memory usage information
   */
  getMemoryInfo(): {
    watermarkCount: number;
    estimatedMemoryBytes: number;
  } {
    const watermarkCount = this.waterMark.size;

    // Rough estimation: each entry has a string key (~64 chars) + number value
    // String: ~64 bytes (feed ID) + Number: 8 bytes + Map overhead: ~32 bytes
    const estimatedMemoryBytes = watermarkCount * (64 + 8 + 32);

    return {
      watermarkCount,
      estimatedMemoryBytes,
    };
  }

  /**
   * Export watermarks for persistence/debugging
   */
  exportWatermarks(): Array<{ feedId: string; timestamp: number }> {
    return Array.from(this.waterMark.entries()).map(([feedId, timestamp]) => ({
      feedId,
      timestamp,
    }));
  }

  /**
   * Import watermarks from external source
   */
  importWatermarks(watermarks: Array<{ feedId: string; timestamp: number }>): void {
    for (const { feedId, timestamp } of watermarks) {
      this.waterMark.set(feedId, timestamp);
    }
  }
}
