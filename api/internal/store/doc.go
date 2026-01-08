// Package store2 provides a high-performance, compressed position store
// for chess analysis data using an LSM-tree-like architecture with L0/L1/L2 levels.
//
// Storage Layers:
//   - L0: Hot data in memtables, flushed to sorted segment files
//   - L1: Compacted sorted segments (~128-256MB each)
//   - L2: Large compressed blocks with separate index file (~1-2GB data files)
//
// File Formats:
//   - V13 (.psv3): Sorted segments with prefix-compressed keys and constant-byte elimination
//   - L2 blocks: Compressed blocks with separate index for block-level access
//
// Key Features:
//   - Efficient prefix compression for sorted key ranges
//   - Byte-striped layout for better compression ratios
//   - K-way merge for compaction with streaming iterators
//   - Continuous L1→L2 compaction (not just at end of ingestion)
//   - Memory-efficient: configurable limits on files per compaction run
package store
