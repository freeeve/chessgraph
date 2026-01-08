// API types matching the Go backend

// PathNode represents a position in the path from start to current position
export interface PathNode {
  position: string;   // Base64 position key
  fen: string;        // FEN string
  uci: string;        // UCI move that led here
  san: string;        // SAN move that led here
  eco?: string;       // ECO opening code
  opening?: string;   // Opening name
  cp?: number;        // Centipawn evaluation
  dtm?: number;       // Distance to mate
  has_eval: boolean;  // Whether position has been evaluated
}

// TreeNode represents a position and its children in the game tree
export interface TreeNode {
  position: string;      // Base64 position key
  fen: string;           // FEN string
  uci?: string;          // UCI move that led here
  san?: string;          // SAN move that led here
  eco?: string;          // ECO opening code
  opening?: string;      // Opening name
  count: number;         // Total games
  wins: number;          // Wins for side to move
  draws: number;         // Draws
  losses: number;        // Losses for side to move
  win_pct: number;       // Win percentage (0-100)
  draw_pct: number;      // Draw percentage (0-100)
  cp?: number;           // Centipawn evaluation
  dtm?: number;          // Distance to mate (+ = win, - = loss)
  proven_depth?: number; // Depth at which eval was proven
  has_eval?: boolean;    // Whether position has been evaluated
  children?: TreeNode[]; // Child nodes (next moves) - deprecated, use top_moves
  path?: PathNode[];     // Path from start (only on root when moves param used)
}

// MoveStub is a skeleton move without stats (for progressive loading)
export interface MoveStub {
  position: string; // Base64 position key
  uci: string;      // UCI notation
  san: string;      // SAN notation
}

// TreeResponse is the new tree endpoint response with progressive loading
export interface TreeResponse {
  root: TreeNode;         // Current position with stats
  top_moves: TreeNode[];  // Top N moves with full stats
  other_moves?: MoveStub[]; // Remaining moves (skeleton only)
  path?: PathNode[];      // Path from start position
}

// PositionStatsResponse is a lightweight response with just position stats
export interface PositionStatsResponse {
  position: string;
  fen: string;           // FEN string (for hover preview)
  count: number;
  wins: number;
  draws: number;
  losses: number;
  win_pct: number;
  draw_pct: number;
  cp?: number;
  dtm?: number;
  proven_depth?: number;
  has_eval: boolean;
}

export interface StatsResponse {
  total_reads: number;
  total_writes: number;
  read_only: boolean;
  total_positions: number;
  evaluated_positions: number;
  cp_positions: number;
  dtm_positions: number;
  dtz_positions: number;
  total_games: number;

  // Memtable stats
  memtable_positions: number;
  memtable_bytes: number;
  memtable_bytes_per_pos: number;

  // L0 stats
  l0_files: number;
  l0_positions: number;
  l0_uncompressed_bytes: number;
  l0_compressed_bytes: number;
  l0_cached_files: number;
  l0_bytes_per_pos: number;
  l0_compress_ratio: number;

  // L1 stats
  l1_files: number;
  l1_positions: number;
  l1_uncompressed_bytes: number;
  l1_compressed_bytes: number;
  l1_cached_files: number;
  l1_bits_per_pos: number;
  l1_bytes_per_pos: number;
  l1_compress_ratio: number;

  // L2 stats
  l2_files: number;
  l2_positions: number;
  l2_uncompressed_bytes: number;
  l2_compressed_bytes: number;
  l2_cached_files: number;
  l2_bits_per_pos: number;
  l2_bytes_per_pos: number;
  l2_compress_ratio: number;

  // Legacy
  uncompressed_bytes: number;
  compressed_bytes: number;
  total_blocks: number;
  cached_blocks: number;
}

const API_BASE = '/v1';

// Simple LRU cache for tree responses
class TreeCache {
  private cache = new Map<string, TreeResponse>();
  private order: string[] = [];
  private maxSize: number;

  constructor(maxSize: number = 1000) {
    this.maxSize = maxSize;
  }

  get(key: string): TreeResponse | undefined {
    const value = this.cache.get(key);
    if (value) {
      // Move to end (most recently used)
      this.order = this.order.filter(k => k !== key);
      this.order.push(key);
    }
    return value;
  }

  set(key: string, value: TreeResponse): void {
    if (this.cache.has(key)) {
      this.cache.set(key, value);
      this.order = this.order.filter(k => k !== key);
      this.order.push(key);
      return;
    }

    // Evict oldest if at capacity
    while (this.cache.size >= this.maxSize && this.order.length > 0) {
      const oldest = this.order.shift()!;
      this.cache.delete(oldest);
    }

    this.cache.set(key, value);
    this.order.push(key);
  }

  clear(): void {
    this.cache.clear();
    this.order = [];
  }

  size(): number {
    return this.cache.size;
  }
}

// Global cache for tree responses (survives across navigations)
const treeCache = new TreeCache(1000);

// Fetch tree with progressive loading support (cached)
export async function fetchTree(
  positionKey?: string,
  topMoves: number = 3
): Promise<TreeResponse> {
  const key = positionKey || 'start';
  const cacheKey = `${key}:${topMoves}`;

  // Check cache first
  const cached = treeCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const response = await fetch(
    `${API_BASE}/tree/${encodeURIComponent(key)}?top=${topMoves}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch tree: ${response.statusText}`);
  }
  const data = await response.json();

  // Cache the response
  treeCache.set(cacheKey, data);

  return data;
}

// Fetch tree using SAN moves from starting position - returns path in single request
export async function fetchTreeWithMoves(
  sanMoves: string[],
  topMoves: number = 3
): Promise<TreeResponse> {
  const movesParam = sanMoves.join(',');
  const response = await fetch(
    `${API_BASE}/tree/start?moves=${encodeURIComponent(movesParam)}&top=${topMoves}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch tree: ${response.statusText}`);
  }
  return response.json();
}

// Cache for position stats (separate from tree cache)
const statsCache = new Map<string, PositionStatsResponse>();
const STATS_CACHE_MAX = 5000;

// Fetch stats for a single position (fast, for async loading) - cached
export async function fetchPositionStats(positionKey: string): Promise<PositionStatsResponse> {
  // Check cache first
  const cached = statsCache.get(positionKey);
  if (cached) {
    return cached;
  }

  const response = await fetch(
    `${API_BASE}/position-stats/${encodeURIComponent(positionKey)}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch position stats: ${response.statusText}`);
  }
  const data = await response.json();

  // Cache the response (simple eviction: clear half when full)
  if (statsCache.size >= STATS_CACHE_MAX) {
    const keys = Array.from(statsCache.keys());
    for (let i = 0; i < keys.length / 2; i++) {
      statsCache.delete(keys[i]);
    }
  }
  statsCache.set(positionKey, data);

  return data;
}

// Batch fetch position stats with concurrency limit
export async function fetchPositionStatsBatch(
  positionKeys: string[],
  concurrencyLimit: number = 10,
  onProgress?: (position: string, stats: PositionStatsResponse) => void
): Promise<Map<string, PositionStatsResponse>> {
  const results = new Map<string, PositionStatsResponse>();
  const queue = [...positionKeys];
  let inFlight = 0;

  return new Promise((resolve) => {
    const processNext = () => {
      while (inFlight < concurrencyLimit && queue.length > 0) {
        const key = queue.shift()!;
        inFlight++;

        fetchPositionStats(key)
          .then((stats) => {
            results.set(key, stats);
            if (onProgress) {
              onProgress(key, stats);
            }
          })
          .catch((err) => {
            console.error(`Failed to fetch stats for ${key}:`, err);
          })
          .finally(() => {
            inFlight--;
            if (queue.length > 0) {
              processNext();
            } else if (inFlight === 0) {
              resolve(results);
            }
          });
      }
    };

    if (queue.length === 0) {
      resolve(results);
    } else {
      processNext();
    }
  });
}

export async function fetchStats(): Promise<StatsResponse> {
  const response = await fetch(`${API_BASE}/stats`);
  if (!response.ok) {
    throw new Error(`Failed to fetch stats: ${response.statusText}`);
  }
  return response.json();
}

export interface FenLookupResponse {
  position: string;
  fen: string;
}

export async function fetchPositionByFen(fen: string): Promise<FenLookupResponse> {
  const response = await fetch(`${API_BASE}/fen?fen=${encodeURIComponent(fen)}`);
  if (!response.ok) {
    throw new Error(`Failed to lookup FEN: ${response.statusText}`);
  }
  return response.json();
}

export interface DeepEvalResponse {
  root_position: string;
  queued: number;
  depth: number;
}

export async function fetchDeepEval(positionKey: string): Promise<DeepEvalResponse> {
  const response = await fetch(`${API_BASE}/deep-eval/${encodeURIComponent(positionKey)}`);
  if (!response.ok) {
    throw new Error(`Failed to queue deep eval: ${response.statusText}`);
  }
  return response.json();
}

