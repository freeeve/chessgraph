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
  count: number;         // Total games
  wins: number;          // Wins for side to move
  draws: number;         // Draws
  losses: number;        // Losses for side to move
  win_pct: number;       // Win percentage (0-100)
  draw_pct: number;      // Draw percentage (0-100)
  cp?: number;           // Centipawn evaluation
  dtm?: number;          // Distance to mate (+ = win, - = loss)
  proven_depth?: number; // Depth at which eval was proven
  children?: TreeNode[]; // Child nodes (next moves)
  path?: PathNode[];     // Path from start (only on root when moves param used)
}

export interface StatsResponse {
  total_reads: number;
  total_writes: number;
  dirty_files: number;
  cached_blocks: number;
  read_only: boolean;
  total_positions: number;
  evaluated_positions: number;
  cp_positions: number;
  dtm_positions: number;
  dtz_positions: number;
  uncompressed_bytes: number;
  compressed_bytes: number;
  total_games: number;
  total_folders: number;
  total_blocks: number;
}

const API_BASE = '/v1';

export async function fetchTree(
  positionKey?: string,
  depth: number = 2,
  topMoves: number = 4,
  fetchMoves: number = 218
): Promise<TreeNode> {
  const key = positionKey || 'start';
  const response = await fetch(
    `${API_BASE}/tree/${encodeURIComponent(key)}?depth=${depth}&top=${topMoves}&fetch=${fetchMoves}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch tree: ${response.statusText}`);
  }
  return response.json();
}

// Fetch tree using UCI moves from starting position - returns path in single request
export async function fetchTreeWithMoves(
  uciMoves: string[],
  depth: number = 2,
  topMoves: number = 4,
  fetchMoves: number = 218
): Promise<TreeNode> {
  const movesParam = uciMoves.join(',');
  const response = await fetch(
    `${API_BASE}/tree/start?moves=${encodeURIComponent(movesParam)}&depth=${depth}&top=${topMoves}&fetch=${fetchMoves}`
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch tree: ${response.statusText}`);
  }
  return response.json();
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

