import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Chessboard } from 'react-chessboard';
import { fetchTree, TreeNode, StatsResponse, fetchPositionByFen } from './api';

// Custom hook to get previous value
function usePrevious<T>(value: T): T | undefined {
  const ref = useRef<T>();
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
}

interface ChessGraphProps {
  topMoves?: number;      // How many moves to show at once
  fetchMoves?: number;    // How many moves to fetch from API (for pagination)
  depth?: number;
  stats?: StatsResponse | null;
  onTopMovesChange?: (value: number) => void;
  onDepthChange?: (value: number) => void;
}

type BoardOrientation = 'auto' | 'white' | 'black';

// Convert UCI move to highlight squares
function getHighlightSquares(uci?: string): { [square: string]: React.CSSProperties } {
  if (!uci || uci.length < 4) return {};
  const from = uci.slice(0, 2);
  const to = uci.slice(2, 4);
  const highlightStyle = { backgroundColor: 'rgba(255, 255, 0, 0.4)' };
  return {
    [from]: highlightStyle,
    [to]: highlightStyle,
  };
}

// Determine board orientation based on mode and FEN
function getBoardOrientation(mode: BoardOrientation, fen: string): 'white' | 'black' {
  if (mode === 'auto') {
    return getSideToMove(fen);
  }
  return mode;
}

interface NavigationState {
  current: TreeNode;
  parents: TreeNode[];
  lastMoveUci?: string;  // UCI move that led to current position (for highlighting)
}

interface UrlState {
  position?: string;
  path: string[];  // Parent position keys
  topMoves?: number;
  depth?: number;
}

// Get side to move from FEN string
function getSideToMove(fen: string): 'white' | 'black' {
  const parts = fen.split(' ');
  return parts[1] === 'b' ? 'black' : 'white';
}

// Format large numbers with K/M/B suffixes
function formatNumber(n: number | undefined): string {
  if (n === undefined || n === null) return '0';
  if (n >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1) + 'B';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toString();
}

// Format byte sizes with appropriate units
function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || bytes === null) return '0 B';
  if (bytes >= 1_000_000_000) return (bytes / 1_000_000_000).toFixed(1) + ' GB';
  if (bytes >= 1_000_000) return (bytes / 1_000_000).toFixed(1) + ' MB';
  if (bytes >= 1_000) return (bytes / 1_000).toFixed(1) + ' KB';
  return bytes + ' B';
}

// Parse URL state from hash and query params
function getStateFromUrl(): UrlState {
  const hash = window.location.hash.slice(1); // Remove '#'
  const params = new URLSearchParams(window.location.search);

  const pathParam = params.get('path');
  const path = pathParam ? pathParam.split(',').filter(Boolean) : [];

  const topMovesParam = params.get('moves');
  const depthParam = params.get('depth');

  return {
    position: hash || undefined,
    path,
    topMoves: topMovesParam ? parseInt(topMovesParam, 10) : undefined,
    depth: depthParam ? parseInt(depthParam, 10) : undefined,
  };
}

// Update URL with current state
function updateUrl(position?: string, path: string[] = [], topMoves?: number, depth?: number) {
  const params = new URLSearchParams();

  if (path.length > 0) {
    params.set('path', path.join(','));
  }
  if (topMoves !== undefined) {
    params.set('moves', topMoves.toString());
  }
  if (depth !== undefined) {
    params.set('depth', depth.toString());
  }

  const queryString = params.toString();
  const newHash = position ? `#${position}` : '';
  const newUrl = queryString
    ? `${window.location.pathname}?${queryString}${newHash}`
    : `${window.location.pathname}${newHash}`;

  if (window.location.href !== new URL(newUrl, window.location.origin).href) {
    window.history.pushState(null, '', newUrl);
  }
}

export default function ChessGraph({
  topMoves: propTopMoves = 4,
  fetchMoves = 218,  // Fetch all possible moves from a position
  depth: propDepth = 2,
  stats,
  onTopMovesChange,
  onDepthChange
}: ChessGraphProps) {
  // Read initial settings from URL (only once on mount) to override props if URL has values
  const [initialUrlState] = useState(() => getStateFromUrl());

  // Use props for the values - they're controlled by the parent component
  // Only use URL state on initial mount if present
  const [hasAppliedUrlState, setHasAppliedUrlState] = useState(false);

  // On first render, if URL has values, notify parent to update
  useEffect(() => {
    if (!hasAppliedUrlState) {
      if (initialUrlState.topMoves !== undefined && initialUrlState.topMoves !== propTopMoves) {
        onTopMovesChange?.(initialUrlState.topMoves);
      }
      if (initialUrlState.depth !== undefined && initialUrlState.depth !== propDepth) {
        onDepthChange?.(initialUrlState.depth);
      }
      setHasAppliedUrlState(true);
    }
  }, [hasAppliedUrlState, initialUrlState, propTopMoves, propDepth, onTopMovesChange, onDepthChange]);

  // Use the props directly - parent controls these
  const topMoves = propTopMoves;
  const depth = propDepth;

  const [navState, setNavState] = useState<NavigationState | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [boardOrientation, setBoardOrientation] = useState<BoardOrientation>('auto');
  const [hoveredMove, setHoveredMove] = useState<TreeNode | null>(null);
  const [showFenDialog, setShowFenDialog] = useState(false);
  const [fenInput, setFenInput] = useState('');
  const [fenError, setFenError] = useState<string | null>(null);

  // Fetch at least as many as we display
  const loadTree = useCallback(async (positionKey?: string, parents: TreeNode[] = [], updateHistory = true, lastMoveUci?: string, currentSan?: string) => {
    setLoading(true);
    setError(null);

    try {
      const tree = await fetchTree(positionKey, depth, topMoves, fetchMoves);
      // Apply san if provided (needed when loading from URL)
      if (currentSan) {
        tree.san = currentSan;
      }
      // Apply uci if provided
      if (lastMoveUci) {
        tree.uci = lastMoveUci;
      }
      setNavState({ current: tree, parents, lastMoveUci });
      if (updateHistory) {
        const parentKeys = parents.map(p => p.position);
        updateUrl(positionKey, parentKeys, topMoves, depth);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load position');
    } finally {
      setLoading(false);
    }
  }, [depth, topMoves, fetchMoves]);

  // Load initial position from URL, reconstructing parent path if needed
  useEffect(() => {
    const loadFromUrl = async () => {
      const state = getStateFromUrl();

      // If we have a path, we need to fetch each parent position to reconstruct the tree nodes
      // We also need to look up san/uci for each position from the previous parent's children
      const parents: TreeNode[] = [];
      let prevNode: TreeNode | null = null;

      // If we have parents to reconstruct, start from the starting position to get first parent's san/uci
      if (state.path.length > 0) {
        try {
          prevNode = await fetchTree(undefined, 1, 4, 218); // Fetch starting position with children
        } catch {
          console.warn('Could not fetch starting position');
        }
      }

      for (const posKey of state.path) {
        try {
          const parentNode = await fetchTree(posKey, 1, 4, 218); // Fetch with depth 1 to get children

          // Look up this node's san/uci from previous node's children
          if (prevNode && prevNode.children) {
            const matchingChild = prevNode.children.find(c => c.position === posKey);
            if (matchingChild) {
              parentNode.san = matchingChild.san;
              parentNode.uci = matchingChild.uci;
            }
          }

          parents.push(parentNode);
          prevNode = parentNode;
        } catch {
          // If we can't fetch a parent, just skip it
          console.warn('Could not fetch parent position:', posKey);
        }
      }

      // Try to find the UCI/SAN move that led to the current position
      let lastMoveUci: string | undefined;
      let currentSan: string | undefined;
      if (state.position && parents.length > 0) {
        const lastParent = parents[parents.length - 1];
        if (lastParent.children) {
          const matchingChild = lastParent.children.find(c => c.position === state.position);
          if (matchingChild) {
            lastMoveUci = matchingChild.uci;
            currentSan = matchingChild.san;
          }
        }
      }

      loadTree(state.position, parents, false, lastMoveUci, currentSan);
    };

    loadFromUrl();
  }, []); // Only run on mount

  // Handle browser back/forward navigation
  useEffect(() => {
    const handlePopState = async () => {
      const state = getStateFromUrl();

      // Reconstruct parents from path
      const parents: TreeNode[] = [];
      let prevNode: TreeNode | null = null;

      // If we have parents to reconstruct, start from the starting position
      if (state.path.length > 0) {
        try {
          prevNode = await fetchTree(undefined, 1, 4, 218);
        } catch {
          console.warn('Could not fetch starting position');
        }
      }

      for (const posKey of state.path) {
        try {
          const parentNode = await fetchTree(posKey, 1, 4, 218);

          // Look up this node's san/uci from previous node's children
          if (prevNode && prevNode.children) {
            const matchingChild = prevNode.children.find(c => c.position === posKey);
            if (matchingChild) {
              parentNode.san = matchingChild.san;
              parentNode.uci = matchingChild.uci;
            }
          }

          parents.push(parentNode);
          prevNode = parentNode;
        } catch {
          console.warn('Could not fetch parent position:', posKey);
        }
      }

      // Try to find the UCI/SAN move that led to the current position
      let lastMoveUci: string | undefined;
      let currentSan: string | undefined;
      if (state.position && parents.length > 0) {
        const lastParent = parents[parents.length - 1];
        if (lastParent.children) {
          const matchingChild = lastParent.children.find(c => c.position === state.position);
          if (matchingChild) {
            lastMoveUci = matchingChild.uci;
            currentSan = matchingChild.san;
          }
        }
      }

      loadTree(state.position, parents, false, lastMoveUci, currentSan);
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, [loadTree]);

  // Reload current position when topMoves or depth changes
  const prevTopMoves = usePrevious(topMoves);
  const prevDepth = usePrevious(depth);

  useEffect(() => {
    // Only reload if values actually changed (not on initial mount)
    const topMovesChanged = prevTopMoves !== undefined && prevTopMoves !== topMoves;
    const depthChanged = prevDepth !== undefined && prevDepth !== depth;

    if (navState && hasAppliedUrlState && (topMovesChanged || depthChanged)) {
      // Reload with same position but new parameters
      const positionKey = navState.current.position;
      loadTree(
        positionKey,
        navState.parents,
        true,
        navState.lastMoveUci,
        navState.current.san
      );
    }
  }, [topMoves, depth, prevTopMoves, prevDepth, navState, hasAppliedUrlState, loadTree]);

  const handleNodeClick = (node: TreeNode, ancestors: TreeNode[] = []) => {
    if (navState && node.position !== navState.current.position) {
      // Build full parent chain: existing parents + current + any intermediate ancestors
      const fullParents = [...navState.parents, navState.current, ...ancestors];
      loadTree(node.position, fullParents, true, node.uci, node.san);
    }
  };

  const handleGoToParent = () => {
    if (navState && navState.parents.length > 0) {
      const newParents = [...navState.parents];
      const parent = newParents.pop()!;
      loadTree(parent.position, newParents, true, parent.uci, parent.san);
    }
  };

  // Handle FEN input submission
  const handleFenSubmit = async () => {
    if (!fenInput.trim()) {
      setFenError('Please enter a FEN string');
      return;
    }

    setFenError(null);
    try {
      const result = await fetchPositionByFen(fenInput.trim());
      // Load the position with no parents (fresh navigation)
      loadTree(result.position, [], true);
      setShowFenDialog(false);
      setFenInput('');
    } catch (err) {
      setFenError(err instanceof Error ? err.message : 'Invalid FEN');
    }
  };

  // Handle move made on the board - find matching child and navigate
  const handleMove = (sourceSquare: string, targetSquare: string, piece: string): boolean => {
    if (!navState) return false;

    const { current } = navState;
    if (!current.children || current.children.length === 0) return false;

    // Build UCI string - add promotion piece if pawn reaches back rank
    let uci = sourceSquare + targetSquare;
    const isPawn = piece.toLowerCase().endsWith('p');
    const isPromotion = isPawn && (targetSquare[1] === '8' || targetSquare[1] === '1');
    if (isPromotion) {
      uci += 'q'; // Default to queen promotion
    }

    // Find child with matching UCI move
    const matchingChild = current.children.find(child => child.uci === uci);

    if (matchingChild) {
      loadTree(matchingChild.position, [...navState.parents, current], true, matchingChild.uci, matchingChild.san);
      return true;
    }

    // Try other promotion pieces if queen didn't match
    if (isPromotion) {
      for (const promo of ['r', 'b', 'n']) {
        const promoUci = sourceSquare + targetSquare + promo;
        const promoChild = current.children.find(child => child.uci === promoUci);
        if (promoChild) {
          loadTree(promoChild.position, [...navState.parents, current], true, promoChild.uci, promoChild.san);
          return true;
        }
      }
    }

    return false; // Move not found in children - reject it
  };

  const formatStats = (node: TreeNode): string => {
    if (node.count === 0) return 'No games';
    return `${node.count.toLocaleString()}`;
  };

  const formatEval = (node: TreeNode): string | null => {
    if (node.dtm) {
      const sign = node.dtm > 0 ? '+' : '';
      return `M${sign}${node.dtm}`;
    }
    if (node.cp !== undefined && node.cp !== 0) {
      const sign = node.cp > 0 ? '+' : '';
      return `${sign}${(node.cp / 100).toFixed(2)}`;
    }
    return null;
  };

  // Build PGN-style move list from parents + current (must be before early returns)
  const moveHistory = useMemo(() => {
    if (!navState) return [];
    const { current, parents } = navState;
    const moves: { moveNumber: number; white?: string; black?: string; node: TreeNode }[] = [];
    const allNodes = [...parents, current].filter(n => n.san); // Exclude starting position

    for (let i = 0; i < allNodes.length; i++) {
      const node = allNodes[i];
      const moveNumber = Math.floor(i / 2) + 1;
      const isWhiteMove = i % 2 === 0;

      if (isWhiteMove) {
        moves.push({ moveNumber, white: node.san, node });
      } else {
        if (moves.length > 0) {
          moves[moves.length - 1].black = node.san;
        }
      }
    }
    return moves;
  }, [navState]);

  // Sort all possible moves: by score (cp/dtm) descending, then count, then win%
  const sortedMoves = useMemo(() => {
    if (!navState || !navState.current.children) return [];
    const children = [...navState.current.children];

    return children.sort((a, b) => {
      // First priority: positions with evaluation (cp or dtm)
      const aHasEval = a.cp !== undefined || a.dtm !== undefined;
      const bHasEval = b.cp !== undefined || b.dtm !== undefined;

      if (aHasEval && bHasEval) {
        // Both have evals - compare them
        // DTM takes priority (mate is better/worse than centipawns)
        if (a.dtm !== undefined && b.dtm !== undefined) {
          return b.dtm - a.dtm; // Higher DTM is better (positive = winning)
        }
        if (a.dtm !== undefined) return -1; // a has mate, prioritize
        if (b.dtm !== undefined) return 1;  // b has mate, prioritize

        // Both have centipawn evals
        return (b.cp ?? 0) - (a.cp ?? 0);
      }

      if (aHasEval) return -1; // a has eval, b doesn't
      if (bHasEval) return 1;  // b has eval, a doesn't

      // Neither has eval - sort by count, then win%
      if (a.count !== b.count) {
        return b.count - a.count;
      }
      return b.win_pct - a.win_pct;
    });
  }, [navState]);

  if (loading && !navState) {
    return <div className="loading">Loading...</div>;
  }

  if (error) {
    return <div className="error">{error}</div>;
  }

  if (!navState) {
    return <div className="error">No data</div>;
  }

  const { current, parents, lastMoveUci } = navState;

  // Get the last move made for highlighting on the main board
  // Use lastMoveUci if available (from navigation), otherwise fall back to current.uci (from API)
  const mainBoardHighlights = getHighlightSquares(lastMoveUci || current.uci);
  const mainBoardOrientation = getBoardOrientation(boardOrientation, current.fen);

  return (
    <div className="chess-graph-container">
      {/* Main board area with sidebars */}
      <div className="main-board-area">
        {/* Left sidebar: title, stats, controls, move history */}
        <div className="left-sidebar">
          <h1 className="app-title">♞ ChessGraph</h1>

          {stats && (
            <div className="stats-panel" title="Database statistics">
              <div className="stats-title">Database Stats</div>
              <div className="stats-columns">
                <div className="stats-column">
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.total_games)}</span>
                    <span className="stat-label">games</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.total_positions)}</span>
                    <span className="stat-label">positions</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.evaluated_positions)}</span>
                    <span className="stat-label">evaluated</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.dtm_positions)}</span>
                    <span className="stat-label">pos w/ DTM</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.dtz_positions)}</span>
                    <span className="stat-label">pos w/ DTZ</span>
                  </div>
                </div>
                <div className="stats-column">
                  <div className="stat-item">
                    <span className="stat-value">{formatNumber(stats.total_blocks)}</span>
                    <span className="stat-label">blocks</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatBytes(stats.uncompressed_bytes)}</span>
                    <span className="stat-label">size</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-value">{formatBytes(stats.compressed_bytes)}</span>
                    <span className="stat-label">on disk</span>
                  </div>
                  {stats.uncompressed_bytes > 0 && stats.compressed_bytes > 0 && (
                    <div className="stat-item">
                      <span className="stat-value">{(stats.uncompressed_bytes / stats.compressed_bytes).toFixed(4)}x</span>
                      <span className="stat-label">compress</span>
                    </div>
                  )}
                  {stats.compressed_bytes > 0 && stats.total_positions > 0 && (
                    <div className="stat-item">
                      <span className="stat-value">{(stats.compressed_bytes / stats.total_positions).toFixed(3)}</span>
                      <span className="stat-label">bytes/pos</span>
                    </div>
                  )}
                </div>
              </div>
              {stats.read_only && <div className="stats-readonly">Read-only mode</div>}
            </div>
          )}

          <div className="controls-group">
            <div className="control-row">
              <label htmlFor="top-moves">Moves:</label>
              <select
                id="top-moves"
                value={topMoves}
                onChange={(e) => onTopMovesChange?.(Number(e.target.value))}
              >
                <option value={2}>2</option>
                <option value={3}>3</option>
                <option value={4}>4</option>
                <option value={5}>5</option>
                <option value={6}>6</option>
              </select>
            </div>
            <div className="control-row">
              <label htmlFor="tree-depth">Depth:</label>
              <select
                id="tree-depth"
                value={depth}
                onChange={(e) => onDepthChange?.(Number(e.target.value))}
              >
                <option value={1}>1</option>
                <option value={2}>2</option>
                <option value={3}>3</option>
              </select>
            </div>
            <div className="control-row">
              <label htmlFor="board-orientation">View:</label>
              <select
                id="board-orientation"
                value={boardOrientation}
                onChange={(e) => setBoardOrientation(e.target.value as BoardOrientation)}
              >
                <option value="auto">Auto</option>
                <option value="white">White</option>
                <option value="black">Black</option>
              </select>
            </div>
            <button
              className="fen-btn"
              onClick={() => setShowFenDialog(true)}
            >
              FEN Input
            </button>
          </div>

          {/* FEN Input Dialog */}
          {showFenDialog && (
            <div className="fen-dialog-overlay" onClick={() => setShowFenDialog(false)}>
              <div className="fen-dialog" onClick={(e) => e.stopPropagation()}>
                <div className="fen-dialog-title">Enter FEN</div>
                <input
                  type="text"
                  className="fen-input"
                  value={fenInput}
                  onChange={(e) => setFenInput(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleFenSubmit()}
                  placeholder="e.g. rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1"
                  autoFocus
                />
                {fenError && <div className="fen-error">{fenError}</div>}
                <div className="fen-dialog-buttons">
                  <button className="fen-cancel-btn" onClick={() => setShowFenDialog(false)}>
                    Cancel
                  </button>
                  <button className="fen-submit-btn" onClick={handleFenSubmit}>
                    Go
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Move history - separate column */}
        <div className="move-history">
          <div className="move-history-header">
            <button
              className="back-btn"
              onClick={handleGoToParent}
              disabled={parents.length === 0}
              title="Go back"
            >
              ←
            </button>
            <div className="move-history-title">Moves</div>
          </div>
          <div className="move-list">
            {moveHistory.length === 0 ? (
              <div className="no-moves">Starting position</div>
            ) : (
              moveHistory.map((move, idx) => (
                <div key={idx} className="move-row">
                  <span className="move-number">{move.moveNumber}.</span>
                  <span
                    className="white-move clickable"
                    onClick={() => {
                      // Navigate to this position: find the node index and rebuild parents
                      const nodeIndex = idx * 2; // White moves are at even indices
                      const allNodes = [...parents, current].filter(n => n.san);
                      if (nodeIndex < allNodes.length) {
                        const targetNode = allNodes[nodeIndex];
                        const newParents = allNodes.slice(0, nodeIndex);
                        loadTree(targetNode.position, newParents, true, targetNode.uci, targetNode.san);
                      }
                    }}
                  >
                    {move.white || ''}
                  </span>
                  <span
                    className={`black-move ${move.black ? 'clickable' : ''}`}
                    onClick={() => {
                      if (!move.black) return;
                      // Navigate to this position: find the node index and rebuild parents
                      const nodeIndex = idx * 2 + 1; // Black moves are at odd indices
                      const allNodes = [...parents, current].filter(n => n.san);
                      if (nodeIndex < allNodes.length) {
                        const targetNode = allNodes[nodeIndex];
                        const newParents = allNodes.slice(0, nodeIndex);
                        loadTree(targetNode.position, newParents, true, targetNode.uci, targetNode.san);
                      }
                    }}
                  >
                    {move.black || ''}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Focused board (main) */}
        <motion.div
          className="board-wrapper focused"
          key={current.position}
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.3 }}
        >
          <Chessboard
            position={current.fen}
            boardWidth={225}
            arePiecesDraggable={true}
            onPieceDrop={handleMove}
            boardOrientation={mainBoardOrientation}
            customSquareStyles={mainBoardHighlights}
          />
          <div className="board-info">
            <div className="move-line">
              {current.san && <span className="move-name">{current.san}</span>}
              <span className="side-to-move">{getSideToMove(current.fen)} to move</span>
            </div>
            <div className="stats">
              {formatStats(current)} games
              {current.count > 0 && (
                <span> • W:{current.win_pct.toFixed(1)}% D:{current.draw_pct.toFixed(1)}% L:{(100 - current.win_pct - current.draw_pct).toFixed(1)}%</span>
              )}
            </div>
            {formatEval(current) && (
            <div className="eval">{formatEval(current)}</div>
          )}
          <WinBar wins={current.wins} draws={current.draws} losses={current.losses} />
        </div>
      </motion.div>

        {/* All possible moves panel with hover preview */}
        <div className="all-moves-container">
          <div className="all-moves-panel">
            <div className="all-moves-title">All Moves ({sortedMoves.length})</div>
            <div className="all-moves-list">
              {sortedMoves.length === 0 ? (
                <div className="no-moves">No moves available</div>
              ) : (
                sortedMoves.map((move) => (
                  <div
                    key={move.position}
                    className="all-moves-row"
                    onClick={() => handleNodeClick(move)}
                    onMouseEnter={() => setHoveredMove(move)}
                    onMouseLeave={() => setHoveredMove(null)}
                  >
                    <span className="move-san">{move.san}</span>
                    <span className="move-eval">
                      {move.dtm !== undefined ? (
                        <span className={move.dtm > 0 ? 'eval-win' : move.dtm < 0 ? 'eval-loss' : ''}>
                          M{move.dtm > 0 ? '+' : ''}{move.dtm}
                        </span>
                      ) : move.cp !== undefined && move.cp !== 0 ? (
                        <span className={move.cp > 0 ? 'eval-win' : 'eval-loss'}>
                          {move.cp > 0 ? '+' : ''}{(move.cp / 100).toFixed(2)}
                        </span>
                      ) : null}
                    </span>
                    <span className="move-count">{move.count.toLocaleString()}</span>
                    {move.count > 0 ? (
                      <span className="move-wdl">
                        <span className="w">{move.win_pct.toFixed(0)}%</span>
                        <span className="d">{move.draw_pct.toFixed(0)}%</span>
                        <span className="l">{(100 - move.win_pct - move.draw_pct).toFixed(0)}%</span>
                      </span>
                    ) : (
                      <span className="move-wdl">
                        <span className="w">-</span>
                        <span className="d">-</span>
                        <span className="l">-</span>
                      </span>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Hover preview board - positioned absolutely */}
          {hoveredMove && (
            <div className="hover-preview">
              <div className="hover-preview-title">{hoveredMove.san}</div>
              <Chessboard
                position={hoveredMove.fen}
                boardWidth={180}
                arePiecesDraggable={false}
                boardOrientation={getBoardOrientation(boardOrientation, hoveredMove.fen)}
                customSquareStyles={getHighlightSquares(hoveredMove.uci)}
              />
              <div className="hover-preview-stats">
                {formatStats(hoveredMove)} games
                {hoveredMove.count > 0 && (
                  <span> • W:{hoveredMove.win_pct.toFixed(0)}%</span>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Connector from root to children */}
      {current.children && current.children.length > 0 && (
        <div className="tree-connector">
          <div className="connector-line" />
        </div>
      )}

      {/* Recursive tree rendering */}
      <AnimatePresence>
        {current.children && current.children.length > 0 && (
          <RootChildren
            children={current.children}
            depth={depth}
            topMoves={topMoves}
            onNodeClick={handleNodeClick}
            formatStats={formatStats}
            formatEval={formatEval}
            boardOrientation={boardOrientation}
          />
        )}
      </AnimatePresence>

      {loading && (
        <div className="loading-overlay">Loading...</div>
      )}
    </div>
  );
}

// Root level children with pagination
interface RootChildrenProps {
  children: TreeNode[];
  depth: number;
  topMoves: number;
  onNodeClick: (node: TreeNode) => void;
  formatStats: (node: TreeNode) => string;
  formatEval: (node: TreeNode) => string | null;
  boardOrientation: BoardOrientation;
}

function RootChildren({ children, depth, topMoves, onNodeClick, formatStats, formatEval, boardOrientation }: RootChildrenProps) {
  const [offset, setOffset] = useState(0);

  const totalChildren = children.length;
  const canPaginate = totalChildren > topMoves;
  const visibleChildren = useMemo(() => {
    if (!canPaginate) return children;
    const result: TreeNode[] = [];
    for (let i = 0; i < topMoves; i++) {
      result.push(children[(offset + i) % totalChildren]);
    }
    return result;
  }, [children, offset, topMoves, totalChildren, canPaginate]);

  const handlePrev = () => {
    setOffset((prev) => (prev - 1 + totalChildren) % totalChildren);
  };

  const handleNext = () => {
    setOffset((prev) => (prev + 1) % totalChildren);
  };

  return (
    <motion.div
      className="children-group"
      initial={{ opacity: 0, y: 30 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: 30 }}
      transition={{ duration: 0.3 }}
    >
      <div className="board-level">
        {canPaginate && (
          <button className="paginate-btn prev" onClick={handlePrev}>‹</button>
        )}
        {visibleChildren.map((child, idx) => (
          <TreeBranch
            key={child.position}
            node={child}
            level={1}
            maxLevel={depth}
            index={idx}
            visibleMoves={topMoves}
            ancestors={[]}
            onNodeClick={onNodeClick}
            formatStats={formatStats}
            formatEval={formatEval}
            boardOrientation={boardOrientation}
          />
        ))}
        {canPaginate && (
          <button className="paginate-btn next" onClick={handleNext}>›</button>
        )}
      </div>
    </motion.div>
  );
}

// Recursive tree branch component
interface TreeBranchProps {
  node: TreeNode;
  level: number;
  maxLevel: number;
  index: number;
  visibleMoves: number;
  ancestors: TreeNode[];  // Chain of nodes from current to this node's parent
  onNodeClick: (node: TreeNode, ancestors: TreeNode[]) => void;
  formatStats: (node: TreeNode) => string;
  formatEval: (node: TreeNode) => string | null;
  boardOrientation: BoardOrientation;
}

function TreeBranch({ node, level, maxLevel, index, visibleMoves, ancestors, onNodeClick, formatStats, formatEval, boardOrientation }: TreeBranchProps) {
  const [offset, setOffset] = useState(0);

  const getBoardSize = (lvl: number): number => {
    const sizes = [225, 135, 105, 75, 52];
    return sizes[Math.min(lvl, sizes.length - 1)];
  };

  const hasChildren = node.children && node.children.length > 0;
  const showChildren = level < maxLevel && hasChildren;

  // Use children directly - API returns them in the correct order
  const nodeChildren = node.children || [];

  // Pagination for children
  const totalChildren = nodeChildren.length;
  const canPaginate = totalChildren > visibleMoves;
  const visibleChildren = useMemo(() => {
    if (nodeChildren.length === 0) return [];
    if (!canPaginate) return nodeChildren;
    // Wrap around if needed
    const result: TreeNode[] = [];
    for (let i = 0; i < visibleMoves; i++) {
      result.push(nodeChildren[(offset + i) % totalChildren]);
    }
    return result;
  }, [nodeChildren, offset, visibleMoves, totalChildren, canPaginate]);

  const handlePrev = (e: React.MouseEvent) => {
    e.stopPropagation();
    setOffset((prev) => (prev - 1 + totalChildren) % totalChildren);
  };

  const handleNext = (e: React.MouseEvent) => {
    e.stopPropagation();
    setOffset((prev) => (prev + 1) % totalChildren);
  };

  return (
    <div className="child-branch">
      <motion.div
        className={`board-wrapper level-${level}`}
        onClick={() => onNodeClick(node, ancestors)}
        whileHover={{ scale: 1.05 }}
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.2, delay: index * 0.03 }}
      >
        <Chessboard
          position={node.fen}
          boardWidth={getBoardSize(level)}
          arePiecesDraggable={false}
          boardOrientation={getBoardOrientation(boardOrientation, node.fen)}
          customSquareStyles={getHighlightSquares(node.uci)}
        />
        <div className={`board-info ${level >= 2 ? 'small' : ''}`}>
          <div className="move-line">
            <span className="move-name">{node.san}</span>
            <span className={`side-to-move ${level >= 2 ? 'small' : ''}`}>{getSideToMove(node.fen)} to move</span>
          </div>
          {level < 2 && (
            <div className="stats">
              {formatStats(node)}
              {node.count > 0 && (
                <span> • W:{node.win_pct.toFixed(1)}% D:{node.draw_pct.toFixed(1)}% L:{(100 - node.win_pct - node.draw_pct).toFixed(1)}%</span>
              )}
            </div>
          )}
          {level < 2 && formatEval(node) && (
            <div className="eval">{formatEval(node)}</div>
          )}
          <WinBar wins={node.wins} draws={node.draws} losses={node.losses} />
        </div>
      </motion.div>

      {showChildren && (
        <>
          <div className="branch-connector">
            <div className="connector-line" />
          </div>
          <div className={`descendants-group level-${level + 1}`}>
            {canPaginate && (
              <button className="paginate-btn prev" onClick={handlePrev}>‹</button>
            )}
            {visibleChildren.map((child, idx) => (
              <TreeBranch
                key={child.position}
                node={child}
                level={level + 1}
                maxLevel={maxLevel}
                index={idx}
                visibleMoves={visibleMoves}
                ancestors={[...ancestors, node]}
                onNodeClick={onNodeClick}
                formatStats={formatStats}
                formatEval={formatEval}
                boardOrientation={boardOrientation}
              />
            ))}
            {canPaginate && (
              <button className="paginate-btn next" onClick={handleNext}>›</button>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// Win/Draw/Loss bar component
function WinBar({ wins, draws, losses }: { wins: number; draws: number; losses: number }) {
  const total = wins + draws + losses;
  if (total === 0) return null;

  const winPct = (wins / total) * 100;
  const drawEnd = winPct + (draws / total) * 100;

  return (
    <div
      className="win-bar"
      style={{
        '--win-pct': `${winPct}%`,
        '--draw-end': `${drawEnd}%`,
      } as React.CSSProperties}
    />
  );
}
