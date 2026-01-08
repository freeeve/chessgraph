import React, {useState, useEffect, useCallback, useMemo, useRef} from 'react';
import {motion, AnimatePresence} from 'framer-motion';
import {Chessboard} from 'react-chessboard';
import {
    fetchTree,
    fetchTreeWithMoves,
    fetchPositionStats,
    fetchPositionStatsBatch,
    TreeNode,
    TreeResponse,
    MoveStub,
    PathNode,
    PositionStatsResponse,
    StatsResponse,
    fetchPositionByFen,
    fetchDeepEval
} from './api';

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
    const highlightStyle = {backgroundColor: 'rgba(255, 255, 0, 0.4)'};
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
    parents: TreeNode[];      // Parent nodes for move history display
    sanMoves: string[];       // SAN moves from starting position (for URL)
    lastMoveUci?: string;     // UCI move that led to current position (for highlighting)
}

interface UrlState {
    moves: string[];  // SAN moves from starting position
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

// Check if any W/D/L count is saturated (maxed out at 65535)
const WDL_SATURATED = 65535;

function isWdlSaturated(wins: number, draws: number, losses: number): boolean {
    return wins >= WDL_SATURATED || draws >= WDL_SATURATED || losses >= WDL_SATURATED;
}

// Format numbers with exact count (with commas)
function formatExact(n: number | undefined): string {
    if (n === undefined || n === null) return '0';
    return n.toLocaleString();
}

// Format byte sizes with appropriate units
function formatBytes(bytes: number | undefined): string {
    if (bytes === undefined || bytes === null) return '0 B';
    if (bytes >= 1_000_000_000) return (bytes / 1_000_000_000).toFixed(1) + ' GB';
    if (bytes >= 1_000_000) return (bytes / 1_000_000).toFixed(1) + ' MB';
    if (bytes >= 1_000) return (bytes / 1_000).toFixed(1) + ' KB';
    return bytes + ' B';
}

// Parse URL state from hash - moves are in the hash as e2e4/e7e5/...
function getStateFromUrl(): UrlState {
    const hash = window.location.hash.slice(1); // Remove '#'
    const params = new URLSearchParams(window.location.search);

    // Parse moves from hash: #e2e4/e7e5/g1f3/...
    const moves = hash ? hash.split('/').filter(Boolean) : [];

    const topMovesParam = params.get('moves');  // Note: 'moves' param is for display count
    const depthParam = params.get('depth');

    return {
        moves,
        topMoves: topMovesParam ? parseInt(topMovesParam, 10) : undefined,
        depth: depthParam ? parseInt(depthParam, 10) : undefined,
    };
}

// Update URL with current state - moves go in hash as e4/e5/Nf3/... (SAN notation)
function updateUrl(sanMoves: string[] = [], topMoves?: number, depth?: number) {
    const params = new URLSearchParams();

    if (topMoves !== undefined) {
        params.set('moves', topMoves.toString());
    }
    if (depth !== undefined) {
        params.set('depth', depth.toString());
    }

    const queryString = params.toString();
    const newHash = sanMoves.length > 0 ? `#${sanMoves.join('/')}` : '';
    const newUrl = queryString
        ? `${window.location.pathname}?${queryString}${newHash}`
        : `${window.location.pathname}${newHash}`;

    if (window.location.href !== new URL(newUrl, window.location.origin).href) {
        window.history.pushState(null, '', newUrl);
    }
}

export default function ChessGraph({
                                       topMoves: propTopMoves = 3,
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
    const [isInitialLoad, setIsInitialLoad] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [boardOrientation, setBoardOrientation] = useState<BoardOrientation>('white');
    const [hoveredMove, setHoveredMove] = useState<TreeNode | null>(null);
    const [showFenDialog, setShowFenDialog] = useState(false);
    const [fenInput, setFenInput] = useState('');
    const [fenError, setFenError] = useState<string | null>(null);
    const [deepEvalStatus, setDeepEvalStatus] = useState<string | null>(null);

    // Load tree - can use SAN moves (preferred) or position key (fallback for FEN input)
    const loadTree = useCallback(async (
        sanMoves: string[],
        updateHistory = true,
        positionKeyOverride?: string  // For FEN input where we don't have moves
    ) => {
        setLoading(true);
        setError(null);

        try {
            let response: TreeResponse;
            let parents: TreeNode[] = [];
            let lastMoveUci: string | undefined;

            if (positionKeyOverride) {
                // Direct position access (e.g., from FEN input) - no path
                response = await fetchTree(positionKeyOverride, topMoves);
            } else if (sanMoves.length > 0) {
                // Use moves parameter (SAN) - single API call returns path
                response = await fetchTreeWithMoves(sanMoves, topMoves);
            } else {
                // Starting position
                response = await fetchTree(undefined, topMoves);
            }

            // Build the tree node from response
            const tree: TreeNode = {...response.root};

            // Combine top_moves and other_moves into children array
            const children: TreeNode[] = [...response.top_moves];

            // Convert other_moves (stubs) to TreeNodes with placeholder stats
            if (response.other_moves) {
                for (const stub of response.other_moves) {
                    children.push({
                        position: stub.position,
                        fen: '', // Will be filled when stats are fetched
                        uci: stub.uci,
                        san: stub.san,
                        count: -1, // -1 indicates stats not yet loaded
                        wins: 0,
                        draws: 0,
                        losses: 0,
                        win_pct: 0,
                        draw_pct: 0,
                    });
                }
            }
            tree.children = children;

            // Build parents from the returned path
            if (response.path) {
                parents = response.path.map((p: PathNode) => ({
                    position: p.position,
                    fen: p.fen,
                    uci: p.uci,
                    san: p.san,
                    eco: p.eco,
                    opening: p.opening,
                    cp: p.cp,
                    dtm: p.dtm,
                    has_eval: p.has_eval,
                    count: 0,
                    wins: 0,
                    draws: 0,
                    losses: 0,
                    win_pct: 0,
                    draw_pct: 0,
                }));
                // Remove last element - that's the current position, not a parent
                parents.pop();
                // Set current node's san/uci/cp/dtm/eco from path
                const lastPath = response.path[response.path.length - 1];
                if (lastPath) {
                    tree.san = lastPath.san;
                    tree.uci = lastPath.uci;
                    if (lastPath.cp !== undefined) tree.cp = lastPath.cp;
                    if (lastPath.dtm !== undefined) tree.dtm = lastPath.dtm;
                    if (lastPath.eco) tree.eco = lastPath.eco;
                    if (lastPath.opening) tree.opening = lastPath.opening;
                    lastMoveUci = lastPath.uci; // Get UCI from API for board highlighting
                }
            }

            // Set navigation state
            setNavState(prev => {
                // For background refresh, check if we should update
                if (!updateHistory && prev) {
                    // If user navigated elsewhere, skip this stale update entirely
                    if (prev.current.position !== tree.position) {
                        return prev;
                    }
                }
                return {current: tree, parents, sanMoves, lastMoveUci};
            });

            if (updateHistory) {
                updateUrl(sanMoves, topMoves, depth);
            }

            // Progressively fetch stats for other_moves (stubs)
            if (response.other_moves && response.other_moves.length > 0) {
                const stubPositions = response.other_moves.map(s => s.position);
                const currentPosition = tree.position;

                fetchPositionStatsBatch(stubPositions, 10, (posKey, stats) => {
                    // Update state as each position's stats arrive
                    setNavState(prev => {
                        if (!prev || prev.current.position !== currentPosition) {
                            // User navigated away, skip update
                            return prev;
                        }

                        // Find and update the child with this position
                        const updatedChildren = prev.current.children?.map(child => {
                            if (child.position === posKey) {
                                return {
                                    ...child,
                                    fen: stats.fen,
                                    count: stats.count,
                                    wins: stats.wins,
                                    draws: stats.draws,
                                    losses: stats.losses,
                                    win_pct: stats.win_pct,
                                    draw_pct: stats.draw_pct,
                                    cp: stats.cp,
                                    dtm: stats.dtm,
                                    has_eval: stats.has_eval,
                                };
                            }
                            return child;
                        });

                        return {
                            ...prev,
                            current: {
                                ...prev.current,
                                children: updatedChildren,
                            },
                        };
                    });
                });
            }

            // Progressively fetch evals for path positions (for PGN scores)
            if (response.path && response.path.length > 0) {
                const pathPositions = response.path.map(p => p.position);
                const currentPosition = tree.position;

                fetchPositionStatsBatch(pathPositions, 5, (posKey, stats) => {
                    // Update parents and current node as evals arrive
                    setNavState(prev => {
                        if (!prev || prev.current.position !== currentPosition) {
                            return prev;
                        }

                        // Update parents with eval data
                        const updatedParents = prev.parents.map(parent => {
                            if (parent.position === posKey) {
                                return {
                                    ...parent,
                                    cp: stats.cp,
                                    dtm: stats.dtm,
                                    has_eval: stats.has_eval,
                                };
                            }
                            return parent;
                        });

                        // Also update current node if it matches
                        let updatedCurrent = prev.current;
                        if (prev.current.position === posKey) {
                            updatedCurrent = {
                                ...prev.current,
                                cp: stats.cp,
                                dtm: stats.dtm,
                                has_eval: stats.has_eval,
                            };
                        }

                        return {
                            ...prev,
                            parents: updatedParents,
                            current: updatedCurrent,
                        };
                    });
                });
            }
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to load position');
        } finally {
            setLoading(false);
            setIsInitialLoad(false);
        }
    }, [depth, topMoves]);

    // Load initial position from URL - single API call with moves parameter
    useEffect(() => {
        const state = getStateFromUrl();
        loadTree(state.moves, false);
    }, []); // Only run on mount

    // Handle browser back/forward navigation
    useEffect(() => {
        const handlePopState = () => {
            const state = getStateFromUrl();
            loadTree(state.moves, false);
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
            // Reload with same moves but new parameters
            loadTree(navState.sanMoves, true);
        }
    }, [topMoves, depth, prevTopMoves, prevDepth, navState, hasAppliedUrlState, loadTree]);

    const handleNodeClick = (node: TreeNode, ancestors: TreeNode[] = []) => {
        if (navState && node.position !== navState.current.position && node.san) {
            // Append SANs for all ancestors + the clicked node
            const ancestorSans = ancestors.map(a => a.san).filter((s): s is string => !!s);
            const newMoves = [...navState.sanMoves, ...ancestorSans, node.san];
            // Fetch tree for new position
            loadTree(newMoves, true);
        }
    };

    const handleGoToParent = () => {
        if (navState && navState.sanMoves.length > 0) {
            // Remove last move to go back
            const newMoves = navState.sanMoves.slice(0, -1);
            loadTree(newMoves, true);
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
            // Load the position directly (no move history since we jumped to a FEN)
            loadTree([], true, result.position);
            setShowFenDialog(false);
            setFenInput('');
        } catch (err) {
            setFenError(err instanceof Error ? err.message : 'Invalid FEN');
        }
    };

    // Handle deep eval request
    const handleDeepEval = async () => {
        if (!navState) return;

        setDeepEvalStatus('Queuing...');
        try {
            const result = await fetchDeepEval(navState.current.position);
            setDeepEvalStatus(`Queued ${result.queued} positions`);
            // Clear status after 3 seconds
            setTimeout(() => setDeepEvalStatus(null), 3000);
        } catch (err) {
            setDeepEvalStatus(err instanceof Error ? err.message : 'Failed');
            setTimeout(() => setDeepEvalStatus(null), 3000);
        }
    };

    // Handle move made on the board - find matching child and navigate
    const handleMove = (sourceSquare: string, targetSquare: string, piece: string): boolean => {
        if (!navState) return false;

        const {current} = navState;
        if (!current.children || current.children.length === 0) return false;

        // Build UCI string - add promotion piece if pawn reaches back rank
        let uci = sourceSquare + targetSquare;
        const isPawn = piece.toLowerCase().endsWith('p');
        const isPromotion = isPawn && (targetSquare[1] === '8' || targetSquare[1] === '1');
        if (isPromotion) {
            uci += 'q'; // Default to queen promotion
        }

        // Find child with matching UCI move
        let matchingChild = current.children.find(child => child.uci === uci);

        // Try other promotion pieces if queen didn't match
        if (!matchingChild && isPromotion) {
            for (const promo of ['r', 'b', 'n']) {
                const promoUci = sourceSquare + targetSquare + promo;
                matchingChild = current.children.find(child => child.uci === promoUci);
                if (matchingChild) break;
            }
        }

        if (matchingChild && matchingChild.san) {
            const newMoves = [...navState.sanMoves, matchingChild.san];
            // Fetch tree for new position
            loadTree(newMoves, true);
            return true;
        }

        return false; // Move not found in children - reject it
    };

    const formatStats = (node: TreeNode): string => {
        if (node.count === -1) return 'Loading...';
        if (node.count === 0) return 'No games';
        return `${node.count.toLocaleString()}`;
    };

    // CPUnknown sentinel value from backend (-32767 means no Stockfish evaluation)
    const CP_UNKNOWN = -32767;

    const hasValidCP = (cp: number | undefined): boolean => {
        return cp !== undefined && cp !== CP_UNKNOWN;
    };

    const formatEval = (node: TreeNode): string | null => {
        if (node.dtm) {
            const sign = node.dtm > 0 ? '+' : '';
            return `M${sign}${node.dtm}`;
        }
        if (hasValidCP(node.cp)) {
            const sign = node.cp! > 0 ? '+' : '';
            return `${sign}${(node.cp! / 100).toFixed(2)}`;
        }
        return null;
    };

    const getEvalClass = (node: TreeNode, parentFen?: string): string => {
        // When showing child moves, we want to color from the perspective of who made the move
        // parentFen tells us who was to move before this position was reached
        // If parentFen has white to move, then WHITE made the move to reach this child

        let whiteMadeTheMove: boolean;
        if (parentFen) {
            // Parent's side to move is who made this move
            whiteMadeTheMove = getSideToMove(parentFen) === 'white';
        } else {
            // No parent - we're showing current position, color for side to move
            // Positive = good for white, so if white to move, positive is good
            whiteMadeTheMove = getSideToMove(node.fen) === 'white';
        }

        if (node.dtm) {
            // Positive DTM = white winning, negative = black winning
            const isGoodForMover = whiteMadeTheMove ? node.dtm > 0 : node.dtm < 0;
            const isBadForMover = whiteMadeTheMove ? node.dtm < 0 : node.dtm > 0;
            if (isGoodForMover) return 'eval-win';
            if (isBadForMover) return 'eval-loss';
            return 'eval-draw';
        }
        if (hasValidCP(node.cp)) {
            // Positive CP = white advantage, negative = black advantage
            const isGoodForMover = whiteMadeTheMove ? node.cp! > 0 : node.cp! < 0;
            const isBadForMover = whiteMadeTheMove ? node.cp! < 0 : node.cp! > 0;
            if (isGoodForMover) return 'eval-win';
            if (isBadForMover) return 'eval-loss';
        }
        return 'eval-draw';
    };

    // Build PGN-style move list from parents + current (must be before early returns)
    const moveHistory = useMemo(() => {
        if (!navState) return [];
        const {current, parents} = navState;
        const moves: {
            moveNumber: number;
            white?: string;
            black?: string;
            whiteNode?: TreeNode;
            blackNode?: TreeNode
        }[] = [];
        const allNodes = [...parents, current].filter(n => n.san); // Exclude starting position

        for (let i = 0; i < allNodes.length; i++) {
            const node = allNodes[i];
            const moveNumber = Math.floor(i / 2) + 1;
            const isWhiteMove = i % 2 === 0;

            if (isWhiteMove) {
                moves.push({moveNumber, white: node.san, whiteNode: node});
            } else {
                if (moves.length > 0) {
                    moves[moves.length - 1].black = node.san;
                    moves[moves.length - 1].blackNode = node;
                }
            }
        }
        return moves;
    }, [navState]);

    // Format score for PGN comment
    const formatPgnScore = (node: TreeNode | undefined): string | null => {
        if (!node) return null;
        if (node.dtm) {
            return `M${node.dtm > 0 ? '+' : ''}${node.dtm}`;
        }
        if (hasValidCP(node.cp)) {
            const score = node.cp! / 100;
            return `${score > 0 ? '+' : ''}${score.toFixed(1)}`;
        }
        return null;
    };

    // Sort all possible moves: by score (cp/dtm) descending, then count, then win%
    // Moves with count === -1 are still loading and sorted last
    const sortedMoves = useMemo(() => {
        if (!navState || !navState.current.children) return [];
        const children = [...navState.current.children];

        // Determine if it's black to move - if so, lower evals are better
        const isBlackToMove = getSideToMove(navState.current.fen) === 'black';
        const evalMultiplier = isBlackToMove ? -1 : 1;

        return children.sort((a, b) => {
            // Loading moves go last
            const aLoading = a.count === -1;
            const bLoading = b.count === -1;
            if (aLoading && !bLoading) return 1;
            if (!aLoading && bLoading) return -1;
            if (aLoading && bLoading) return 0; // Both loading, maintain order

            // First priority: positions with evaluation (valid cp or dtm)
            const aHasEval = hasValidCP(a.cp) || a.dtm !== undefined;
            const bHasEval = hasValidCP(b.cp) || b.dtm !== undefined;

            if (aHasEval && bHasEval) {
                // Both have evals - compare them
                // DTM takes priority (mate is better/worse than centipawns)
                if (a.dtm !== undefined && b.dtm !== undefined) {
                    return (b.dtm - a.dtm) * evalMultiplier;
                }
                if (a.dtm !== undefined) return -1; // a has mate, prioritize
                if (b.dtm !== undefined) return 1;  // b has mate, prioritize

                // Both have centipawn evals
                return ((b.cp ?? 0) - (a.cp ?? 0)) * evalMultiplier;
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

    // Default starting position for when tree fails to load
    const defaultPosition: TreeNode = {
        position: '',
        fen: 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',
        count: 0,
        wins: 0,
        draws: 0,
        losses: 0,
        win_pct: 0,
        draw_pct: 0,
    };

    // Use navState if available, otherwise use defaults
    const current = navState?.current || defaultPosition;
    const lastMoveUci = navState?.lastMoveUci;

    // Get the last move made for highlighting on the main board
    // Use lastMoveUci if available (from navigation), otherwise fall back to current.uci (from API)
    const mainBoardHighlights = getHighlightSquares(lastMoveUci || current.uci);
    const mainBoardOrientation = getBoardOrientation(boardOrientation, current.fen);

    return (
        <div className="chess-graph-container">
            {/* Error banner - shows at top but doesn't block UI */}
            {error && (
                <div className="error-banner">
                    Tree endpoint error: {error}
                    <button className="error-dismiss" onClick={() => setError(null)}>×</button>
                </div>
            )}

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

                                    <div className="stat-item" title="Memtable (in-memory buffer) positions">
                                        <span className="stat-value">{formatNumber(stats.memtable_positions)}</span>
                                        <span className="stat-label">memtable pos</span>
                                    </div>
                                    <div className="stat-item" title="Memtable (in-memory buffer) space">
                                        <span className="stat-value">{formatBytes(stats.memtable_positions * 40)}</span>
                                        <span className="stat-label">memtable mem</span>
                                    </div>
                                    <div className="stat-item" title="L0 files (unsorted, fast writes)">
                                        <span
                                            className="stat-value">{formatNumber(stats.l0_positions)} ({stats.l0_files})</span>
                                        <span className="stat-label">L0 pos (files)</span>
                                    </div>
                                    <div className="stat-item" title="L0 files (unsorted, fast writes)">
                                        <span
                                            className="stat-value">{formatBytes(stats.l0_positions * stats.memtable_bytes_per_pos)}</span>
                                        <span className="stat-label">theoretical L0</span>
                                    </div>

                                    <div className="stat-item" title="L1 files (sorted, optimized)">
                                        <span
                                            className="stat-value">{formatNumber(stats.l1_positions)} ({stats.l1_files})</span>
                                        <span className="stat-label">L1 pos (files)</span>
                                    </div>
                                    <div className="stat-item" title="Theoretical L1 size (L1 positions × 40 bytes)">
                                        <span className="stat-value">{formatBytes(stats.l1_positions * 40)}</span>
                                        <span className="stat-label">theoretical L1</span>
                                    </div>

                                    <div className="stat-item" title="L2 files (sorted, optimized)">
                                        <span
                                            className="stat-value">{formatNumber(stats.l2_positions)} ({stats.l2_files})</span>
                                        <span className="stat-label">L2 pos (files)</span>
                                    </div>
                                    <div className="stat-item" title="Theoretical L2 size (L2 positions × 40 bytes)">
                                        <span className="stat-value">{formatBytes(stats.l2_positions * 40)}</span>
                                        <span className="stat-label">theoretical L2</span>
                                    </div>

                                    <div className="stat-item"
                                         title="Theoretical overall size (total positions × 40 bytes)">
                                        <span className="stat-value">{formatBytes(stats.total_positions * 40)}</span>
                                        <span className="stat-label">theoretical total</span>
                                    </div>
                                </div>
                                <div className="stats-column">

                                    <div className="stat-item">
                                        <span className="stat-value">{formatExact(stats.evaluated_positions)}</span>
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

                                    <div className="stat-item"
                                         title="Bytes per position in memtable (key + value size)">
                                        <span
                                            className="stat-value">{stats.memtable_positions > 0 ? stats.memtable_bytes_per_pos.toFixed(3) + '(1.0x)' : '-'}</span>
                                        <span className="stat-label">B/pos</span>
                                    </div>
                                    <div className="stat-item" title="Bytes per position in L0 (compressed)">
                                        <span className="stat-value">
                                          {stats.l0_bytes_per_pos > 0 ? stats.l0_bytes_per_pos.toFixed(3) : '-'}
                                            {stats.l0_compressed_bytes > 0 && <span
                                                className="stat-ratio"> ({((stats.l0_positions * 40) / stats.l0_compressed_bytes).toFixed(3)}x)</span>}
                                        </span>
                                        <span className="stat-label">B/pos</span>
                                    </div>
                                    <div className="stat-item" title="L0 compression ratio (theoretical / disk)">
                                        <span
                                            className="stat-value">{formatBytes(stats.l0_compressed_bytes)} ({stats.l0_compressed_bytes > 0 ? ((stats.l0_positions * 40) / stats.l0_compressed_bytes).toFixed(3) : '-'}x)</span>
                                        <span className="stat-label">compress</span>
                                    </div>

                                    <div className="stat-item" title="Bytes per position in L1 (compressed)">
                                        <span className="stat-value">
                                          {stats.l1_bytes_per_pos > 0 ? stats.l1_bytes_per_pos.toFixed(3) : '-'}
                                            {stats.l1_compressed_bytes > 0 && <span
                                                className="stat-ratio"> ({((stats.l1_positions * 40) / stats.l1_compressed_bytes).toFixed(3)}x)</span>}
                                        </span>
                                        <span className="stat-label">B/pos</span>
                                    </div>
                                    <div className="stat-item" title="L1 compression ratio (theoretical / disk)">
                                        <span
                                            className="stat-value">{formatBytes(stats.l1_compressed_bytes)} ({stats.l1_compressed_bytes > 0 ? ((stats.l1_positions * 40) / stats.l1_compressed_bytes).toFixed(3) : '-'}x)</span>
                                        <span className="stat-label">compress</span>
                                    </div>

                                    <div className="stat-item" title="Bytes per position in L2 (compressed)">
                                        <span className="stat-value">
                                          {stats.l2_positions > 0 ? (stats.l2_compressed_bytes/stats.l2_positions).toFixed(3) : '-'}
                                            {stats.l2_compressed_bytes > 0 && <span
                                                className="stat-ratio"> ({((stats.l2_positions * 40) / stats.l2_compressed_bytes).toFixed(3)}x)</span>}
                                        </span>
                                        <span className="stat-label">B/pos</span>
                                    </div>
                                    <div className="stat-item" title="L2 compression ratio (theoretical / disk)">
                                        <span
                                            className="stat-value">{formatBytes(stats.l2_compressed_bytes)} ({stats.l2_compressed_bytes > 0 ? ((stats.l2_positions * 40) / stats.l2_compressed_bytes).toFixed(3) : '-'}x)</span>
                                        <span className="stat-label">compress</span>
                                    </div>

                                    <div className="stat-item" title="Overall compression ratio (theoretical / disk)">
                                        <span
                                            className="stat-value">{formatBytes(stats.l1_compressed_bytes + stats.l0_compressed_bytes + stats.memtable_bytes)} ({stats.compressed_bytes > 0 ? ((stats.total_positions * 40) / (stats.l1_compressed_bytes + stats.l0_compressed_bytes + stats.memtable_bytes)).toFixed(3) : '-'}x)</span>
                                        <span className="stat-label">compress</span>
                                    </div>
                                </div>
                            </div>
                            {stats.read_only && <div className="stats-readonly">Read-only mode</div>}
                        </div>
                    )}

                    <div className="controls-group">
                        <div className="control-item">
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
                        <div className="control-item">
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
                        <div className="control-item">
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
                        <div>
                            <button className="control-button fen-btn" onClick={() => setShowFenDialog(true)}>
                                Input FEN
                            </button>
                            <button className="control-button pgn-btn" onClick={() => setShowFenDialog(true)}>
                                Input PGN
                            </button>
                            <button
                                className="control-button deep-eval-btn"
                                onClick={handleDeepEval}
                                disabled={deepEvalStatus === 'Queuing...'}
                                title="Queue this position and 3 levels of children for evaluation">
                                {deepEvalStatus || 'Deep Eval'}
                            </button>
                        </div>
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
                            disabled={!navState || navState.sanMoves.length === 0}
                            title="Go back"
                        >
                            ←
                        </button>
                        <div className="move-history-title">Moves</div>
                    </div>
                    {(() => {
                        // Find the last ECO from path or current position
                        const eco = current.eco || current.path?.slice().reverse().find(p => p.eco)?.eco;
                        const opening = current.opening || current.path?.slice().reverse().find(p => p.opening)?.opening;
                        return eco ? (
                            <div className="eco-display">
                                <span className="eco-code">{eco}</span>
                                {opening && <span className="eco-name">{opening}</span>}
                            </div>
                        ) : null;
                    })()}
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
                                            // Navigate to this position using the first N moves
                                            const moveIndex = idx * 2 + 1; // +1 because sanMoves[0] leads to move 1
                                            const newMoves = navState.sanMoves.slice(0, moveIndex);
                                            loadTree(newMoves, true);
                                        }}
                                    >
                    {move.white || ''}
                  </span>
                                    {formatPgnScore(move.whiteNode) && (
                                        <span className="pgn-comment">{`{${formatPgnScore(move.whiteNode)}}`}</span>
                                    )}
                                    <span
                                        className={`black-move ${move.black ? 'clickable' : ''}`}
                                        onClick={() => {
                                            if (!move.black) return;
                                            // Navigate to this position using the first N moves
                                            const moveIndex = idx * 2 + 2; // +2 for black's move
                                            const newMoves = navState.sanMoves.slice(0, moveIndex);
                                            loadTree(newMoves, true);
                                        }}
                                    >
                    {move.black || ''}
                  </span>
                                    {formatPgnScore(move.blackNode) && (
                                        <span className="pgn-comment">{`{${formatPgnScore(move.blackNode)}}`}</span>
                                    )}
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* Focused board (main) */}
                <div className="board-wrapper focused">
                    <Chessboard
                        position={current.fen}
                        boardWidth={225}
                        arePiecesDraggable={true}
                        onPieceDrop={handleMove}
                        boardOrientation={mainBoardOrientation}
                        customSquareStyles={mainBoardHighlights}
                        animationDuration={200}
                    />
                    <div className="board-info">
                        <div className="move-line">
                            {current.san && <span className="move-name">{current.san}</span>}
                            {formatEval(current) && <span
                                className={`move-eval-inline ${getEvalClass(current)}`}>{formatEval(current)}</span>}
                            <span className="game-count">{formatStats(current)}</span>
                            <span className="side-to-move">{getSideToMove(current.fen)} to move</span>
                        </div>
                        <WinBar wins={current.wins} draws={current.draws} losses={current.losses}/>
                    </div>
                </div>

                {/* All possible moves panel with hover preview */}
                <div className="all-moves-container">
                    <div className="all-moves-panel">
                        <div className="all-moves-title">All Moves ({sortedMoves.length})</div>
                        <div className="all-moves-header">
                            <span>Move</span>
                            <span>Eval</span>
                            <span>Games</span>
                            <span>W / D / L</span>
                        </div>
                        <div className="all-moves-list">
                            {sortedMoves.length === 0 ? (
                                <div className="no-moves">No moves available</div>
                            ) : (
                                sortedMoves.map((move) => {
                                    const isLoading = move.count === -1;
                                    return (
                                        <div
                                            key={move.position}
                                            className={`all-moves-row ${isLoading ? 'loading' : ''}`}
                                            onClick={() => handleNodeClick(move)}
                                            onMouseEnter={() => move.fen && setHoveredMove(move)}
                                            onMouseLeave={() => setHoveredMove(null)}
                                        >
                                            <span className="move-san">{move.san}</span>
                                            <span className="move-eval">
                        {move.dtm !== undefined ? (
                            <span className={getEvalClass(move, current.fen)}>
                            M{move.dtm > 0 ? '+' : ''}{move.dtm}
                          </span>
                        ) : hasValidCP(move.cp) ? (
                            <span className={getEvalClass(move, current.fen)}>
                            {move.cp! > 0 ? '+' : ''}{(move.cp! / 100).toFixed(2)}
                          </span>
                        ) : null}
                      </span>
                                            <span className="move-count">
                        {isLoading ? '...' : isWdlSaturated(move.wins, move.draws, move.losses) ? 'many' : formatNumber(move.count)}
                      </span>
                                            <span className="move-bar">
                                                <WinBar wins={move.wins} draws={move.draws} losses={move.losses}/>
                                            </span>
                                        </div>
                                    );
                                })
                            )}
                        </div>
                    </div>

                    {/* Hover preview board - positioned absolutely */}
                    {hoveredMove && hoveredMove.fen && (
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
                    <div className="connector-line"/>
                </div>
            )}

            {/* Recursive tree rendering */}
            {current.children && current.children.length > 0 && (
                <RootChildren
                    children={current.children}
                    depth={depth}
                    topMoves={topMoves}
                    onNodeClick={handleNodeClick}
                    formatStats={formatStats}
                    formatEval={formatEval}
                    getEvalClass={getEvalClass}
                    boardOrientation={boardOrientation}
                    parentFen={current.fen}
                />
            )}

            {loading && (
                <div className="loading-indicator">Loading tree...</div>
            )}
        </div>
    );
}

// Root level children with pagination
interface RootChildrenProps {
    children: TreeNode[];
    depth: number;
    topMoves: number;
    onNodeClick: (node: TreeNode, ancestors?: TreeNode[]) => void;
    formatStats: (node: TreeNode) => string;
    formatEval: (node: TreeNode) => string | null;
    getEvalClass: (node: TreeNode, parentFen?: string) => string;
    boardOrientation: BoardOrientation;
    parentFen: string;
}

function RootChildren({
                          children,
                          depth,
                          topMoves,
                          onNodeClick,
                          formatStats,
                          formatEval,
                          getEvalClass,
                          boardOrientation,
                          parentFen
                      }: RootChildrenProps) {
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
        <div className="children-group">
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
                        getEvalClass={getEvalClass}
                        boardOrientation={boardOrientation}
                        parentFen={parentFen}
                    />
                ))}
                {canPaginate && (
                    <button className="paginate-btn next" onClick={handleNext}>›</button>
                )}
            </div>
        </div>
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
    getEvalClass: (node: TreeNode, parentFen?: string) => string;
    boardOrientation: BoardOrientation;
    parentFen: string;
}

const TreeBranch = React.memo(function TreeBranch({
                                                      node,
                                                      level,
                                                      maxLevel,
                                                      index,
                                                      visibleMoves,
                                                      ancestors,
                                                      onNodeClick,
                                                      formatStats,
                                                      formatEval,
                                                      getEvalClass,
                                                      boardOrientation,
                                                      parentFen
                                                  }: TreeBranchProps) {
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
            <div
                className={`board-wrapper level-${level}`}
                onClick={() => onNodeClick(node, ancestors)}
            >
                <Chessboard
                    key={node.position}
                    position={node.fen}
                    boardWidth={getBoardSize(level)}
                    arePiecesDraggable={false}
                    boardOrientation={getBoardOrientation(boardOrientation, node.fen)}
                    customSquareStyles={getHighlightSquares(node.uci)}
                />
                <div className={`board-info ${level >= 2 ? 'small' : ''}`}>
                    <div className="move-line">
                        <span className="move-name">{node.san}</span>
                        {formatEval(node) && <span
                            className={`move-eval-inline ${getEvalClass(node, parentFen)}`}>{formatEval(node)}</span>}
                        <span className="game-count">{formatStats(node)}</span>
                        {level < 2 && <span className={`side-to-move`}>{getSideToMove(node.fen)} to move</span>}
                    </div>
                    <WinBar wins={node.wins} draws={node.draws} losses={node.losses} compact={level >= 2}/>
                </div>
            </div>

            {showChildren && (
                <>
                    <div className="branch-connector">
                        <div className="connector-line"/>
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
                                getEvalClass={getEvalClass}
                                boardOrientation={boardOrientation}
                                parentFen={node.fen}
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
}, (prevProps, nextProps) => {
    // Custom comparison - only re-render if the node position changes
    return prevProps.node.position === nextProps.node.position &&
        prevProps.level === nextProps.level &&
        prevProps.maxLevel === nextProps.maxLevel &&
        prevProps.visibleMoves === nextProps.visibleMoves &&
        prevProps.boardOrientation === nextProps.boardOrientation;
});

// Win/Draw/Loss bar component with percentages inside
function WinBar({wins, draws, losses, compact = false}: {
    wins: number;
    draws: number;
    losses: number;
    compact?: boolean
}) {
    const total = wins + draws + losses;
    if (total === 0) return null;

    const winPct = (wins / total) * 100;
    const drawPct = (draws / total) * 100;
    const lossPct = (losses / total) * 100;
    const drawEnd = winPct + drawPct;
    // Center of draw section
    const drawCenter = winPct + drawPct / 2;

    return (
        <div
            className={`win-bar ${compact ? 'compact' : ''}`}
            style={{
                '--win-pct': `${winPct}%`,
                '--draw-end': `${drawEnd}%`,
                '--draw-center': `${drawCenter}%`,
            } as React.CSSProperties}
        >
            {!compact && (
                <>
                    <span className="bar-label win">{winPct.toFixed(0)}%</span>
                    <span className="bar-label draw">{drawPct.toFixed(0)}%</span>
                    <span className="bar-label loss">{lossPct.toFixed(0)}%</span>
                </>
            )}
        </div>
    );
}
