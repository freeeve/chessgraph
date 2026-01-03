import { useState, useEffect } from 'react';
import ChessGraph from './ChessGraph';
import { fetchStats, StatsResponse } from './api';

function App() {
  const [topMoves, setTopMoves] = useState(3);
  const [depth, setDepth] = useState(2);
  const [stats, setStats] = useState<StatsResponse | null>(null);

  useEffect(() => {
    // Initial fetch
    fetchStats()
      .then(setStats)
      .catch(console.error);

    // Poll every 30 seconds
    const interval = setInterval(() => {
      fetchStats()
        .then(setStats)
        .catch(console.error);
    }, 30000);

    return () => clearInterval(interval);
  }, []);

  return (
    <ChessGraph
      topMoves={topMoves}
      depth={depth}
      stats={stats}
      onTopMovesChange={setTopMoves}
      onDepthChange={setDepth}
    />
  );
}

export default App;

