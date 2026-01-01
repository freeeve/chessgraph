# ChessGraph UI

Interactive visualization of chess opening possibilities, showing a tree of positions with miniature chessboards.

## Features

- **Hierarchical board view**: Shows the current position with its top moves displayed as smaller boards below
- **Interactive navigation**: Click any child board to focus on it, the view shifts with animations
- **Parent navigation**: Arrow to go back to parent positions
- **Win/Draw/Loss bars**: Visual indicator of game outcomes for each move
- **Evaluation display**: Shows centipawn or mate distance when available
- **Configurable**: Choose how many levels and top moves to display

## API

The UI uses the `/v1/tree/` endpoint which returns a complete game tree:

```
GET /v1/tree/{positionKey}?depth=2&top=4

Response:
{
  "position": "base64-key",
  "fen": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
  "san": "e4",
  "uci": "e2e4",
  "count": 123456,
  "wins": 45000,
  "draws": 35000,
  "losses": 43456,
  "win_pct": 36.5,
  "draw_pct": 28.4,
  "cp": 25,
  "dtm": null,
  "proven_depth": 30,
  "children": [...]
}
```

## Development

```bash
# Install dependencies
npm install

# Start dev server (with API proxy to localhost:8007)
npm run dev

# Build for production
npm run build
```

## Requirements

The API must be running on `localhost:8007` (or configure the proxy in `vite.config.ts`).

```bash
cd ../api
./api --tablebase ./data/tablebase --max-depth 7
```

## Architecture

- `src/api.ts` - API client for fetching tree data
- `src/ChessGraph.tsx` - Main graph visualization component
- `src/App.tsx` - Root component with configuration controls

## Libraries

- **React 18** - UI framework
- **Vite** - Build tool
- **react-chessboard** - Chess board rendering
- **framer-motion** - Animations

