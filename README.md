# Binance MCP

<p align="center">
  <img alt="MCP logo" src="assets/binance-logo.png" width="180" height="180" />
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  <img alt="Binance logo" src="assets/mcp-logo.png" width="180" height="180" />
</p>

<p align="center">
  <strong>A TypeScript MCP server for Binance market data, account insights, and Convert workflows.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Model%20Context%20Protocol-Compatible-00BFA6?style=for-the-badge" alt="MCP compatible badge" />
  <img src="https://img.shields.io/badge/Binance-API%20Integration-FCD535?style=for-the-badge" alt="Binance integration badge" />
  <img src="https://img.shields.io/badge/License-MIT-111827?style=for-the-badge" alt="MIT license badge" />
</p>

## What This Server Does

`binance-mcp` exposes a focused set of MCP tools for working with Binance data from Claude Desktop or any MCP-compatible client.

It covers:

- Live spot prices
- Best bid/ask quotes
- Spot account balances
- Funding wallet balances
- Combined account and portfolio views
- Binance Convert quote and order workflows
- Convert trade history

## Tools

| Tool | Purpose | Auth |
| --- | --- | --- |
| `get_last_prices` | Fetch latest last-traded prices for one or more spot symbols | No |
| `get_book_ticker` | Fetch current best bid and ask for one or more spot symbols | No |
| `get_spot_account` | Return spot account balances and account flags | Yes |
| `get_funding_wallet` | Return funding wallet balances | Yes |
| `get_account_overview` | Merge spot and funding balances into one view | Yes |
| `get_portfolio_snapshot` | Build a live valuation snapshot across balances and prices | Yes |
| `get_convert_quote` | Request a Binance Convert quote without executing it | Yes |
| `accept_convert_quote` | Execute a live Convert quote | Yes |
| `get_convert_order_status` | Check the status of a Convert order | Yes |
| `get_convert_trade_history` | Fetch Convert trade history for a time window | Yes |

## Why This MCP Is Useful

- Keeps Binance workflows inside your AI assistant instead of switching between apps.
- Separates safe quote retrieval from live trade execution.
- Uses short-lived market-data caching and websocket updates for fast price lookups.
- Returns structured JSON so Claude can reason over balances, prices, and conversions cleanly.

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Build the server

```bash
npm run build
```

### 3. Run locally

```bash
npm start
```

The server communicates over `stdio`, which is the standard transport used by Claude Desktop.

## Full Documentation

If you want the deeper implementation notes, endpoint details, and design rationale, see [DOCUMENTATION.md](DOCUMENTATION.md).

## Claude Desktop Setup

Use the compiled server entry point in your claude_desktop_config.json file.

### Example configuration

```json
{
  "mcpServers": {
    "binance-mcp": {
      "command": "node",
      "args": ["C:/Binance-mcp/build/index.js"],
      "env": {
        "BINANCE_API_KEY": "your_api_key",
        "BINANCE_API_SECRET": "your_api_secret"
      }
    }
  }
}
```

### Notes

- Public market tools work without credentials.
- Account and Convert tools require `BINANCE_API_KEY` and `BINANCE_API_SECRET`.
- If you want to customize Binance endpoints, you can override the environment variables listed below.

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `BINANCE_API_KEY` | none | Binance API key for signed endpoints |
| `BINANCE_API_SECRET` | none | Binance API secret for request signing |
| `BINANCE_BASE_URL` | `https://api.binance.com` | Base URL for signed REST requests |
| `BINANCE_PUBLIC_BASE_URL` | `https://data-api.binance.vision` | Base URL for public market-data requests |
| `BINANCE_STREAM_BASE_URL` | `wss://stream.binance.com:9443/stream` | Binance websocket stream endpoint |
| `BINANCE_TIMEOUT_MS` | `8000` | Request timeout in milliseconds |
| `BINANCE_RECV_WINDOW_MS` | `5000` | Binance signed request recvWindow |
| `BINANCE_MAX_RETRIES` | `1` | Retry attempts for safe requests |
| `BINANCE_PRICE_CACHE_TTL_MS` | `1500` | Freshness window for market-data cache |
| `BINANCE_STREAM_ENABLED` | `true` | Enables websocket price streaming |

## Example Usage

### Price lookup

```text
Get the latest price for BTCUSDT and ETHUSDT.
```

### Portfolio snapshot

```text
Show my portfolio snapshot in USDT.
```

### Convert flow

```text
Create a Convert quote from BTC to USDT for 0.01 BTC, then show the quote details.
```

## Build and Implementation Details

This project is implemented in TypeScript and uses the official MCP SDK.

- `src/index.ts` registers MCP tools and formats responses.
- `src/binance.ts` handles Binance REST calls, HMAC signing, websocket market data, retries, and portfolio math.
- `build/index.js` is the compiled entry point used by Claude Desktop.

## Safety

- `accept_convert_quote` performs a live conversion.
- Signed endpoints are unavailable until API credentials are present.
- Convert quotes should always be reviewed before execution.
- Binance rate limits still apply, so avoid unnecessary polling.

## License

Released under the MIT License. See [LICENSE](LICENSE).
