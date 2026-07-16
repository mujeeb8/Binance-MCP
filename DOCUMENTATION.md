# Binance MCP Server Documentation

This repository implements a professional Binance MCP server in TypeScript for Claude and any other MCP-compatible client.

## What This Server Does

The server exposes tools for:

- Live Binance spot market prices
- Current best bid/ask on Binance spot markets
- Spot account balances
- Funding wallet balances
- A combined account overview
- Binance Convert quote requests
- Binance Convert quote acceptance
- Binance Convert order status
- Binance Convert trade history

## Official Binance Docs Used

The implementation was based on the latest official Binance documentation available at the time of build:

- Binance Developer Docs overview: https://developers.binance.com/en/docs/introduction
- Spot REST general information: https://developers.binance.com/en/docs/products/spot/rest-api
- Convert introduction: https://developers.binance.com/en/docs/products/convert/Introduction
- Convert trade endpoints: https://developers.binance.com/en/docs/catalog/core-trading-convert/api/rest-api/trade
- Wallet asset endpoints: https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/asset
- Spot account endpoints: https://developers.binance.com/legacy-docs/binance-spot-api-docs/rest-api/account-endpoints

Important endpoint facts reflected in the code:

- Spot signed endpoints require `X-MBX-APIKEY`, `timestamp`, and a `signature`.
- Convert quote requests are signed and use `POST /sapi/v1/convert/getQuote`.
- Convert accept-quote requests are signed and use `POST /sapi/v1/convert/acceptQuote`.
- Spot account balances come from `GET /api/v3/account`.
- Funding wallet balances come from `POST /sapi/v1/asset/get-funding-asset`.
- Public prices are requested from Binance public market-data endpoints.

## Project Structure

- `src/index.ts`
  - MCP server bootstrap
  - Tool registration
  - Output formatting
- `src/binance.ts`
  - Binance HTTP client
  - Query serialization
  - HMAC signing
  - Retries and timeout handling
  - Data normalization helpers
- `package.json`
  - Project metadata
  - Scripts
  - Dependencies
- `tsconfig.json`
  - TypeScript compiler settings
- `.gitignore`
  - Build and local env exclusions

## Environment Variables

Set these before launching the server:

- `BINANCE_API_KEY`
  - Required for all signed endpoints
- `BINANCE_API_SECRET`
  - Required for HMAC signing of signed endpoints
- `BINANCE_BASE_URL`
  - Defaults to `https://api.binance.com`
- `BINANCE_PUBLIC_BASE_URL`
  - Defaults to `https://data-api.binance.vision`
- `BINANCE_STREAM_BASE_URL`
  - Defaults to `wss://stream.binance.com:9443/stream`
- `BINANCE_TIMEOUT_MS`
  - Defaults to `8000`
- `BINANCE_RECV_WINDOW_MS`
  - Defaults to `5000`
- `BINANCE_MAX_RETRIES`
  - Defaults to `1`
- `BINANCE_PRICE_CACHE_TTL_MS`
  - Defaults to `1500`
- `BINANCE_STREAM_ENABLED`
  - Defaults to `true`

## Build

```bash
npm install
npm run build
```

## Run

```bash
npm start
```

The server communicates over stdio, which is the normal MCP transport used by Claude Desktop and other MCP hosts.

## Claude Desktop Configuration

Example configuration:

```json
{
  "mcpServers": {
    "binance-mcp": {
      "command": "node",
      "args": ["C:/Users/hp/Desktop/Binance-mcp/build/index.js"],
      "env": {
        "BINANCE_API_KEY": "your_api_key",
        "BINANCE_API_SECRET": "your_api_secret"
      }
    }
  }
}
```

## Tool Reference

### `get_last_prices`

Returns Binance last-traded prices for one or more symbols.

Input:

- `symbols`

### `get_book_ticker`

Returns the current best bid and ask for one or more symbols.

Input:

- `symbols`

### `get_spot_account`

Returns spot account data from `GET /api/v3/account`.

Input:

- `omitZeroBalances`

### `get_funding_wallet`

Returns funding wallet balances from `POST /sapi/v1/asset/get-funding-asset`.

Input:

- `asset`
- `needBtcValuation`

### `get_account_overview`

Returns a merged view of spot and funding balances.

Input:

- `asset`
- `omitZeroBalances`
- `needBtcValuation`

### `get_portfolio_snapshot`

Returns a merged view of balances plus live pricing and estimated total value.

Input:

- `omitZeroBalances`
- `needBtcValuation`
- `valuationQuoteAsset`

### `get_convert_quote`

Requests a Convert quote without executing it.

Input:

- `fromAsset`
- `toAsset`
- `fromAmount` or `toAmount`
- `walletType`
- `validTime`

### `accept_convert_quote`

Executes a live Convert trade.

Input:

- `quoteId`

### `get_convert_order_status`

Checks the current status of a Convert order.

Input:

- `orderId` or `quoteId`

### `get_convert_trade_history`

Returns Convert trade history for a time window.

Input:

- `startTime`
- `endTime`
- `limit`

## Performance Notes

The server is designed to stay fast and efficient:

- Uses a single reusable HTTP client abstraction
- Keeps request payloads small and deterministic
- Uses one public request for multiple market prices when possible
- Normalizes symbols once before requests are made
- Applies short request timeouts
- Retries only when safe and useful
- Keeps a short-lived in-memory ticker cache for hot symbols
- Maintains a Binance websocket market-data subscription for live ticker updates
- Builds portfolio snapshots with parallel account fetching and live price resolution

## Safety Notes

- `accept_convert_quote` performs a live conversion.
- Binance API secrets are never logged.
- Signed endpoints are blocked until valid credentials are present.
- Convert and account endpoints are rate-limited by Binance, so the client should avoid unnecessary polling.
- Convert trading is REST-only in the current Binance docs; this server does not use a Convert websocket because Binance does not document one.

## Change Log

This repository was created from scratch in this session.

### Added Files

- `package.json`
  - Project metadata, build scripts, runtime entry point, and dependencies.
- `tsconfig.json`
  - Strict TypeScript configuration.
- `.gitignore`
  - Ignores build artifacts and local secrets.
- `src/binance.ts`
  - Binance client, signing, retries, and typed endpoint wrappers.
- `src/index.ts`
  - MCP server definition and tool handlers.
- `DOCUMENTATION.md`
  - Full operational and implementation documentation.

### Endpoint Coverage Implemented

- Public price lookups
- Public book ticker lookups
- Spot account balances
- Funding wallet balances
- Combined account overview
- Convert quote request
- Convert quote acceptance
- Convert order status
- Convert trade history

### Optimization Pass Added

- Websocket spot market data streaming
- Short-lived in-memory price cache
- Parallel REST fallback for missing symbols
- Combined portfolio snapshot tool with live valuation

## Notes for Future Changes

If you add more Binance endpoints later, keep them in `src/binance.ts` as thin typed wrappers and keep `src/index.ts` focused on tool registration and response formatting.
