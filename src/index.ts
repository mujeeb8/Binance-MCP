import "dotenv/config";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  BinanceClient,
  type BinanceConfig,
  type AccountOverview,
  type NormalizedBookTicker,
  type NormalizedPrice,
  type PortfolioSnapshot,
} from "./binance.js";

const envBool = (defaultValue: boolean) =>
  z.preprocess((value) => {
    if (typeof value !== "string") {
      return value;
    }

    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "off"].includes(normalized)) {
      return false;
    }
    return value;
  }, z.boolean().default(defaultValue));

const envSchema = z.object({
  BINANCE_API_KEY: z.string().min(1).optional(),
  BINANCE_API_SECRET: z.string().min(1).optional(),
  BINANCE_BASE_URL: z.string().url().default("https://api.binance.com"),
  BINANCE_PUBLIC_BASE_URL: z.string().url().default("https://data-api.binance.vision"),
  BINANCE_STREAM_BASE_URL: z.string().url().default("wss://stream.binance.com:9443/stream"),
  BINANCE_TIMEOUT_MS: z.coerce.number().int().positive().max(60_000).default(8_000),
  BINANCE_RECV_WINDOW_MS: z.coerce.number().int().positive().max(60_000).default(5_000),
  BINANCE_MAX_RETRIES: z.coerce.number().int().min(0).max(3).default(1),
  BINANCE_PRICE_CACHE_TTL_MS: z.coerce.number().int().positive().max(60_000).default(1_500),
  BINANCE_STREAM_ENABLED: envBool(true),
});

const env = envSchema.parse(process.env);

const clientConfig: BinanceConfig = {
  apiKey: env.BINANCE_API_KEY,
  apiSecret: env.BINANCE_API_SECRET,
  baseUrl: env.BINANCE_BASE_URL,
  publicBaseUrl: env.BINANCE_PUBLIC_BASE_URL,
  streamBaseUrl: env.BINANCE_STREAM_BASE_URL,
  timeoutMs: env.BINANCE_TIMEOUT_MS,
  recvWindowMs: env.BINANCE_RECV_WINDOW_MS,
  maxRetries: env.BINANCE_MAX_RETRIES,
  priceCacheTtlMs: env.BINANCE_PRICE_CACHE_TTL_MS,
  streamEnabled: env.BINANCE_STREAM_ENABLED,
};

const client = new BinanceClient(clientConfig);
const server = new McpServer({
  name: "binance-mcp",
  version: "1.0.0",
});

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function textResponse(title: string, lines: string[], data: unknown): { content: Array<{ type: "text"; text: string }> } {
  const parts = [title, ...lines.filter(Boolean), "", "```json", formatJson(data), "```"];
  return {
    content: [
      {
        type: "text",
        text: parts.join("\n"),
      },
    ],
  };
}

function formatPriceSummary(prices: NormalizedPrice[]): string[] {
  if (prices.length === 0) {
    return ["No matching symbols were returned by Binance."];
  }
  return prices.map((item) => `${item.symbol}: ${item.price}`);
}

function formatBookTickerSummary(tickers: NormalizedBookTicker[]): string[] {
  if (tickers.length === 0) {
    return ["No matching symbols were returned by Binance."];
  }
  return tickers.map((item) => `${item.symbol}: bid ${item.bidPrice} x ${item.bidQty}, ask ${item.askPrice} x ${item.askQty}`);
}

function formatAccountOverviewSummary(overview: AccountOverview): string[] {
  return [
    `Spot balances: ${overview.spotAccount.balances.length}`,
    `Funding wallet balances: ${overview.fundingWallet.length}`,
    `Merged assets: ${overview.mergedBalances.length}`,
  ];
}

function formatPortfolioSnapshotSummary(snapshot: PortfolioSnapshot): string[] {
  const lines = [
    `Estimated total value: ${snapshot.totalUsdtValue.toFixed(8)} USDT`,
    `Holdings: ${snapshot.holdings.length}`,
    `Valuation quote asset: ${snapshot.valuationQuoteAsset}`,
  ];

  if (snapshot.unpricedAssets.length > 0) {
    lines.push(`Unpriced assets: ${snapshot.unpricedAssets.join(", ")}`);
  }

  return lines;
}

server.registerTool(
  "get_last_prices",
  {
    description: "Get the latest Binance last-traded prices for one or more spot symbols.",
    inputSchema: {
      symbols: z.array(z.string().min(1)).min(1).describe("One or more Binance symbols, for example BTCUSDT or ETHUSDT."),
    },
  },
  async ({ symbols }) => {
    const prices = await client.getMarketPrices(symbols);
    return textResponse("Binance last prices", formatPriceSummary(prices), prices);
  },
);

server.registerTool(
  "get_book_ticker",
  {
    description: "Get the current best bid and ask for one or more Binance spot symbols.",
    inputSchema: {
      symbols: z.array(z.string().min(1)).min(1).describe("One or more Binance symbols, for example BTCUSDT or ETHUSDT."),
    },
  },
  async ({ symbols }) => {
    const tickers = await client.getBookTickers(symbols);
    return textResponse("Binance best bid and ask", formatBookTickerSummary(tickers), tickers);
  },
);

server.registerTool(
  "get_spot_account",
  {
    description: "Get the current Binance spot account, including balances and account flags.",
    inputSchema: {
      omitZeroBalances: z.boolean().optional().describe("When true, only non-zero balances are returned."),
    },
  },
  async ({ omitZeroBalances }) => {
    const account = await client.getSpotAccount(omitZeroBalances ?? false);
    return textResponse(
      "Binance spot account",
      [`Balances returned: ${account.balances.length}`, `Account type: ${account.accountType}`],
      account,
    );
  },
);

server.registerTool(
  "get_funding_wallet",
  {
    description: "Get the Binance funding wallet balances.",
    inputSchema: {
      asset: z.string().min(1).optional().describe("Optional asset filter such as BTC or USDT."),
      needBtcValuation: z.boolean().optional().describe("When true, includes BTC valuation where available."),
    },
  },
  async ({ asset, needBtcValuation }) => {
    const fundingWallet = await client.getFundingWallet(asset, needBtcValuation ?? true);
    return textResponse(
      "Binance funding wallet",
      [`Balances returned: ${fundingWallet.length}`],
      fundingWallet,
    );
  },
);

server.registerTool(
  "get_account_overview",
  {
    description: "Get a combined overview of spot account balances and funding wallet balances.",
    inputSchema: {
      asset: z.string().min(1).optional().describe("Optional asset filter such as BTC or USDT."),
      omitZeroBalances: z.boolean().optional().describe("When true, the spot account returns only non-zero balances."),
      needBtcValuation: z.boolean().optional().describe("When true, the funding wallet includes BTC valuation where available."),
    },
  },
  async ({ asset, omitZeroBalances, needBtcValuation }) => {
    const overview = await client.getAccountOverview({
      asset,
      omitZeroBalances,
      needBtcValuation,
    });
    return textResponse("Binance account overview", formatAccountOverviewSummary(overview), overview);
  },
);

server.registerTool(
  "get_portfolio_snapshot",
  {
    description: "Get balances and a live valuation snapshot in one call.",
    inputSchema: {
      omitZeroBalances: z.boolean().optional().describe("When true, only non-zero spot balances are included."),
      needBtcValuation: z.boolean().optional().describe("When true, funding balances include BTC valuation where available."),
      valuationQuoteAsset: z.string().min(1).optional().describe("Quote asset used for valuation, usually USDT."),
    },
  },
  async ({ omitZeroBalances, needBtcValuation, valuationQuoteAsset }) => {
    const snapshot = await client.getPortfolioSnapshot({
      omitZeroBalances,
      needBtcValuation,
      valuationQuoteAsset,
    });
    return textResponse("Binance portfolio snapshot", formatPortfolioSnapshotSummary(snapshot), snapshot);
  },
);

server.registerTool(
  "get_convert_quote",
  {
    description: "Request a Binance Convert quote. This does not execute the conversion.",
    inputSchema: {
      fromAsset: z.string().min(1).describe("Source asset, for example BTC."),
      toAsset: z.string().min(1).describe("Destination asset, for example USDT."),
      fromAmount: z.string().min(1).optional().describe("Exact source amount as a decimal string."),
      toAmount: z.string().min(1).optional().describe("Exact destination amount as a decimal string."),
      walletType: z.enum(["SPOT", "FUNDING", "EARN", "SPOT_FUNDING", "FUNDING_EARN", "SPOT_FUNDING_EARN", "SPOT_EARN"]).optional().describe("Wallet source used by Convert."),
      validTime: z.enum(["10s", "30s", "1m"]).optional().describe("How long the quote stays valid."),
    },
  },
  async ({ fromAsset, toAsset, fromAmount, toAmount, walletType, validTime }) => {
    const quote = await client.getConvertQuote({
      fromAsset,
      toAsset,
      fromAmount,
      toAmount,
      walletType,
      validTime,
    });
    return textResponse("Binance Convert quote", [`Quote ID: ${quote.quoteId}`, `Valid until: ${quote.validTimestamp}`], quote);
  },
);

server.registerTool(
  "accept_convert_quote",
  {
    description: "Execute a Binance Convert quote. This performs a live conversion.",
    inputSchema: {
      quoteId: z.string().min(1).describe("The quote ID returned by get_convert_quote."),
    },
  },
  async ({ quoteId }) => {
    const order = await client.acceptConvertQuote({ quoteId });
    return textResponse("Binance Convert accept quote", [`Order ID: ${order.orderId}`, `Status: ${order.orderStatus}`], order);
  },
);

server.registerTool(
  "get_convert_order_status",
  {
    description: "Check the status of a Binance Convert order by orderId or quoteId.",
    inputSchema: {
      orderId: z.string().min(1).optional().describe("Convert order ID."),
      quoteId: z.string().min(1).optional().describe("Convert quote ID."),
    },
  },
  async ({ orderId, quoteId }) => {
    const status = await client.getConvertOrderStatus({ orderId, quoteId });
    return textResponse("Binance Convert order status", [`Order status: ${status.orderStatus}`], status);
  },
);

server.registerTool(
  "get_convert_trade_history",
  {
    description: "Get Binance Convert trade history for a 30-day window or less.",
    inputSchema: {
      startTime: z.number().int().nonnegative().describe("Start of the query window in milliseconds."),
      endTime: z.number().int().nonnegative().describe("End of the query window in milliseconds."),
      limit: z.number().int().positive().max(1000).optional().describe("Maximum number of records to return."),
    },
  },
  async ({ startTime, endTime, limit }) => {
    const history = await client.getConvertTradeHistory({ startTime, endTime, limit });
    return textResponse(
      "Binance Convert trade history",
      [`Rows returned: ${history.list.length}`],
      history,
    );
  },
);

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Binance MCP server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error starting Binance MCP server:", error);
  process.exit(1);
});
