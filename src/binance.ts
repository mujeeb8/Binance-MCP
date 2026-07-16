import crypto from "node:crypto";
import WebSocket from "ws";

export type HttpMethod = "GET" | "POST";

export interface BinanceConfig {
  apiKey?: string;
  apiSecret?: string;
  baseUrl: string;
  publicBaseUrl: string;
  streamBaseUrl: string;
  timeoutMs: number;
  recvWindowMs: number;
  maxRetries: number;
  priceCacheTtlMs: number;
  streamEnabled: boolean;
}

export interface SignedRequestOptions {
  method?: HttpMethod;
  path: string;
  params?: Record<string, string | number | boolean | undefined | null>;
  signed?: boolean;
  retryable?: boolean;
  baseUrl?: string;
}

export interface BinanceErrorPayload {
  code?: number;
  msg?: string;
}

export class BinanceApiError extends Error {
  public readonly status: number;
  public readonly payload?: unknown;
  public readonly retryAfter?: number;

  constructor(status: number, message: string, payload?: unknown, retryAfter?: number) {
    super(message);
    this.name = "BinanceApiError";
    this.status = status;
    this.payload = payload;
    this.retryAfter = retryAfter;
  }
}

type QueryValue = string | number | boolean;
type QueryEntry = readonly [string, QueryValue];

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function encode(value: QueryValue): string {
  return encodeURIComponent(String(value));
}

function buildQuery(entries: readonly QueryEntry[]): string {
  return entries
    .filter(([, value]) => value !== undefined && value !== null)
    .map(([key, value]) => `${encodeURIComponent(key)}=${encode(value)}`)
    .join("&");
}

function normalizeMaybeArray<T>(value: T | T[]): T[] {
  return Array.isArray(value) ? value : [value];
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface CachedTickerEntry {
  symbol: string;
  price?: string;
  bidPrice?: string;
  bidQty?: string;
  askPrice?: string;
  askQty?: string;
  updatedAt: number;
  source: "websocket" | "rest";
}

class MarketDataCache {
  private readonly enabled: boolean;
  private readonly streamBaseUrl: string;
  private readonly ttlMs: number;
  private ws: WebSocket | null = null;
  private connectPromise: Promise<void> | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectAttempts = 0;
  private nextRequestId = 1;
  private readonly subscribedStreams = new Set<string>();
  private readonly cache = new Map<string, CachedTickerEntry>();

  constructor(options: { enabled: boolean; streamBaseUrl: string; ttlMs: number }) {
    this.enabled = options.enabled;
    this.streamBaseUrl = options.streamBaseUrl;
    this.ttlMs = options.ttlMs;
  }

  public ensureSymbols(symbols: string[]): void {
    if (!this.enabled || symbols.length === 0) {
      return;
    }

    const streams = this.toStreams(symbols);
    let hasNewStream = false;
    for (const stream of streams) {
      if (!this.subscribedStreams.has(stream)) {
        this.subscribedStreams.add(stream);
        hasNewStream = true;
      }
    }

    if (!hasNewStream && this.ws?.readyState === WebSocket.OPEN) {
      return;
    }

    void this.connect().catch(() => undefined);
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.sendSubscribe(streams);
    }
  }

  public async resolvePrices(
    symbols: string[],
    fetchMissing: (symbols: string[]) => Promise<PriceTicker[]>,
  ): Promise<NormalizedPrice[]> {
    const normalized = this.normalizeSymbols(symbols);
    if (normalized.length === 0) {
      throw new Error("At least one symbol is required.");
    }

    this.ensureSymbols(normalized);

    const missing = normalized.filter((symbol) => !this.hasFreshPrice(symbol));
    if (missing.length > 0) {
      const fetched = await fetchMissing(missing);
      for (const item of fetched) {
        this.upsertPriceFromRest(item);
      }
    }

    return normalized.map((symbol) => {
      const entry = this.cache.get(symbol);
      if (!entry?.price) {
        throw new Error(`No live price is available for ${symbol}.`);
      }
      return { symbol, price: entry.price };
    });
  }

  public async resolveBookTickers(
    symbols: string[],
    fetchMissing: (symbols: string[]) => Promise<BookTicker[]>,
  ): Promise<NormalizedBookTicker[]> {
    const normalized = this.normalizeSymbols(symbols);
    if (normalized.length === 0) {
      throw new Error("At least one symbol is required.");
    }

    this.ensureSymbols(normalized);

    const missing = normalized.filter((symbol) => !this.hasFreshBookTicker(symbol));
    if (missing.length > 0) {
      const fetched = await fetchMissing(missing);
      for (const item of fetched) {
        this.upsertBookTickerFromRest(item);
      }
    }

    return normalized.map((symbol) => {
      const entry = this.cache.get(symbol);
      if (!entry?.bidPrice || !entry.bidQty || !entry.askPrice || !entry.askQty) {
        throw new Error(`No live bid/ask is available for ${symbol}.`);
      }
      return {
        symbol,
        bidPrice: entry.bidPrice,
        bidQty: entry.bidQty,
        askPrice: entry.askPrice,
        askQty: entry.askQty,
      };
    });
  }

  public async primeSymbols(symbols: string[]): Promise<void> {
    if (!this.enabled || symbols.length === 0) {
      return;
    }

    this.ensureSymbols(symbols);
    await this.connect();
  }

  private async connect(): Promise<void> {
    if (!this.enabled) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return this.connectPromise ?? Promise.resolve();
    }

    if (this.connectPromise) {
      return this.connectPromise;
    }

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.connectPromise = new Promise<void>((resolve) => {
      const socket = new WebSocket(this.streamBaseUrl);
      this.ws = socket;
      let settled = false;
      const settle = (): void => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };

      socket.on("open", () => {
        this.reconnectAttempts = 0;
        const underlyingSocket = (socket as WebSocket & { _socket?: { unref?: () => void } })._socket;
        underlyingSocket?.unref?.();
        const streams = [...this.subscribedStreams];
        if (streams.length > 0) {
          this.sendSubscribe(streams);
        }
        settle();
      });

      socket.on("message", (data) => {
        void this.onMessage(data.toString());
      });

      socket.on("close", () => {
        if (this.ws === socket) {
          this.ws = null;
        }
        this.scheduleReconnect();
        settle();
      });

      socket.on("error", () => {
        if (this.ws === socket) {
          this.ws = null;
        }
        this.scheduleReconnect();
        settle();
      });
    }).finally(() => {
      this.connectPromise = null;
    });

    return this.connectPromise;
  }

  private scheduleReconnect(): void {
    if (!this.enabled || this.subscribedStreams.size === 0) {
      return;
    }

    if (this.reconnectTimer) {
      return;
    }

    const delayMs = Math.min(1_000 * 2 ** this.reconnectAttempts, 30_000);
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect().catch(() => undefined);
    }, delayMs);
  }

  private sendSubscribe(streams: string[]): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || streams.length === 0) {
      return;
    }

    const batchSize = 100;
    for (let index = 0; index < streams.length; index += batchSize) {
      const params = streams.slice(index, index + batchSize);
      this.ws.send(JSON.stringify({ method: "SUBSCRIBE", params, id: this.nextRequestId++ }));
    }
  }

  private async onMessage(payload: string): Promise<void> {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      return;
    }

    const data = this.unwrapStreamPayload(parsed);
    if (!isPlainObject(data) || typeof data.s !== "string") {
      return;
    }

    const symbol = data.s.toUpperCase();
    const now = Date.now();
    const entry = this.cache.get(symbol) ?? { symbol, updatedAt: now, source: "websocket" as const };

    if (typeof data.c === "string") {
      entry.price = data.c;
    }
    if (typeof data.b === "string") {
      entry.bidPrice = data.b;
    }
    if (typeof data.B === "string") {
      entry.bidQty = data.B;
    }
    if (typeof data.a === "string") {
      entry.askPrice = data.a;
    }
    if (typeof data.A === "string") {
      entry.askQty = data.A;
    }
    entry.updatedAt = now;
    entry.source = "websocket";
    this.cache.set(symbol, entry);
  }

  private unwrapStreamPayload(value: unknown): unknown {
    if (!isPlainObject(value)) {
      return value;
    }

    if ("data" in value && isPlainObject(value.data)) {
      return value.data;
    }

    return value;
  }

  private upsertPriceFromRest(item: PriceTicker): void {
    const symbol = item.symbol.toUpperCase();
    const existing = this.cache.get(symbol);
    this.cache.set(symbol, {
      symbol,
      price: item.price,
      bidPrice: existing?.bidPrice,
      bidQty: existing?.bidQty,
      askPrice: existing?.askPrice,
      askQty: existing?.askQty,
      updatedAt: Date.now(),
      source: "rest",
    });
  }

  private upsertBookTickerFromRest(item: BookTicker): void {
    const symbol = item.symbol.toUpperCase();
    const existing = this.cache.get(symbol);
    this.cache.set(symbol, {
      symbol,
      price: existing?.price,
      bidPrice: item.bidPrice,
      bidQty: item.bidQty,
      askPrice: item.askPrice,
      askQty: item.askQty,
      updatedAt: Date.now(),
      source: "rest",
    });
  }

  private hasFreshPrice(symbol: string): boolean {
    const entry = this.cache.get(symbol);
    return Boolean(entry?.price && Date.now() - entry.updatedAt <= this.ttlMs);
  }

  private hasFreshBookTicker(symbol: string): boolean {
    const entry = this.cache.get(symbol);
    return Boolean(
      entry?.bidPrice &&
        entry?.bidQty &&
        entry?.askPrice &&
        entry?.askQty &&
        Date.now() - entry.updatedAt <= this.ttlMs,
    );
  }

  private normalizeSymbols(symbols: string[]): string[] {
    return [...new Set(symbols.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean))];
  }

  private toStreams(symbols: string[]): string[] {
    return this.normalizeSymbols(symbols).map((symbol) => `${symbol.toLowerCase()}@ticker`);
  }
}

export class BinanceClient {
  private readonly config: BinanceConfig;
  private readonly marketData: MarketDataCache;

  constructor(config: BinanceConfig) {
    this.config = config;
    this.marketData = new MarketDataCache({
      enabled: config.streamEnabled,
      streamBaseUrl: config.streamBaseUrl,
      ttlMs: config.priceCacheTtlMs,
    });
  }

  public async getJson<T>(options: SignedRequestOptions): Promise<T> {
    const method = options.method ?? "GET";
    const baseUrl = options.baseUrl ?? this.config.baseUrl;
    const retryable = options.retryable ?? (method === "GET");
    const params = options.params ?? {};
    const entries = Object.entries(params).filter(([, value]) => value !== undefined && value !== null) as QueryEntry[];

    if (options.signed) {
      this.ensureAuth();
    }

    let payload = buildQuery(entries);
    if (options.signed) {
      const signedEntries: QueryEntry[] = [
        ...entries,
        ["timestamp", Date.now()],
        ["recvWindow", this.config.recvWindowMs],
      ];
      payload = buildQuery(signedEntries);
      const signature = crypto.createHmac("sha256", this.config.apiSecret as string).update(payload).digest("hex");
      payload = `${payload}&signature=${signature}`;
    }

    const url = new URL(options.path, baseUrl);
    const requestInit: RequestInit = {
      method,
      headers: {
        Accept: "application/json",
        ...(options.signed ? { "X-MBX-APIKEY": this.config.apiKey as string } : {}),
        ...(method === "POST" ? { "Content-Type": "application/x-www-form-urlencoded" } : {}),
      },
    };

    if (method === "GET") {
      if (payload) {
        url.search = payload;
      }
    } else if (payload) {
      requestInit.body = payload;
    }

    let lastError: unknown;
    for (let attempt = 0; attempt <= this.config.maxRetries; attempt += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), this.config.timeoutMs);
      requestInit.signal = controller.signal;

      try {
        const response = await fetch(url, requestInit);
        const text = await response.text();
        const data: unknown = text.length > 0 ? this.safeJsonParse<unknown>(text) : ({} as T);

        if (!response.ok) {
          const retryAfter = this.parseRetryAfter(response.headers.get("retry-after"));
          const message = this.buildErrorMessage(response.status, data);
          throw new BinanceApiError(response.status, message, data, retryAfter);
        }

        return data as T;
      } catch (error) {
        lastError = error;
        const shouldRetry = retryable && attempt < this.config.maxRetries && this.isRetryableError(error);
        if (!shouldRetry) {
          throw this.normalizeError(error);
        }

        const retryDelay = this.retryDelayForError(error, attempt);
        await sleep(retryDelay);
      } finally {
        clearTimeout(timeout);
      }
    }

    throw this.normalizeError(lastError);
  }

  public async getMarketPrices(symbols: string[]): Promise<NormalizedPrice[]> {
    const normalizedSymbols = this.normalizeSymbols(symbols);
    return this.marketData.resolvePrices(normalizedSymbols, (missing) => this.fetchPricesFromRest(missing));
  }

  public async getBookTickers(symbols: string[]): Promise<NormalizedBookTicker[]> {
    const normalizedSymbols = this.normalizeSymbols(symbols);
    return this.marketData.resolveBookTickers(normalizedSymbols, (missing) => this.fetchBookTickersFromRest(missing));
  }

  public async getSpotAccount(omitZeroBalances = false): Promise<SpotAccountResponse> {
    return this.getJson<SpotAccountResponse>({
      method: "GET",
      path: "/api/v3/account",
      params: { omitZeroBalances },
      signed: true,
      retryable: true,
    });
  }

  public async getFundingWallet(asset?: string, needBtcValuation = true): Promise<FundingBalance[]> {
    return this.getJson<FundingBalance[]>({
      method: "POST",
      path: "/sapi/v1/asset/get-funding-asset",
      params: {
        asset: asset ? asset.toUpperCase() : undefined,
        needBtcValuation,
      },
      signed: true,
      retryable: true,
    });
  }

  public async getAccountOverview(options: {
    asset?: string;
    omitZeroBalances?: boolean;
    needBtcValuation?: boolean;
  } = {}): Promise<AccountOverview> {
    const [spotAccount, fundingWallet] = await Promise.all([
      this.getSpotAccount(options.omitZeroBalances ?? false),
      this.getFundingWallet(options.asset, options.needBtcValuation ?? true),
    ]);

    const spotBalances = new Map<string, SpotAccountBalance>(
      spotAccount.balances.map((balance) => [balance.asset, balance] as const),
    );
    const fundingBalances = new Map<string, FundingBalance>(
      fundingWallet.map((balance) => [balance.asset, balance] as const),
    );
    const allAssets = new Set([...spotBalances.keys(), ...fundingBalances.keys()]);

    const mergedBalances = [...allAssets].sort().map((asset) => ({
      asset,
      spot: spotBalances.get(asset) ?? null,
      funding: fundingBalances.get(asset) ?? null,
    }));

    return {
      spotAccount,
      fundingWallet,
      mergedBalances,
    };
  }

  public async getPortfolioSnapshot(options: {
    omitZeroBalances?: boolean;
    needBtcValuation?: boolean;
    valuationQuoteAsset?: string;
  } = {}): Promise<PortfolioSnapshot> {
    const overview = await this.getAccountOverview({
      omitZeroBalances: options.omitZeroBalances,
      needBtcValuation: options.needBtcValuation ?? true,
    });

    const valuationQuoteAsset = (options.valuationQuoteAsset ?? "USDT").toUpperCase();
    const holdings = this.buildHoldings(overview.spotAccount.balances, overview.fundingWallet);
    const assetSymbols = holdings
      .map((holding) => holding.asset)
      .filter((asset) => !this.isStableAsset(asset))
      .map((asset) => `${asset}${valuationQuoteAsset}`);

    const uniqueSymbols = [...new Set(assetSymbols)];
    if (uniqueSymbols.length > 0) {
      this.marketData.ensureSymbols(uniqueSymbols);
    }

    const prices = uniqueSymbols.length > 0 ? await this.getMarketPrices(uniqueSymbols) : [];
    const priceMap = new Map(prices.map((item) => [item.symbol, Number(item.price)] as const));

    const pricedHoldings = holdings.map((holding) => {
      const priceSymbol = this.isStableAsset(holding.asset) ? undefined : `${holding.asset}${valuationQuoteAsset}`;
      const unitPrice = priceSymbol ? priceMap.get(priceSymbol) : 1;
      const estimatedUsdtValue = unitPrice !== undefined ? holding.totalAmount * unitPrice : undefined;
      return {
        ...holding,
        priceSymbol,
        unitPrice,
        estimatedUsdtValue,
      };
    });

    const totalUsdtValue = pricedHoldings.reduce((sum, holding) => sum + (holding.estimatedUsdtValue ?? 0), 0);
    const unpricedAssets = pricedHoldings.filter((holding) => holding.estimatedUsdtValue === undefined).map((holding) => holding.asset);

    return {
      ...overview,
      valuationQuoteAsset,
      holdings: pricedHoldings,
      livePrices: prices,
      totalUsdtValue,
      unpricedAssets,
    };
  }

  public async getConvertQuote(options: {
    fromAsset: string;
    toAsset: string;
    fromAmount?: string;
    toAmount?: string;
    walletType?: WalletType;
    validTime?: ConvertValidTime;
  }): Promise<ConvertQuoteResponse> {
    this.ensureAuth();
    this.assertExactlyOneAmount(options.fromAmount, options.toAmount);

    return this.getJson<ConvertQuoteResponse>({
      method: "POST",
      path: "/sapi/v1/convert/getQuote",
      params: {
        fromAsset: options.fromAsset.toUpperCase(),
        toAsset: options.toAsset.toUpperCase(),
        fromAmount: options.fromAmount,
        toAmount: options.toAmount,
        walletType: options.walletType ?? "SPOT",
        validTime: options.validTime ?? "10s",
      },
      signed: true,
      retryable: true,
    });
  }

  public async acceptConvertQuote(options: { quoteId: string }): Promise<AcceptConvertQuoteResponse> {
    this.ensureAuth();
    return this.getJson<AcceptConvertQuoteResponse>({
      method: "POST",
      path: "/sapi/v1/convert/acceptQuote",
      params: { quoteId: options.quoteId },
      signed: true,
      retryable: false,
    });
  }

  public async getConvertOrderStatus(options: { orderId?: string; quoteId?: string }): Promise<ConvertOrderStatusResponse> {
    this.ensureAuth();
    if (!options.orderId && !options.quoteId) {
      throw new Error("Either orderId or quoteId is required.");
    }

    return this.getJson<ConvertOrderStatusResponse>({
      method: "GET",
      path: "/sapi/v1/convert/orderStatus",
      params: {
        orderId: options.orderId,
        quoteId: options.quoteId,
      },
      signed: true,
      retryable: true,
    });
  }

  public async getConvertTradeHistory(options: {
    startTime: number;
    endTime: number;
    limit?: number;
  }): Promise<ConvertTradeHistoryResponse> {
    this.ensureAuth();
    return this.getJson<ConvertTradeHistoryResponse>({
      method: "GET",
      path: "/sapi/v1/convert/tradeFlow",
      params: {
        startTime: options.startTime,
        endTime: options.endTime,
        limit: options.limit ?? 100,
      },
      signed: true,
      retryable: true,
    });
  }

  private async fetchPricesFromRest(symbols: string[]): Promise<PriceTicker[]> {
    const normalizedSymbols = this.normalizeSymbols(symbols);
    if (normalizedSymbols.length === 0) {
      return [];
    }

    if (normalizedSymbols.length <= 3) {
      const responses = await Promise.all(
        normalizedSymbols.map((symbol) =>
          this.getJson<PriceTicker>({
            method: "GET",
            path: "/api/v3/ticker/price",
            params: { symbol },
            retryable: true,
            baseUrl: this.config.publicBaseUrl,
          }),
        ),
      );
      return responses;
    }

    const response = await this.getJson<PriceTicker[]>({
      method: "GET",
      path: "/api/v3/ticker/price",
      retryable: true,
      baseUrl: this.config.publicBaseUrl,
    });
    const wanted = new Set(normalizedSymbols);
    return response.filter((item) => wanted.has(item.symbol));
  }

  private async fetchBookTickersFromRest(symbols: string[]): Promise<BookTicker[]> {
    const normalizedSymbols = this.normalizeSymbols(symbols);
    if (normalizedSymbols.length === 0) {
      return [];
    }

    if (normalizedSymbols.length <= 3) {
      const responses = await Promise.all(
        normalizedSymbols.map((symbol) =>
          this.getJson<BookTicker>({
            method: "GET",
            path: "/api/v3/ticker/bookTicker",
            params: { symbol },
            retryable: true,
            baseUrl: this.config.publicBaseUrl,
          }),
        ),
      );
      return responses;
    }

    const response = await this.getJson<BookTicker[]>({
      method: "GET",
      path: "/api/v3/ticker/bookTicker",
      retryable: true,
      baseUrl: this.config.publicBaseUrl,
    });
    const wanted = new Set(normalizedSymbols);
    return response.filter((item) => wanted.has(item.symbol));
  }

  private buildHoldings(
    spotBalances: SpotAccountBalance[],
    fundingWallet: FundingBalance[],
  ): PortfolioHolding[] {
    const holdings = new Map<string, PortfolioHolding>();

    for (const balance of spotBalances) {
      holdings.set(balance.asset, {
        asset: balance.asset,
        spotFree: balance.free,
        spotLocked: balance.locked,
        fundingFree: "0",
        fundingLocked: "0",
        fundingFreeze: "0",
        fundingWithdrawing: "0",
        totalAmount: Number(balance.free) + Number(balance.locked),
      });
    }

    for (const balance of fundingWallet) {
      const existing = holdings.get(balance.asset) ?? {
        asset: balance.asset,
        spotFree: "0",
        spotLocked: "0",
        fundingFree: "0",
        fundingLocked: "0",
        fundingFreeze: "0",
        fundingWithdrawing: "0",
        totalAmount: 0,
      };

      existing.fundingFree = balance.free;
      existing.fundingLocked = balance.locked;
      existing.fundingFreeze = balance.freeze;
      existing.fundingWithdrawing = balance.withdrawing;
      existing.totalAmount += Number(balance.free) + Number(balance.locked) + Number(balance.freeze) + Number(balance.withdrawing);
      holdings.set(balance.asset, existing);
    }

    return [...holdings.values()]
      .filter((item) => item.totalAmount > 0)
      .sort((left, right) => left.asset.localeCompare(right.asset));
  }

  private isStableAsset(asset: string): boolean {
    return STABLE_ASSETS.has(asset.toUpperCase());
  }

  private ensureAuth(): void {
    if (!this.config.apiKey || !this.config.apiSecret) {
      throw new Error("BINANCE_API_KEY and BINANCE_API_SECRET are required for signed Binance endpoints.");
    }
  }

  private buildErrorMessage(status: number, data: unknown): string {
    if (typeof data === "string") {
      return `Binance request failed with HTTP ${status}: ${data}`;
    }
    if (isPlainObject(data) && typeof data.msg === "string") {
      return `Binance request failed with HTTP ${status}: ${data.msg}${typeof data.code === "number" ? ` (code ${data.code})` : ""}`;
    }
    return `Binance request failed with HTTP ${status}`;
  }

  private parseRetryAfter(value: string | null): number | undefined {
    if (!value) {
      return undefined;
    }
    const seconds = Number(value);
    return Number.isFinite(seconds) ? seconds : undefined;
  }

  private safeJsonParse<T>(value: string): T | string {
    try {
      return JSON.parse(value) as T;
    } catch {
      return value;
    }
  }

  private isRetryableError(error: unknown): boolean {
    if (error instanceof BinanceApiError) {
      return error.status >= 500;
    }
    if (error instanceof Error) {
      return error.name === "AbortError" || error.name === "TimeoutError";
    }
    return false;
  }

  private retryDelayForError(error: unknown, attempt: number): number {
    if (error instanceof BinanceApiError && error.retryAfter !== undefined) {
      return Math.min(error.retryAfter * 1000, 10_000);
    }
    const base = 250 * 2 ** attempt;
    return Math.min(base, 2_000);
  }

  private normalizeError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }
    return new Error(String(error));
  }

  private normalizeSymbols(symbols: string[]): string[] {
    return [...new Set(symbols.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean))];
  }

  private toNormalizedPrice(item: PriceTicker): NormalizedPrice {
    return {
      symbol: item.symbol,
      price: item.price,
    };
  }

  private toNormalizedBookTicker(item: BookTicker): NormalizedBookTicker {
    return {
      symbol: item.symbol,
      bidPrice: item.bidPrice,
      bidQty: item.bidQty,
      askPrice: item.askPrice,
      askQty: item.askQty,
    };
  }

  private assertExactlyOneAmount(fromAmount?: string, toAmount?: string): void {
    const hasFrom = Boolean(fromAmount);
    const hasTo = Boolean(toAmount);
    if (hasFrom === hasTo) {
      throw new Error("Provide exactly one of fromAmount or toAmount.");
    }
  }
}

export interface PriceTicker {
  symbol: string;
  price: string;
}

export interface NormalizedPrice {
  symbol: string;
  price: string;
}

export interface BookTicker {
  symbol: string;
  bidPrice: string;
  bidQty: string;
  askPrice: string;
  askQty: string;
}

export interface NormalizedBookTicker {
  symbol: string;
  bidPrice: string;
  bidQty: string;
  askPrice: string;
  askQty: string;
}

export interface SpotAccountBalance {
  asset: string;
  free: string;
  locked: string;
}

export interface SpotAccountResponse {
  makerCommission: number;
  takerCommission: number;
  buyerCommission: number;
  sellerCommission: number;
  commissionRates?: {
    maker: string;
    taker: string;
    buyer: string;
    seller: string;
  };
  canTrade: boolean;
  canWithdraw: boolean;
  canDeposit: boolean;
  brokered?: boolean;
  requireSelfTradePrevention?: boolean;
  preventSor?: boolean;
  updateTime: number;
  accountType: string;
  balances: SpotAccountBalance[];
  permissions: string[];
  uid?: number;
}

export interface FundingBalance {
  asset: string;
  free: string;
  locked: string;
  freeze: string;
  withdrawing: string;
  btcValuation?: string;
}

export interface AccountOverview {
  spotAccount: SpotAccountResponse;
  fundingWallet: FundingBalance[];
  mergedBalances: Array<{
    asset: string;
    spot: SpotAccountBalance | null;
    funding: FundingBalance | null;
  }>;
}

export type WalletType =
  | "SPOT"
  | "FUNDING"
  | "EARN"
  | "SPOT_FUNDING"
  | "FUNDING_EARN"
  | "SPOT_FUNDING_EARN"
  | "SPOT_EARN";

export type ConvertValidTime = "10s" | "30s" | "1m";

export interface ConvertQuoteResponse {
  quoteId: string;
  ratio: string;
  inverseRatio: string;
  validTimestamp: number;
  fromAmount?: string;
  toAmount?: string;
}

export interface AcceptConvertQuoteResponse {
  orderId: string;
  createTime: number;
  orderStatus: "PROCESS" | "ACCEPT_SUCCESS" | "SUCCESS" | "FAIL";
}

export interface ConvertOrderStatusResponse {
  orderId: string;
  orderStatus: string;
  fromAsset: string;
  fromAmount: string;
  toAsset: string;
  toAmount: string;
  ratio: string;
  inverseRatio: string;
  createTime: number;
}

export interface ConvertTradeHistoryResponse {
  list: Array<{
    quoteId: string;
    orderId: number;
    orderStatus: string;
    fromAsset: string;
    fromAmount: string;
    toAsset: string;
    toAmount: string;
    ratio: string;
    inverseRatio: string;
    createTime: number;
    startTime: number;
    endTime: number;
    limit: number;
    moreData: boolean;
  }>;
}

export interface PortfolioHolding {
  asset: string;
  spotFree: string;
  spotLocked: string;
  fundingFree: string;
  fundingLocked: string;
  fundingFreeze: string;
  fundingWithdrawing: string;
  totalAmount: number;
  priceSymbol?: string;
  unitPrice?: number;
  estimatedUsdtValue?: number;
}

export interface PortfolioSnapshot {
  spotAccount: SpotAccountResponse;
  fundingWallet: FundingBalance[];
  mergedBalances: AccountOverview["mergedBalances"];
  valuationQuoteAsset: string;
  holdings: PortfolioHolding[];
  livePrices: NormalizedPrice[];
  totalUsdtValue: number;
  unpricedAssets: string[];
}

const STABLE_ASSETS = new Set(["USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "USDP", "EUR", "TRY", "BRL", "GBP"]);
