import type * as NodeOs from "node:os";
import type * as NodeZlib from "node:zlib";
import type {
	Tool as OpenAITool,
	ResponseCreateParamsStreaming,
	ResponseInput,
	ResponseStreamEvent,
} from "openai/resources/responses/responses.js";

type ProcessWithOsBuiltinModule = typeof process & {
	getBuiltinModule?: (id: "node:os") => typeof NodeOs;
};

function loadNodeOs(): typeof NodeOs | null {
	if (typeof process === "undefined" || !(process.versions?.node || process.versions?.bun)) {
		return null;
	}
	return (process as ProcessWithOsBuiltinModule).getBuiltinModule?.("node:os") ?? null;
}

// NEVER convert to top-level runtime imports - breaks browser/Vite builds
const _os: typeof NodeOs | null = loadNodeOs();

import { clampThinkingLevel } from "../models.ts";
import { registerSessionResourceCleanup } from "../session-resources.ts";
import type {
	Api,
	AssistantMessage,
	Context,
	Model,
	ProviderEnv,
	ProviderHeaders,
	SimpleStreamOptions,
	StreamFunction,
	StreamOptions,
	Usage,
} from "../types.ts";
import { combineAbortSignals } from "../utils/abort-signals.ts";
import { splitDeferredTools } from "../utils/deferred-tools.ts";
import {
	appendAssistantMessageDiagnostic,
	createAssistantMessageDiagnostic,
	formatThrownValue,
} from "../utils/diagnostics.ts";
import { formatProviderError, normalizeProviderError } from "../utils/error-body.ts";
import { AssistantMessageEventStream } from "../utils/event-stream.ts";
import { headersToRecord } from "../utils/headers.ts";
import { resolveHttpProxyUrlForTarget } from "../utils/node-http-proxy.ts";
import { uuidv7 } from "../utils/uuid.ts";
import { createGrammarToolInputProperties } from "./constrained-sampling.ts";
import { clampOpenAIPromptCacheKey } from "./openai-prompt-cache.ts";
import { convertResponsesMessages, convertResponsesTools, processResponsesStream } from "./openai-responses-shared.ts";
import { buildBaseOptions } from "./simple-options.ts";

// ============================================================================
// Configuration
// ============================================================================

const DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api";
const JWT_CLAIM_PATH = "https://api.openai.com/auth" as const;
const DEFAULT_MAX_RETRIES = 0;
const BASE_DELAY_MS = 1000;
const DEFAULT_MAX_RETRY_DELAY_MS = 60_000;
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS = 15_000;
// The Codex backend accepts zstd-compressed request bodies on the SSE responses
// endpoint (the same endpoint the official Codex client compresses against).
const REQUEST_COMPRESSION_ZSTD_LEVEL = 3;
const CODEX_TOOL_CALL_PROVIDERS = new Set(["openai", "openai-codex", "opencode"]);
const WEBSOCKET_MESSAGE_TOO_BIG_CLOSE_CODE = 1009;
const WEBSOCKET_CONNECTION_LIMIT_REACHED_CODE = "websocket_connection_limit_reached";
const PREVIOUS_RESPONSE_NOT_FOUND_CODE = "previous_response_not_found";

const CODEX_RESPONSE_STATUSES = new Set<CodexResponseStatus>([
	"completed",
	"incomplete",
	"failed",
	"cancelled",
	"queued",
	"in_progress",
]);

// ============================================================================
// Types
// ============================================================================

export interface OpenAICodexResponsesOptions extends StreamOptions {
	reasoningEffort?: "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
	reasoningSummary?: "auto" | "concise" | "detailed" | "off" | "on" | null;
	serviceTier?: ResponseCreateParamsStreaming["service_tier"];
	textVerbosity?: "low" | "medium" | "high";
	toolChoice?: "auto" | "none" | "required";
}

type CodexResponseStatus = "completed" | "incomplete" | "failed" | "cancelled" | "queued" | "in_progress";

interface RequestBody {
	model: string;
	store?: boolean;
	stream?: boolean;
	instructions?: string;
	previous_response_id?: string;
	input?: ResponseInput;
	tools?: OpenAITool[];
	tool_choice?: OpenAICodexResponsesOptions["toolChoice"];
	parallel_tool_calls?: boolean;
	temperature?: number;
	reasoning?: { effort?: string; summary?: string };
	service_tier?: ResponseCreateParamsStreaming["service_tier"];
	text?: { verbosity?: string };
	include?: string[];
	prompt_cache_key?: string;
	[key: string]: unknown;
}

type SuccessfulAssistantMessage = AssistantMessage & { stopReason: "stop" | "length" | "toolUse" };

function assertSuccessfulOutput(output: AssistantMessage): asserts output is SuccessfulAssistantMessage {
	if (output.stopReason === "pending") {
		throw new Error("Codex stream ended without a stop reason");
	}
	if (output.stopReason === "error" || output.stopReason === "aborted") {
		throw new Error(output.errorMessage || "An unknown error occurred");
	}
}

// ============================================================================
// Retry Helpers
// ============================================================================

function isTerminalRateLimitError(errorText: string): boolean {
	return /GoUsageLimitError|FreeUsageLimitError|Monthly usage limit reached|available balance|insufficient_quota|out of budget|quota exceeded|billing/i.test(
		errorText,
	);
}

function isRetryableError(status: number, errorText: string): boolean {
	if (status === 429 && isTerminalRateLimitError(errorText)) {
		return false;
	}
	if (status === 429 || status === 500 || status === 502 || status === 503 || status === 504) {
		return true;
	}
	return /rate.?limit|overloaded|service.?unavailable|upstream.?connect|connection.?refused/i.test(errorText);
}

function getRetryAfterDelayMs(headers: Headers): number | undefined {
	const retryAfterMs = headers.get("retry-after-ms");
	if (retryAfterMs !== null) {
		const millis = Number(retryAfterMs);
		if (Number.isFinite(millis)) {
			return Math.max(0, millis);
		}
	}

	const retryAfter = headers.get("retry-after");
	if (!retryAfter) {
		return undefined;
	}

	const seconds = Number(retryAfter);
	if (Number.isFinite(seconds)) {
		return Math.max(0, seconds * 1000);
	}

	const date = Date.parse(retryAfter);
	if (!Number.isNaN(date)) {
		return Math.max(0, date - Date.now());
	}

	return undefined;
}

class RetryDelayExceededError extends Error {}

function validateRetryDelayMs(delayMs: number, options?: StreamOptions): number {
	const maxRetryDelayMs = options?.maxRetryDelayMs ?? DEFAULT_MAX_RETRY_DELAY_MS;
	if (maxRetryDelayMs > 0 && delayMs > maxRetryDelayMs) {
		throw new RetryDelayExceededError(
			`Server requested ${Math.ceil(delayMs / 1000)}s retry delay (max: ${Math.ceil(maxRetryDelayMs / 1000)}s)`,
		);
	}
	return delayMs;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal?.aborted) {
			reject(new Error("Request was aborted"));
			return;
		}
		const timeout = setTimeout(resolve, ms);
		signal?.addEventListener("abort", () => {
			clearTimeout(timeout);
			reject(new Error("Request was aborted"));
		});
	});
}

function normalizeTimeoutMs(value: number | undefined): number | undefined {
	if (value === undefined) return undefined;
	if (!Number.isFinite(value) || value < 0) {
		throw new Error(`Invalid timeoutMs: ${String(value)}`);
	}
	return Math.floor(value);
}

/**
 * Error thrown when an in-flight SSE response body goes idle (no bytes for
 * `idleTimeoutMs`) after the response headers have already arrived. The
 * per-request `timeoutMs`/`AbortSignal.timeout` only bounds the initial
 * header fetch; without this guard a Codex response that stops yielding
 * tokens while holding the socket open would hang the read forever. Mirrors
 * the WebSocket transport's `WebSocket idle timeout` so both transports honor
 * the documented "stream idleness after connection uses timeoutMs" contract.
 */
class CodexStreamIdleTimeoutError extends Error {
	constructor(idleTimeoutMs: number) {
		super(`Codex SSE stream idle timeout after ${idleTimeoutMs}ms`);
		this.name = "CodexStreamIdleTimeoutError";
	}
}

// ============================================================================
// Request Compression
// ============================================================================

type ProcessWithBuiltinModule = typeof process & {
	getBuiltinModule?: (id: "node:zlib") => typeof NodeZlib;
};

function loadNodeZlib(): typeof NodeZlib | null {
	if (typeof process === "undefined" || !(process.versions?.node || process.versions?.bun)) {
		return null;
	}
	return (process as ProcessWithBuiltinModule).getBuiltinModule?.("node:zlib") ?? null;
}

// Returns the zstd-compressed body bytes, or null when compression is
// unavailable (browser/Vite builds). Callers fall back to sending the
// uncompressed JSON when this returns null.
function compressRequestBodyZstd(bodyJson: string): Uint8Array | null {
	const zlib = loadNodeZlib();
	if (!zlib || typeof zlib.zstdCompressSync !== "function") {
		return null;
	}
	try {
		const compressed = zlib.zstdCompressSync(bodyJson, {
			params: { [zlib.constants.ZSTD_c_compressionLevel]: REQUEST_COMPRESSION_ZSTD_LEVEL },
		});
		return new Uint8Array(compressed.buffer, compressed.byteOffset, compressed.byteLength);
	} catch {
		return null;
	}
}

// ============================================================================
// Main Stream Function
// ============================================================================

export const stream: StreamFunction<"openai-codex-responses", OpenAICodexResponsesOptions> = (
	model: Model<"openai-codex-responses">,
	context: Context,
	options?: OpenAICodexResponsesOptions,
): AssistantMessageEventStream => {
	const stream = new AssistantMessageEventStream();

	(async () => {
		const output: AssistantMessage = {
			role: "assistant",
			content: [],
			api: "openai-codex-responses" as Api,
			provider: model.provider,
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "pending",
			timestamp: Date.now(),
		};

		try {
			const apiKey = options?.apiKey;
			if (!apiKey) {
				throw new Error(`No API key for provider: ${model.provider}`);
			}

			const accountId = extractAccountId(apiKey);
			const grammarToolInputProperties = createGrammarToolInputProperties(
				context.tools,
				model.compat?.supportsOpenAIGrammarTools ?? false,
			);
			const cacheSessionId = options?.cacheRetention === "none" ? undefined : options?.sessionId;
			const codexSessionId = clampOpenAIPromptCacheKey(cacheSessionId);
			let body = buildRequestBody(model, context, options, codexSessionId, grammarToolInputProperties);
			const nextBody = await options?.onPayload?.(body, model);
			if (nextBody !== undefined) {
				body = nextBody as RequestBody;
			}
			const websocketRequestId = codexSessionId || uuidv7();
			const sseHeaders = buildSSEHeaders(model.headers, options?.headers, accountId, apiKey, codexSessionId);
			const websocketHeaders = buildWebSocketHeaders(
				model.headers,
				options?.headers,
				accountId,
				apiKey,
				websocketRequestId,
			);
			const bodyJson = JSON.stringify(body);
			const httpTimeoutMs = normalizeTimeoutMs(options?.timeoutMs);
			const websocketConnectTimeoutMs = normalizeTimeoutMs(options?.websocketConnectTimeoutMs);
			const transport = options?.transport || "auto";
			let startEmitted = false;
			const websocketDisabledForSession = transport !== "sse" && isWebSocketSseFallbackActive(cacheSessionId);
			if (websocketDisabledForSession) {
				recordWebSocketSseFallback(cacheSessionId);
			}

			if (transport !== "sse" && !websocketDisabledForSession) {
				let websocketStarted = false;
				let retriedWebSocketConnectionLimit = false;
				let retriedMissingWebSocketContinuation = false;
				while (true) {
					websocketStarted = false;
					try {
						await processWebSocketStream(
							resolveCodexWebSocketUrl(model.baseUrl),
							body,
							websocketHeaders,
							output,
							stream,
							model,
							() => {
								websocketStarted = true;
								if (!startEmitted) {
									startEmitted = true;
									stream.push({ type: "start", partial: output });
								}
							},
							httpTimeoutMs,
							websocketConnectTimeoutMs,
							cacheSessionId,
							accountId,
							grammarToolInputProperties,
							options,
						);

						if (options?.signal?.aborted) {
							throw new Error("Request was aborted");
						}
						assertSuccessfulOutput(output);
						stream.push({
							type: "done",
							reason: output.stopReason,
							message: output,
						});
						stream.end();
						return;
					} catch (error) {
						const aborted = options?.signal?.aborted;
						const connectionLimitBeforeStart = !websocketStarted && isWebSocketConnectionLimitReachedError(error);
						const previousResponseNotFound = isPreviousResponseNotFoundError(error);
						if (!aborted && previousResponseNotFound && !retriedMissingWebSocketContinuation) {
							retriedMissingWebSocketContinuation = true;
							continue;
						}
						if (!aborted && connectionLimitBeforeStart && !retriedWebSocketConnectionLimit) {
							retriedWebSocketConnectionLimit = true;
							continue;
						}
						if (aborted || (isCodexNonTransportError(error) && !connectionLimitBeforeStart)) {
							throw error;
						}
						appendAssistantMessageDiagnostic(
							output,
							createAssistantMessageDiagnostic("provider_transport_failure", error, {
								configuredTransport: transport,
								fallbackTransport: websocketStarted ? undefined : "sse",
								eventsEmitted: websocketStarted,
								phase: websocketStarted ? "after_message_stream_start" : "before_message_stream_start",
								requestBytes: new TextEncoder().encode(bodyJson).byteLength,
								websocket: describeWebSocketTransportFailure(error),
							}),
						);
						recordWebSocketFailure(cacheSessionId, error);
						if (websocketStarted) {
							throw error;
						}
						recordWebSocketSseFallback(cacheSessionId);
						break;
					}
				}
			}

			// Compress the request body once for the SSE path. The Codex backend
			// decodes Content-Encoding: zstd; the WebSocket transport above sends the
			// uncompressed JSON frame, matching the official Codex client.
			const compressedBody = compressRequestBodyZstd(bodyJson);
			if (compressedBody) {
				sseHeaders.set("content-encoding", "zstd");
			}
			const sseBody: Uint8Array | string = compressedBody ?? bodyJson;

			// Fetch with retry logic for rate limits and transient errors
			let response: Response | undefined;
			let lastError: Error | undefined;
			const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;

			for (let attempt = 0; attempt <= maxRetries; attempt++) {
				if (options?.signal?.aborted) {
					throw new Error("Request was aborted");
				}

				try {
					const headerTimeoutSignal =
						httpTimeoutMs !== undefined && httpTimeoutMs > 0 ? AbortSignal.timeout(httpTimeoutMs) : undefined;
					const combinedSignal = combineAbortSignals([options?.signal, headerTimeoutSignal]);
					try {
						response = await (options?.fetch ?? globalThis.fetch)(resolveCodexUrl(model.baseUrl), {
							method: "POST",
							headers: sseHeaders,
							body: sseBody,
							signal: combinedSignal.signal,
						});
					} catch (error) {
						if (headerTimeoutSignal?.aborted && !options?.signal?.aborted) {
							throw new Error(`Codex SSE response headers timed out after ${httpTimeoutMs}ms`);
						}
						throw error;
					} finally {
						combinedSignal.cleanup();
					}
					await options?.onResponse?.(
						{ status: response.status, headers: headersToRecord(response.headers) },
						model,
					);

					if (response.ok) {
						break;
					}

					const errorText = await response.text();
					if (attempt < maxRetries && isRetryableError(response.status, errorText)) {
						const retryAfterDelayMs = getRetryAfterDelayMs(response.headers);
						const delayMs =
							retryAfterDelayMs === undefined
								? BASE_DELAY_MS * 2 ** attempt
								: validateRetryDelayMs(retryAfterDelayMs, options);

						await sleep(delayMs, options?.signal);
						continue;
					}

					// Parse error for friendly message on final attempt or non-retryable error
					const fakeResponse = new Response(errorText, {
						status: response.status,
						statusText: response.statusText,
					});
					const info = await parseErrorResponse(fakeResponse);
					throw new Error(info.friendlyMessage || info.message);
				} catch (error) {
					if (error instanceof Error) {
						if (error.name === "AbortError" || error.message === "Request was aborted") {
							throw new Error("Request was aborted");
						}
					}
					lastError = error instanceof Error ? error : new Error(String(error));
					// Network errors are retryable
					if (
						attempt < maxRetries &&
						!(lastError instanceof RetryDelayExceededError) &&
						!lastError.message.includes("usage limit")
					) {
						const delayMs = BASE_DELAY_MS * 2 ** attempt;
						await sleep(delayMs, options?.signal);
						continue;
					}
					throw lastError;
				}
			}

			if (!response?.ok) {
				throw lastError ?? new Error("Failed after retries");
			}

			if (!response.body) {
				throw new Error("No response body");
			}

			if (!startEmitted) {
				startEmitted = true;
				stream.push({ type: "start", partial: output });
			}
			await processStream(response, output, stream, model, grammarToolInputProperties, options);

			if (options?.signal?.aborted) {
				throw new Error("Request was aborted");
			}

			assertSuccessfulOutput(output);
			stream.push({ type: "done", reason: output.stopReason, message: output });
			stream.end();
		} catch (error) {
			for (const block of output.content) {
				// Streaming scratch buffers are only used during parsing; never persist them.
				delete (block as { partialJson?: string }).partialJson;
				delete (block as { customInput?: unknown }).customInput;
			}
			output.stopReason = options?.signal?.aborted ? "aborted" : "error";
			output.errorMessage = formatProviderError(normalizeProviderError(error));
			stream.push({ type: "error", reason: output.stopReason, error: output });
			stream.end();
		}
	})();

	return stream;
};

export const streamSimple: StreamFunction<"openai-codex-responses", SimpleStreamOptions> = (
	model: Model<"openai-codex-responses">,
	context: Context,
	options?: SimpleStreamOptions,
): AssistantMessageEventStream => {
	const apiKey = options?.apiKey;
	if (!apiKey) {
		throw new Error(`No API key for provider: ${model.provider}`);
	}

	const base = buildBaseOptions(model, context, options, apiKey);
	const clampedReasoning = options?.reasoning ? clampThinkingLevel(model, options.reasoning) : undefined;
	const reasoningEffort = clampedReasoning === "off" ? undefined : clampedReasoning;

	return stream(model, context, {
		...base,
		reasoningEffort,
	} satisfies OpenAICodexResponsesOptions);
};

// ============================================================================
// Request Building
// ============================================================================

function buildRequestBody(
	model: Model<"openai-codex-responses">,
	context: Context,
	options: OpenAICodexResponsesOptions | undefined,
	cacheSessionId: string | undefined,
	grammarToolInputProperties: ReadonlyMap<string, string> = createGrammarToolInputProperties(
		context.tools,
		model.compat?.supportsOpenAIGrammarTools ?? false,
	),
): RequestBody {
	const supportsStrictMode = model.compat?.supportsStrictMode ?? true;
	const supportsOpenAIGrammarTools = model.compat?.supportsOpenAIGrammarTools ?? false;
	const toolPlacement = splitDeferredTools(context, model.compat?.supportsToolSearch ?? false);
	const messages = convertResponsesMessages(model, context, CODEX_TOOL_CALL_PROVIDERS, {
		includeSystemPrompt: false,
		grammarToolInputProperties,
		deferredTools: toolPlacement.deferred,
		toolOptions: {
			strict: null,
			supportsStrictMode,
			supportsOpenAIGrammarTools,
		},
	});

	const body: RequestBody = {
		model: model.id,
		store: false,
		stream: true,
		instructions: context.systemPrompt || "You are a helpful assistant.",
		input: messages,
		text: { verbosity: options?.textVerbosity || "low" },
		include: ["reasoning.encrypted_content"],
		prompt_cache_key: cacheSessionId,
		tool_choice: options?.toolChoice ?? "auto",
		parallel_tool_calls: true,
	};

	if (options?.temperature !== undefined) {
		body.temperature = options.temperature;
	}

	if (options?.serviceTier !== undefined) {
		body.service_tier = options.serviceTier;
	}

	if (toolPlacement.immediate.length > 0) {
		body.tools = convertResponsesTools(toolPlacement.immediate, {
			strict: null,
			supportsStrictMode,
			supportsOpenAIGrammarTools,
		});
	}

	if (options?.reasoningEffort !== undefined) {
		const effort =
			options.reasoningEffort === "none"
				? (model.thinkingLevelMap?.off ?? "none")
				: (model.thinkingLevelMap?.[options.reasoningEffort] ?? options.reasoningEffort);
		if (effort !== null) {
			body.reasoning = {
				effort,
				summary: options.reasoningSummary ?? "auto",
			};
		}
	}

	return body;
}

function getServiceTierCostMultiplier(
	model: Pick<Model<"openai-codex-responses">, "id">,
	serviceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
): number {
	switch (serviceTier) {
		case "flex":
			return 0.5;
		case "priority":
			return model.id === "gpt-5.5" ? 2.5 : 2;
		default:
			return 1;
	}
}

function applyServiceTierPricing(
	usage: Usage,
	serviceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
	model: Pick<Model<"openai-codex-responses">, "id">,
) {
	const multiplier = getServiceTierCostMultiplier(model, serviceTier);
	if (multiplier === 1) return;

	usage.cost.input *= multiplier;
	usage.cost.output *= multiplier;
	usage.cost.cacheRead *= multiplier;
	usage.cost.cacheWrite *= multiplier;
	usage.cost.total = usage.cost.input + usage.cost.output + usage.cost.cacheRead + usage.cost.cacheWrite;
}

function resolveCodexServiceTier(
	responseServiceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
	requestServiceTier: ResponseCreateParamsStreaming["service_tier"] | undefined,
): ResponseCreateParamsStreaming["service_tier"] | undefined {
	if (responseServiceTier === "default" && (requestServiceTier === "flex" || requestServiceTier === "priority")) {
		return requestServiceTier;
	}
	return responseServiceTier ?? requestServiceTier;
}

function resolveCodexUrl(baseUrl?: string): string {
	const raw = baseUrl && baseUrl.trim().length > 0 ? baseUrl : DEFAULT_CODEX_BASE_URL;
	const normalized = raw.replace(/\/+$/, "");
	if (normalized.endsWith("/codex/responses")) return normalized;
	if (normalized.endsWith("/codex")) return `${normalized}/responses`;
	return `${normalized}/codex/responses`;
}

function resolveCodexWebSocketUrl(baseUrl?: string): string {
	const url = new URL(resolveCodexUrl(baseUrl));
	if (url.protocol === "https:") url.protocol = "wss:";
	if (url.protocol === "http:") url.protocol = "ws:";
	return url.toString();
}

// ============================================================================
// Response Processing
// ============================================================================

async function processStream(
	response: Response,
	output: AssistantMessage,
	stream: AssistantMessageEventStream,
	model: Model<"openai-codex-responses">,
	grammarToolInputProperties: ReadonlyMap<string, string>,
	options?: OpenAICodexResponsesOptions,
): Promise<void> {
	// Bound stream idleness after the headers arrive, matching the WebSocket
	// transport's `idleTimeoutMs`. The per-request `timeoutMs` only guards the
	// initial header fetch; without this a Codex response that stops yielding
	// tokens while holding the socket open would hang `reader.read()` forever.
	const streamIdleTimeoutMs = normalizeTimeoutMs(options?.timeoutMs);
	await processResponsesStream(
		mapCodexEvents(parseSSE(response, options?.signal, streamIdleTimeoutMs)),
		output,
		stream,
		model,
		{
			serviceTier: options?.serviceTier,
			grammarToolInputProperties,
			resolveServiceTier: resolveCodexServiceTier,
			applyServiceTierPricing: (usage, serviceTier) => applyServiceTierPricing(usage, serviceTier, model),
		},
	);
}

class CodexApiError extends Error {
	readonly code?: string;
	readonly payload?: Record<string, unknown>;

	constructor(message: string, options?: { code?: string; payload?: Record<string, unknown>; cause?: unknown }) {
		super(message);
		this.name = "CodexApiError";
		this.code = options?.code;
		this.payload = options?.payload;
		this.cause = options?.cause;
	}
}

class CodexProtocolError extends Error {
	readonly payload?: unknown;

	constructor(message: string, options?: { payload?: unknown; cause?: unknown }) {
		super(message);
		this.name = "CodexProtocolError";
		this.payload = options?.payload;
		this.cause = options?.cause;
	}
}

function isCodexNonTransportError(error: unknown): boolean {
	return error instanceof CodexApiError || error instanceof CodexProtocolError;
}

function isWebSocketConnectionLimitReachedError(error: unknown): boolean {
	return error instanceof CodexApiError && error.code === WEBSOCKET_CONNECTION_LIMIT_REACHED_CODE;
}

function isPreviousResponseNotFoundError(error: unknown): boolean {
	return error instanceof CodexApiError && error.code === PREVIOUS_RESPONSE_NOT_FOUND_CODE;
}

function extractCodexEventError(event: Record<string, unknown>): { code?: string; message?: string } {
	const nested = event.error && typeof event.error === "object" ? (event.error as Record<string, unknown>) : undefined;
	return {
		code: typeof event.code === "string" ? event.code : typeof nested?.code === "string" ? nested.code : undefined,
		message:
			typeof event.message === "string"
				? event.message
				: typeof nested?.message === "string"
					? nested.message
					: undefined,
	};
}

async function* mapCodexEvents(events: AsyncIterable<Record<string, unknown>>): AsyncGenerator<ResponseStreamEvent> {
	for await (const event of events) {
		const type = typeof event.type === "string" ? event.type : undefined;
		if (!type) continue;

		if (type === "error") {
			const { code, message } = extractCodexEventError(event);
			throw new CodexApiError(`Codex error: ${message || code || JSON.stringify(event)}`, {
				code,
				payload: event,
			});
		}

		if (type === "response.failed") {
			const response = (event as { response?: { error?: { code?: string; message?: string } } }).response;
			const code = response?.error?.code;
			const message = response?.error?.message;
			throw new CodexApiError(message || "Codex response failed", { code, payload: event });
		}

		if (type === "response.done" || type === "response.completed" || type === "response.incomplete") {
			const response = (event as { response?: { status?: unknown } }).response;
			const normalizedResponse = response
				? { ...response, status: normalizeCodexStatus(response.status) }
				: response;
			yield { ...event, type: "response.completed", response: normalizedResponse } as ResponseStreamEvent;
			return;
		}

		yield event as unknown as ResponseStreamEvent;
	}
}

function normalizeCodexStatus(status: unknown): CodexResponseStatus | undefined {
	if (typeof status !== "string") return undefined;
	return CODEX_RESPONSE_STATUSES.has(status as CodexResponseStatus) ? (status as CodexResponseStatus) : undefined;
}

// ============================================================================
// SSE Parsing
// ============================================================================

async function* parseSSE(
	response: Response,
	signal?: AbortSignal,
	idleTimeoutMs?: number,
): AsyncGenerator<Record<string, unknown>> {
	if (!response.body) return;

	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";
	const onAbort = () => {
		void reader.cancel().catch(() => {});
	};
	signal?.addEventListener("abort", onAbort, { once: true });

	// Race each read against an idle timer that resets on every chunk. A stalled
	// stream (socket held open, no bytes) trips it; a stream that keeps yielding
	// tokens never does. `undefined`/`0` disables it (operator "disabled").
	type ReadResult = Awaited<ReturnType<typeof reader.read>>;
	const readWithIdleTimeout = (): Promise<ReadResult> => {
		const read = reader.read();
		if (idleTimeoutMs === undefined || idleTimeoutMs <= 0) return read;
		return new Promise<ReadResult>((resolve, reject) => {
			const timer = setTimeout(() => {
				void reader.cancel().catch(() => {});
				reject(new CodexStreamIdleTimeoutError(idleTimeoutMs));
			}, idleTimeoutMs);
			read.then(
				(result) => {
					clearTimeout(timer);
					resolve(result);
				},
				(error) => {
					clearTimeout(timer);
					reject(error);
				},
			);
		});
	};

	try {
		while (true) {
			if (signal?.aborted) {
				throw new Error("Request was aborted");
			}
			const { done, value } = await readWithIdleTimeout();
			if (signal?.aborted) {
				throw new Error("Request was aborted");
			}
			if (done) break;
			buffer += decoder.decode(value, { stream: true });

			let idx = buffer.indexOf("\n\n");
			while (idx !== -1) {
				const chunk = buffer.slice(0, idx);
				buffer = buffer.slice(idx + 2);

				const dataLines = chunk
					.split("\n")
					.filter((l) => l.startsWith("data:"))
					.map((l) => l.slice(5).trim());
				if (dataLines.length > 0) {
					const data = dataLines.join("\n").trim();
					if (data && data !== "[DONE]") {
						try {
							yield JSON.parse(data) as Record<string, unknown>;
						} catch (cause) {
							throw new CodexProtocolError(`Invalid Codex SSE JSON: ${formatThrownValue(cause)}`, {
								cause,
								payload: data,
							});
						}
					}
				}
				idx = buffer.indexOf("\n\n");
			}
		}
	} finally {
		signal?.removeEventListener("abort", onAbort);
		try {
			await reader.cancel();
		} catch {}
		try {
			reader.releaseLock();
		} catch {}
	}
}

// ============================================================================
// WebSocket Parsing
// ============================================================================

const OPENAI_BETA_RESPONSES_WEBSOCKETS = "responses_websockets=2026-02-06";
const SESSION_WEBSOCKET_CACHE_TTL_MS = 5 * 60 * 1000;
const SESSION_WEBSOCKET_MAX_AGE_MS = 55 * 60 * 1000;
const WEBSOCKET_ABNORMAL_CLOSE_CODE = 1006;
const WEBSOCKET_HANDSHAKE_PROBE_TIMEOUT_MS = 5_000;
const WEBSOCKET_HANDSHAKE_PROBE_BODY_LIMIT = 400;
// undici reports every rejected upgrade with the same placeholder text (and
// npm undici >= 8 reports no text at all), so these strings carry no signal and
// must not suppress the handshake probe below.
const WEBSOCKET_GENERIC_FAILURE_MESSAGES = new Set([
	"Received network error or non-101 status code.",
	"Received network error or non-200 status code.",
]);
// Response headers worth reporting when an upgrade is rejected: they are what
// tells a usage cap apart from an auth failure or an edge block. Anything that
// looks like a credential is dropped by WEBSOCKET_HANDSHAKE_REDACTED_HEADERS.
const WEBSOCKET_HANDSHAKE_REPORTED_HEADERS = new Set([
	"retry-after",
	"retry-after-ms",
	"content-type",
	"server",
	"date",
	"cf-ray",
	"cf-mitigated",
	"x-request-id",
	"x-should-retry",
	"www-authenticate",
]);
const WEBSOCKET_HANDSHAKE_REPORTED_HEADER_PREFIXES = ["x-ratelimit-", "ratelimit-", "x-codex-", "x-openai-"];
// Defense in depth on top of the allow-list above: never report a header whose
// name reads like a credential, however it got allow-listed.
const WEBSOCKET_HANDSHAKE_REDACTED_HEADERS = /(^|-)(authorization|cookie|token|secret|key|credential)(-|$)/i;

type WebSocketEventType = "open" | "message" | "error" | "close";
type WebSocketListener = (event: unknown) => void;

interface WebSocketLike {
	close(code?: number, reason?: string): void;
	send(data: string): void;
	addEventListener(type: WebSocketEventType, listener: WebSocketListener): void;
	removeEventListener(type: WebSocketEventType, listener: WebSocketListener): void;
}

interface CachedWebSocketContinuationState {
	lastRequestBody: RequestBody;
	lastResponseId: string;
	lastResponseItems: ResponseInput;
}

interface CachedWebSocketConnection {
	socket: WebSocketLike;
	busy: boolean;
	createdAt: number;
	idleTimer?: ReturnType<typeof setTimeout>;
	continuation?: CachedWebSocketContinuationState;
}

export interface OpenAICodexWebSocketDebugStats {
	requests: number;
	connectionsCreated: number;
	connectionsReused: number;
	cachedContextRequests: number;
	storeTrueRequests: number;
	fullContextRequests: number;
	deltaRequests: number;
	lastInputItems: number;
	lastDeltaInputItems?: number;
	lastPreviousResponseId?: string;
	websocketFailures: number;
	sseFallbacks: number;
	websocketFallbackActive?: boolean;
	lastWebSocketError?: string;
}

const websocketSessionCache = new Map<string, Map<string, CachedWebSocketConnection>>();
const websocketDebugStats = new Map<string, OpenAICodexWebSocketDebugStats>();
const websocketSseFallbackSessions = new Set<string>();

function getOrCreateWebSocketDebugStats(sessionId: string): OpenAICodexWebSocketDebugStats {
	let stats = websocketDebugStats.get(sessionId);
	if (!stats) {
		stats = {
			requests: 0,
			connectionsCreated: 0,
			connectionsReused: 0,
			cachedContextRequests: 0,
			storeTrueRequests: 0,
			fullContextRequests: 0,
			deltaRequests: 0,
			lastInputItems: 0,
			websocketFailures: 0,
			sseFallbacks: 0,
		};
		websocketDebugStats.set(sessionId, stats);
	}
	return stats;
}

export function getOpenAICodexWebSocketDebugStats(sessionId: string): OpenAICodexWebSocketDebugStats | undefined {
	const stats = websocketDebugStats.get(sessionId);
	return stats ? { ...stats } : undefined;
}

export function resetOpenAICodexWebSocketDebugStats(sessionId?: string): void {
	if (sessionId) {
		websocketDebugStats.delete(sessionId);
		websocketSseFallbackSessions.delete(sessionId);
		return;
	}
	websocketDebugStats.clear();
	websocketSseFallbackSessions.clear();
}

export function closeOpenAICodexWebSocketSessions(sessionId?: string): void {
	const closeEntry = (entry: CachedWebSocketConnection) => {
		if (entry.idleTimer) clearTimeout(entry.idleTimer);
		closeWebSocketSilently(entry.socket, 1000, "debug_close");
	};
	if (sessionId) {
		for (const entry of websocketSessionCache.get(sessionId)?.values() ?? []) closeEntry(entry);
		websocketSessionCache.delete(sessionId);
		return;
	}
	for (const accountEntries of websocketSessionCache.values()) {
		for (const entry of accountEntries.values()) closeEntry(entry);
	}
	websocketSessionCache.clear();
}

registerSessionResourceCleanup(closeOpenAICodexWebSocketSessions);

function isWebSocketSseFallbackActive(sessionId: string | undefined): boolean {
	return sessionId ? websocketSseFallbackSessions.has(sessionId) : false;
}

function recordWebSocketSseFallback(sessionId: string | undefined): void {
	if (!sessionId) return;
	const stats = getOrCreateWebSocketDebugStats(sessionId);
	stats.sseFallbacks++;
	stats.websocketFallbackActive = isWebSocketSseFallbackActive(sessionId);
}

function recordWebSocketFailure(sessionId: string | undefined, error: unknown): void {
	if (!sessionId) return;
	websocketSseFallbackSessions.add(sessionId);

	const stats = getOrCreateWebSocketDebugStats(sessionId);
	stats.websocketFailures++;
	stats.lastWebSocketError = formatThrownValue(error);
	stats.websocketFallbackActive = true;
}

type WebSocketConstructor = new (
	url: string,
	protocols?: string | string[] | { headers?: Record<string, string> },
) => WebSocketLike;

let _cachedWebsocket: WebSocketConstructor | null = null;
async function getWebSocketConstructor(env?: ProviderEnv): Promise<WebSocketConstructor | null> {
	if (!env && _cachedWebsocket) return _cachedWebsocket;

	// bun doesn't respect http proxy envs, ref: https://github.com/oven-sh/bun/issues/15489
	// TODO: remove this when bun supports proxy envs in websocket.
	if (typeof process !== "undefined" && process.versions?.bun) {
		const WebSocketWithProxy = class extends WebSocket {
			constructor(url: string | URL, options?: string | string[] | Record<string, unknown>) {
				let _opts: Record<string, unknown> = {};
				if (Array.isArray(options) || typeof options === "string") {
					_opts = { protocols: options };
				} else {
					_opts = { ...options };
				}

				const proxyUrl = resolveHttpProxyUrlForTarget(
					url.toString().replace(/^wss:/, "https:").replace(/^ws:/, "http:"),
					env,
				);
				super(url, { ..._opts, ...(proxyUrl ? { proxy: proxyUrl.toString() } : {}) } as any);
			}
		};
		if (!env) {
			_cachedWebsocket = WebSocketWithProxy;
		}
		return WebSocketWithProxy;
	}

	const ctor = (globalThis as { WebSocket?: unknown }).WebSocket;
	if (typeof ctor !== "function") return null;
	return ctor as unknown as WebSocketConstructor;
}

/**
 * Everything the runtime is willing to tell us about a failed Codex WebSocket.
 *
 * The events alone are close to useless: coding-agent installs npm undici's
 * globals, and npm undici routes every connect failure through
 * `failWebsocketConnection` -> `#onSocketClose`, which drops the close code,
 * the close reason and the whole handshake response, then fires
 * `ErrorEvent { message: "", error: TypeError("") }` followed by
 * `CloseEvent { code: 1006, reason: "" }`. Node's bundled undici is only
 * marginally better: one placeholder message for every cause. So a usage cap
 * (429), an expired token (401) and an edge block (403) all look identical
 * unless we go and ask — which is what `probeWebSocketHandshake` does.
 */
export interface WebSocketFailureDetails {
	/** Close code from the `close` event (1006 = closed with no close frame). */
	closeCode?: number;
	/** Close reason from the `close` event, when the peer sent one. */
	closeReason?: string;
	wasClean?: boolean;
	/** Text carried by the `error` event (or its nested error). */
	eventMessage?: string;
	/** Constructor name of the nested error, e.g. `TypeError`. */
	errorName?: string;
	errorCode?: string | number;
	/** Real transport failure undici hides in `error.cause` (ENOTFOUND, ...). */
	causeMessage?: string;
	causeCode?: string | number;
	/** HTTP status recovered by re-requesting the endpoint after the failure. */
	httpStatus?: number;
	httpStatusText?: string;
	/** Allow-listed, credential-free response headers (retry-after, ratelimit). */
	httpHeaders?: Record<string, string>;
	/** Truncated response body, where OpenAI puts the human-readable reason. */
	httpBody?: string;
	/** Why the handshake probe itself could not answer. */
	handshakeProbeError?: string;
}

class WebSocketCloseError extends Error {
	readonly code?: number;
	readonly reason?: string;
	readonly wasClean?: boolean;
	readonly details?: WebSocketFailureDetails;

	constructor(
		message: string,
		options?: { code?: number; reason?: string; wasClean?: boolean; details?: WebSocketFailureDetails },
	) {
		super(message);
		this.name = "WebSocketCloseError";
		this.code = options?.code;
		this.reason = options?.reason;
		this.wasClean = options?.wasClean;
		this.details = options?.details;
	}
}

/**
 * Structured transport detail for `provider_transport_failure` diagnostics.
 * `extractDiagnosticError` only forwards name/message/stack/code, so the same
 * facts are also folded into the error message by
 * `formatWebSocketFailureMessage`.
 */
export function describeWebSocketTransportFailure(error: unknown): WebSocketFailureDetails | undefined {
	if (!(error instanceof WebSocketCloseError)) return undefined;
	const details = error.details;
	if (!details || Object.values(details).every((value) => value === undefined)) return undefined;
	return details;
}

function getWebSocketReadyState(socket: WebSocketLike): number | undefined {
	const readyState = (socket as { readyState?: unknown }).readyState;
	return typeof readyState === "number" ? readyState : undefined;
}

function isWebSocketReusable(socket: WebSocketLike): boolean {
	const readyState = getWebSocketReadyState(socket);
	// If readyState is unavailable, assume the runtime keeps it open/reusable.
	return readyState === undefined || readyState === 1;
}

function isWebSocketSessionExpired(entry: CachedWebSocketConnection): boolean {
	return Date.now() - entry.createdAt >= SESSION_WEBSOCKET_MAX_AGE_MS;
}

function closeWebSocketSilently(socket: WebSocketLike, code = 1000, reason = "done"): void {
	try {
		socket.close(code, reason);
	} catch {}
}

function scheduleSessionWebSocketExpiry(sessionId: string, accountId: string, entry: CachedWebSocketConnection): void {
	if (entry.idleTimer) {
		clearTimeout(entry.idleTimer);
	}
	entry.idleTimer = setTimeout(() => {
		if (entry.busy) return;
		closeWebSocketSilently(entry.socket, 1000, "idle_timeout");
		const accountEntries = websocketSessionCache.get(sessionId);
		if (accountEntries?.get(accountId) === entry) accountEntries.delete(accountId);
		if (accountEntries?.size === 0) websocketSessionCache.delete(sessionId);
	}, SESSION_WEBSOCKET_CACHE_TTL_MS);
}

async function connectWebSocket(
	url: string,
	headers: Headers,
	signal?: AbortSignal,
	connectTimeoutMs = DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS,
	env?: ProviderEnv,
): Promise<WebSocketLike> {
	const WebSocketCtor = await getWebSocketConstructor(env);
	if (!WebSocketCtor) {
		throw new Error("WebSocket transport is not available in this runtime");
	}

	const wsHeaders = headersToRecord(headers);
	delete wsHeaders["OpenAI-Beta"];

	return new Promise<WebSocketLike>((resolve, reject) => {
		let settled = false;
		let timeout: ReturnType<typeof setTimeout> | undefined;
		let socket: WebSocketLike;
		let failureDetails: WebSocketFailureDetails | undefined;
		let failureScheduled = false;

		try {
			socket = new WebSocketCtor(url, { headers: wsHeaders });
		} catch (error) {
			reject(error instanceof Error ? error : new Error(String(error)));
			return;
		}

		const cleanup = () => {
			if (timeout) {
				clearTimeout(timeout);
				timeout = undefined;
			}
			socket.removeEventListener("open", onOpen);
			socket.removeEventListener("error", onError);
			socket.removeEventListener("close", onClose);
			signal?.removeEventListener("abort", onAbort);
		};
		const fail = (error: Error, closeReason?: string) => {
			if (settled) return;
			settled = true;
			cleanup();
			if (closeReason) {
				closeWebSocketSilently(socket, 1000, closeReason);
			}
			reject(error);
		};
		const onOpen: WebSocketListener = () => {
			if (settled) return;
			settled = true;
			cleanup();
			resolve(socket);
		};
		// The `error` and `close` events each carry half the story and undici
		// fires them back to back in the same tick, so settling on whichever
		// arrives first discards the other half — historically the close code.
		// Collect both, then recover the HTTP status if neither said anything.
		const settleFailure = async () => {
			if (settled) return;
			let details = failureDetails ?? {};
			if (shouldProbeWebSocketHandshake(details)) {
				details = mergeWebSocketFailureDetails(details, await probeWebSocketHandshake(url, wsHeaders, signal));
			}
			fail(createWebSocketFailureError(details, "WebSocket error"), "transport_failure");
		};
		const recordFailure = (details: WebSocketFailureDetails) => {
			failureDetails = mergeWebSocketFailureDetails(failureDetails, details);
			if (settled || failureScheduled) return;
			failureScheduled = true;
			// Stop the connect timeout from winning the race against the probe.
			if (timeout) {
				clearTimeout(timeout);
				timeout = undefined;
			}
			queueMicrotask(() => {
				void settleFailure();
			});
		};
		const onError: WebSocketListener = (event) => {
			recordFailure(collectWebSocketErrorDetails(event));
		};
		const onClose: WebSocketListener = (event) => {
			recordFailure(collectWebSocketCloseDetails(event));
		};
		const onAbort = () => {
			fail(new Error("Request was aborted"), "aborted");
		};

		socket.addEventListener("open", onOpen);
		socket.addEventListener("error", onError);
		socket.addEventListener("close", onClose);
		signal?.addEventListener("abort", onAbort);

		if (connectTimeoutMs > 0) {
			timeout = setTimeout(() => {
				fail(new Error(`WebSocket connect timeout after ${connectTimeoutMs}ms`), "connect_timeout");
			}, connectTimeoutMs);
		}
		if (signal?.aborted) {
			onAbort();
		}
	});
}

async function acquireWebSocket(
	url: string,
	headers: Headers,
	sessionId: string | undefined,
	accountId: string,
	signal?: AbortSignal,
	connectTimeoutMs?: number,
	env?: ProviderEnv,
): Promise<{
	socket: WebSocketLike;
	entry?: CachedWebSocketConnection;
	reused: boolean;
	release: (options?: { keep?: boolean }) => void;
}> {
	if (!sessionId) {
		const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs, env);
		return {
			socket,
			reused: false,
			release: () => closeWebSocketSilently(socket),
		};
	}

	let accountEntries = websocketSessionCache.get(sessionId);
	const cached = accountEntries?.get(accountId);
	if (cached) {
		if (cached.idleTimer) {
			clearTimeout(cached.idleTimer);
			cached.idleTimer = undefined;
		}
		if (!cached.busy && isWebSocketSessionExpired(cached)) {
			closeWebSocketSilently(cached.socket, 1000, "connection_age_limit");
			accountEntries?.delete(accountId);
			if (accountEntries?.size === 0) websocketSessionCache.delete(sessionId);
		} else if (!cached.busy && isWebSocketReusable(cached.socket)) {
			cached.busy = true;
			return {
				socket: cached.socket,
				entry: cached,
				reused: true,
				release: ({ keep } = {}) => {
					if (!keep || !isWebSocketReusable(cached.socket)) {
						closeWebSocketSilently(cached.socket);
						const currentEntries = websocketSessionCache.get(sessionId);
						if (currentEntries?.get(accountId) === cached) currentEntries.delete(accountId);
						if (currentEntries?.size === 0) websocketSessionCache.delete(sessionId);
						return;
					}
					cached.busy = false;
					scheduleSessionWebSocketExpiry(sessionId, accountId, cached);
				},
			};
		}
		if (cached.busy) {
			const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs, env);
			return {
				socket,
				reused: false,
				release: () => {
					closeWebSocketSilently(socket);
				},
			};
		}
		if (!isWebSocketReusable(cached.socket)) {
			closeWebSocketSilently(cached.socket);
			accountEntries?.delete(accountId);
			if (accountEntries?.size === 0) websocketSessionCache.delete(sessionId);
		}
	}

	const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs, env);
	const entry: CachedWebSocketConnection = { socket, busy: true, createdAt: Date.now() };
	accountEntries = websocketSessionCache.get(sessionId);
	if (!accountEntries) {
		accountEntries = new Map();
		websocketSessionCache.set(sessionId, accountEntries);
	}
	accountEntries.set(accountId, entry);
	return {
		socket,
		entry,
		reused: false,
		release: ({ keep } = {}) => {
			if (!keep || !isWebSocketReusable(entry.socket)) {
				closeWebSocketSilently(entry.socket);
				if (entry.idleTimer) clearTimeout(entry.idleTimer);
				const currentEntries = websocketSessionCache.get(sessionId);
				if (currentEntries?.get(accountId) === entry) currentEntries.delete(accountId);
				if (currentEntries?.size === 0) websocketSessionCache.delete(sessionId);
				return;
			}
			entry.busy = false;
			scheduleSessionWebSocketExpiry(sessionId, accountId, entry);
		},
	};
}

function readEventString(source: unknown, key: string): string | undefined {
	if (!source || typeof source !== "object") return undefined;
	const value = (source as Record<string, unknown>)[key];
	return typeof value === "string" && value.length > 0 ? value : undefined;
}

function readEventErrorCode(source: unknown): string | number | undefined {
	if (!source || typeof source !== "object") return undefined;
	const value = (source as { code?: unknown }).code;
	return typeof value === "string" || typeof value === "number" ? value : undefined;
}

function collectWebSocketErrorDetails(event: unknown): WebSocketFailureDetails {
	const details: WebSocketFailureDetails = {};
	if (!event || typeof event !== "object") return details;

	details.eventMessage = readEventString(event, "message");
	const nestedError = (event as { error?: unknown }).error;
	if (nestedError && typeof nestedError === "object") {
		details.eventMessage ??= readEventString(nestedError, "message");
		const name = readEventString(nestedError, "name");
		if (name && name !== "Error") details.errorName = name;
		details.errorCode = readEventErrorCode(nestedError);
		// Node's bundled undici puts the real transport failure (ENOTFOUND,
		// ECONNREFUSED, TLS verification, UND_ERR_SOCKET) in `cause` while the
		// message stays a placeholder; the old extractor threw it away.
		const cause = (nestedError as { cause?: unknown }).cause;
		if (cause !== undefined) {
			details.causeMessage = typeof cause === "string" ? cause : readEventString(cause, "message");
			details.causeCode = readEventErrorCode(cause);
		}
	}
	return details;
}

function collectWebSocketCloseDetails(event: unknown): WebSocketFailureDetails {
	const details: WebSocketFailureDetails = {};
	if (!event || typeof event !== "object") return details;

	const code = (event as { code?: unknown }).code;
	if (typeof code === "number") details.closeCode = code;
	details.closeReason = readEventString(event, "reason");
	const wasClean = (event as { wasClean?: unknown }).wasClean;
	if (typeof wasClean === "boolean") details.wasClean = wasClean;
	if (!details.closeReason && details.closeCode === WEBSOCKET_MESSAGE_TOO_BIG_CLOSE_CODE) {
		details.closeReason = "message too big";
	}
	return details;
}

function mergeWebSocketFailureDetails(...parts: (WebSocketFailureDetails | undefined)[]): WebSocketFailureDetails {
	const merged: Record<string, unknown> = {};
	for (const part of parts) {
		if (!part) continue;
		for (const [key, value] of Object.entries(part)) {
			if (value !== undefined) merged[key] ??= value;
		}
	}
	return merged as WebSocketFailureDetails;
}

function describeWebSocketCloseCode(code: number | undefined): string | undefined {
	switch (code) {
		case 1000:
			return "normal closure";
		case 1001:
			return "going away";
		case 1002:
			return "protocol error";
		case 1005:
			return "no close code";
		case WEBSOCKET_ABNORMAL_CLOSE_CODE:
			return "abnormal closure, no close frame";
		case 1008:
			return "policy violation";
		case WEBSOCKET_MESSAGE_TOO_BIG_CLOSE_CODE:
			return "message too big";
		case 1011:
			return "server error";
		case 1013:
			return "try again later";
		case 1015:
			return "TLS handshake failure";
		default:
			return undefined;
	}
}

function formatWebSocketHeaderHints(headers: Record<string, string> | undefined): string | undefined {
	if (!headers) return undefined;
	const entries = Object.entries(headers).map(([name, value]) => `${name}=${value}`);
	return entries.length > 0 ? entries.join(", ") : undefined;
}

/**
 * Fold every recovered fact into the message, because that is the only field
 * `extractDiagnosticError` reliably forwards into an assistant diagnostic.
 * With nothing recovered the message stays the historical placeholder.
 */
function formatWebSocketFailureMessage(details: WebSocketFailureDetails, fallback: string): string {
	const parts: string[] = [];
	if (details.httpStatus !== undefined) {
		parts.push(`HTTP ${details.httpStatus}${details.httpStatusText ? ` ${details.httpStatusText}` : ""}`);
	}
	if (details.closeCode !== undefined) {
		const label = details.closeReason ?? describeWebSocketCloseCode(details.closeCode);
		parts.push(`close ${details.closeCode}${label ? ` ${label}` : ""}`);
	} else if (details.closeReason) {
		parts.push(`close ${details.closeReason}`);
	}
	if (details.eventMessage) parts.push(details.eventMessage);
	if (details.causeMessage) parts.push(`cause ${details.causeMessage}`);
	else if (details.causeCode !== undefined) parts.push(`cause ${details.causeCode}`);
	const headerHints = formatWebSocketHeaderHints(details.httpHeaders);
	if (headerHints) parts.push(headerHints);
	if (details.httpBody) parts.push(details.httpBody);
	if (details.handshakeProbeError) parts.push(`handshake probe failed: ${details.handshakeProbeError}`);

	if (parts.length === 0) {
		return details.errorName ? `${fallback} (${details.errorName})` : fallback;
	}
	return `${fallback}: ${parts.join("; ")}`;
}

function createWebSocketFailureError(details: WebSocketFailureDetails, fallback: string): WebSocketCloseError {
	return new WebSocketCloseError(formatWebSocketFailureMessage(details, fallback), {
		code: details.closeCode,
		reason: details.closeReason,
		wasClean: details.wasClean,
		details,
	});
}

function isGenericWebSocketFailureText(text: string | undefined): boolean {
	return text === undefined || WEBSOCKET_GENERIC_FAILURE_MESSAGES.has(text);
}

/**
 * Only probe when the events said nothing usable. A real close reason, a real
 * error message or a `cause` already explains the failure, and re-requesting
 * would just be an extra call.
 */
function shouldProbeWebSocketHandshake(details: WebSocketFailureDetails): boolean {
	if (details.httpStatus !== undefined) return false;
	if (details.causeMessage !== undefined || details.causeCode !== undefined) return false;
	return isGenericWebSocketFailureText(details.eventMessage) && isGenericWebSocketFailureText(details.closeReason);
}

function resolveWebSocketProbeUrl(url: string): string | undefined {
	if (url.startsWith("wss://")) return `https://${url.slice("wss://".length)}`;
	if (url.startsWith("ws://")) return `http://${url.slice("ws://".length)}`;
	if (url.startsWith("https://") || url.startsWith("http://")) return url;
	return undefined;
}

function shouldReportHandshakeHeader(name: string): boolean {
	if (WEBSOCKET_HANDSHAKE_REDACTED_HEADERS.test(name)) return false;
	if (WEBSOCKET_HANDSHAKE_REPORTED_HEADERS.has(name)) return true;
	return WEBSOCKET_HANDSHAKE_REPORTED_HEADER_PREFIXES.some((prefix) => name.startsWith(prefix));
}

async function readWebSocketProbeBody(response: Response): Promise<string | undefined> {
	try {
		const collapsed = (await response.text()).replace(/\s+/g, " ").trim();
		if (!collapsed) return undefined;
		return collapsed.length > WEBSOCKET_HANDSHAKE_PROBE_BODY_LIMIT
			? `${collapsed.slice(0, WEBSOCKET_HANDSHAKE_PROBE_BODY_LIMIT)}...`
			: collapsed;
	} catch {
		return undefined;
	}
}

/**
 * Recover the HTTP status the WebSocket layer threw away.
 *
 * undici's fetch rejects `Connection`/`Upgrade`/`Sec-WebSocket-*` as forbidden
 * header names, so this is a plain authenticated GET rather than a replayed
 * upgrade. It still traverses the same edge, auth and quota layers that
 * rejected the upgrade, which is exactly what separates 429 (usage cap, with
 * `retry-after`) from 401 (auth) and 403 (edge block). Credentials are sent,
 * never reported: only allow-listed non-credential response headers come back.
 */
async function probeWebSocketHandshake(
	url: string,
	headers: Record<string, string>,
	signal?: AbortSignal,
): Promise<WebSocketFailureDetails> {
	const probeUrl = resolveWebSocketProbeUrl(url);
	if (!probeUrl || typeof globalThis.fetch !== "function") return {};

	const probeHeaders: Record<string, string> = {};
	for (const [name, value] of Object.entries(headers)) {
		const lower = name.toLowerCase();
		if (lower === "connection" || lower === "upgrade" || lower.startsWith("sec-websocket-")) continue;
		probeHeaders[name] = value;
	}

	const combined = combineAbortSignals([signal, AbortSignal.timeout(WEBSOCKET_HANDSHAKE_PROBE_TIMEOUT_MS)]);
	try {
		const response = await globalThis.fetch(probeUrl, {
			method: "GET",
			headers: probeHeaders,
			redirect: "manual",
			signal: combined.signal,
		});
		const reportedHeaders: Record<string, string> = {};
		for (const [name, value] of response.headers) {
			const lower = name.toLowerCase();
			if (shouldReportHandshakeHeader(lower)) reportedHeaders[lower] = value;
		}
		const details: WebSocketFailureDetails = {
			httpStatus: response.status,
			httpStatusText: response.statusText || undefined,
			httpHeaders: Object.keys(reportedHeaders).length > 0 ? reportedHeaders : undefined,
		};
		details.httpBody = await readWebSocketProbeBody(response);
		return details;
	} catch (error) {
		return { handshakeProbeError: formatThrownValue(error) };
	} finally {
		combined.cleanup();
	}
}

function extractWebSocketCloseError(event: unknown): Error {
	const details = collectWebSocketCloseDetails(event);
	const codeText = details.closeCode !== undefined ? ` ${details.closeCode}` : "";
	const label = details.closeReason ?? describeWebSocketCloseCode(details.closeCode);
	return new WebSocketCloseError(`WebSocket closed${codeText}${label ? ` ${label}` : ""}`.trim(), {
		code: details.closeCode,
		reason: details.closeReason,
		wasClean: details.wasClean,
		details,
	});
}

async function decodeWebSocketData(data: unknown): Promise<string | null> {
	if (typeof data === "string") return data;
	if (data instanceof ArrayBuffer) {
		return new TextDecoder().decode(new Uint8Array(data));
	}
	if (ArrayBuffer.isView(data)) {
		const view = data as ArrayBufferView;
		return new TextDecoder().decode(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
	}
	if (data && typeof data === "object" && "arrayBuffer" in data) {
		const blobLike = data as { arrayBuffer: () => Promise<ArrayBuffer> };
		const arrayBuffer = await blobLike.arrayBuffer();
		return new TextDecoder().decode(new Uint8Array(arrayBuffer));
	}
	return null;
}

async function* parseWebSocket(
	socket: WebSocketLike,
	signal?: AbortSignal,
	idleTimeoutMs?: number,
): AsyncGenerator<Record<string, unknown>> {
	const queue: Record<string, unknown>[] = [];
	let pending: (() => void) | null = null;
	let done = false;
	let failed: Error | null = null;
	let sawCompletion = false;
	let sawFailureEvent = false;
	let failureDetails: WebSocketFailureDetails | undefined;

	const wake = () => {
		if (!pending) return;
		const resolve = pending;
		pending = null;
		resolve();
	};

	const onMessage: WebSocketListener = (event) => {
		void (async () => {
			let text: string | null = null;
			try {
				if (!event || typeof event !== "object" || !("data" in event)) return;
				text = await decodeWebSocketData((event as { data?: unknown }).data);
				if (!text) return;
				const parsed = JSON.parse(text) as Record<string, unknown>;
				const type = typeof parsed.type === "string" ? parsed.type : "";
				if (type === "response.completed" || type === "response.done" || type === "response.incomplete") {
					sawCompletion = true;
					done = true;
				}
				queue.push(parsed);
				wake();
			} catch (cause) {
				failed = new CodexProtocolError(`Invalid Codex WebSocket JSON: ${formatThrownValue(cause)}`, {
					cause,
					payload: text,
				});
				done = true;
				wake();
			}
		})();
	};

	const onError: WebSocketListener = (event) => {
		sawFailureEvent = true;
		failureDetails = mergeWebSocketFailureDetails(failureDetails, collectWebSocketErrorDetails(event));
		failed = createWebSocketFailureError(failureDetails, "WebSocket error");
		done = true;
		wake();
	};

	const onClose: WebSocketListener = (event) => {
		if (sawCompletion) {
			done = true;
			wake();
			return;
		}
		failureDetails = mergeWebSocketFailureDetails(failureDetails, collectWebSocketCloseDetails(event));
		// The close event is what carries the code and reason. Let it refine an
		// already-recorded `error` event instead of being dropped outright.
		if (sawFailureEvent) {
			failed = createWebSocketFailureError(failureDetails, "WebSocket error");
		} else if (!failed) {
			failed = extractWebSocketCloseError(event);
		}
		done = true;
		wake();
	};

	const onAbort = () => {
		failed = new Error("Request was aborted");
		done = true;
		wake();
	};

	socket.addEventListener("message", onMessage);
	socket.addEventListener("error", onError);
	socket.addEventListener("close", onClose);
	signal?.addEventListener("abort", onAbort);

	try {
		while (true) {
			if (signal?.aborted) {
				throw new Error("Request was aborted");
			}
			if (queue.length > 0) {
				yield queue.shift()!;
				continue;
			}
			if (done) break;
			let timeout: ReturnType<typeof setTimeout> | undefined;
			await new Promise<void>((resolve, reject) => {
				pending = resolve;
				if (idleTimeoutMs !== undefined && idleTimeoutMs > 0) {
					timeout = setTimeout(() => {
						const error = new Error(`WebSocket idle timeout after ${idleTimeoutMs}ms`);
						failed = error;
						done = true;
						pending = null;
						closeWebSocketSilently(socket, 1000, "idle_timeout");
						reject(error);
					}, idleTimeoutMs);
				}
			}).finally(() => {
				if (timeout) {
					clearTimeout(timeout);
				}
			});
		}

		if (failed) {
			throw failed;
		}
		if (!sawCompletion) {
			throw new Error("WebSocket stream closed before response.completed");
		}
	} finally {
		socket.removeEventListener("message", onMessage);
		socket.removeEventListener("error", onError);
		socket.removeEventListener("close", onClose);
		signal?.removeEventListener("abort", onAbort);
	}
}

function requestBodyWithoutInput(body: RequestBody): RequestBody {
	const { input: _input, previous_response_id: _previousResponseId, ...rest } = body;
	return rest;
}

function responseInputsEqual(a: ResponseInput | undefined, b: ResponseInput | undefined): boolean {
	return JSON.stringify(a ?? []) === JSON.stringify(b ?? []);
}

function requestBodiesMatchExceptInput(a: RequestBody, b: RequestBody): boolean {
	return JSON.stringify(requestBodyWithoutInput(a)) === JSON.stringify(requestBodyWithoutInput(b));
}

function getCachedWebSocketInputDelta(
	body: RequestBody,
	continuation: CachedWebSocketContinuationState,
): ResponseInput | undefined {
	if (!requestBodiesMatchExceptInput(body, continuation.lastRequestBody)) {
		return undefined;
	}

	const currentInput = body.input ?? [];
	const baseline = [...(continuation.lastRequestBody.input ?? []), ...continuation.lastResponseItems];
	if (currentInput.length < baseline.length) {
		return undefined;
	}

	const prefix = currentInput.slice(0, baseline.length);
	if (!responseInputsEqual(prefix, baseline)) {
		return undefined;
	}

	return currentInput.slice(baseline.length);
}

function buildCachedWebSocketRequestBody(entry: CachedWebSocketConnection, body: RequestBody): RequestBody {
	const continuation = entry.continuation;
	if (!continuation) {
		return body;
	}

	const delta = getCachedWebSocketInputDelta(body, continuation);
	if (!delta || !continuation.lastResponseId) {
		entry.continuation = undefined;
		return body;
	}

	return {
		...body,
		previous_response_id: continuation.lastResponseId,
		input: delta,
	};
}

async function* startWebSocketOutputOnFirstEvent(
	events: AsyncIterable<ResponseStreamEvent>,
	onStart: () => void,
): AsyncGenerator<ResponseStreamEvent> {
	let started = false;
	for await (const event of events) {
		if (!started) {
			started = true;
			onStart();
		}
		yield event;
	}
}

async function processWebSocketStream(
	url: string,
	body: RequestBody,
	headers: Headers,
	output: AssistantMessage,
	stream: AssistantMessageEventStream,
	model: Model<"openai-codex-responses">,
	onStart: () => void,
	idleTimeoutMs: number | undefined,
	websocketConnectTimeoutMs: number | undefined,
	cacheSessionId: string | undefined,
	accountId: string,
	grammarToolInputProperties: ReadonlyMap<string, string>,
	options?: OpenAICodexResponsesOptions,
): Promise<void> {
	const { socket, entry, reused, release } = await acquireWebSocket(
		url,
		headers,
		cacheSessionId,
		accountId,
		options?.signal,
		websocketConnectTimeoutMs,
		options?.env,
	);
	let keepConnection = true;
	const useCachedContext = options?.transport === "websocket-cached" || options?.transport === "auto";
	// ChatGPT Codex Responses rejects `store: true` ("Store must be set to false").
	// WebSocket continuation still works via connection-scoped previous_response_id state.
	const fullBody = body;
	const requestBody = useCachedContext && entry ? buildCachedWebSocketRequestBody(entry, fullBody) : fullBody;
	const stats = cacheSessionId ? getOrCreateWebSocketDebugStats(cacheSessionId) : undefined;
	if (stats) {
		stats.requests++;
		if (reused) stats.connectionsReused++;
		else stats.connectionsCreated++;
		if (useCachedContext) stats.cachedContextRequests++;
		if (requestBody.store === true) stats.storeTrueRequests++;
		stats.lastInputItems = requestBody.input?.length ?? 0;
		if (requestBody.previous_response_id) {
			stats.deltaRequests++;
			stats.lastDeltaInputItems = requestBody.input?.length ?? 0;
			stats.lastPreviousResponseId = requestBody.previous_response_id;
		} else {
			stats.fullContextRequests++;
			stats.lastDeltaInputItems = undefined;
			stats.lastPreviousResponseId = undefined;
		}
	}
	try {
		socket.send(JSON.stringify({ type: "response.create", ...requestBody }));
		await processResponsesStream(
			startWebSocketOutputOnFirstEvent(
				mapCodexEvents(parseWebSocket(socket, options?.signal, idleTimeoutMs)),
				onStart,
			),
			output,
			stream,
			model,
			{
				serviceTier: options?.serviceTier,
				grammarToolInputProperties,
				resolveServiceTier: resolveCodexServiceTier,
				applyServiceTierPricing: (usage, serviceTier) => applyServiceTierPricing(usage, serviceTier, model),
			},
		);
		if (options?.signal?.aborted) {
			keepConnection = false;
		} else if (useCachedContext && entry && output.responseId) {
			const responseItems = convertResponsesMessages(model, { messages: [output] }, CODEX_TOOL_CALL_PROVIDERS, {
				includeSystemPrompt: false,
				grammarToolInputProperties,
			}).filter((item) => item.type !== "function_call_output" && item.type !== "custom_tool_call_output");
			entry.continuation = {
				lastRequestBody: fullBody,
				lastResponseId: output.responseId,
				lastResponseItems: responseItems,
			};
		}
	} catch (error) {
		if (entry) {
			entry.continuation = undefined;
		}
		keepConnection = false;
		throw error;
	} finally {
		release({ keep: keepConnection });
	}
}

// ============================================================================
// Error Handling
// ============================================================================

async function parseErrorResponse(response: Response): Promise<{ message: string; friendlyMessage?: string }> {
	const raw = await response.text();
	let message = raw || response.statusText || "Request failed";
	let friendlyMessage: string | undefined;

	try {
		const parsed = JSON.parse(raw) as {
			error?: { code?: string; type?: string; message?: string; plan_type?: string; resets_at?: number };
		};
		const err = parsed?.error;
		if (err) {
			const code = err.code || err.type || "";
			if (/usage_limit_reached|usage_not_included|rate_limit_exceeded/i.test(code) || response.status === 429) {
				const plan = err.plan_type ? ` (${err.plan_type.toLowerCase()} plan)` : "";
				const mins = err.resets_at
					? Math.max(0, Math.round((err.resets_at * 1000 - Date.now()) / 60000))
					: undefined;
				const when = mins !== undefined ? ` Try again in ~${mins} min.` : "";
				friendlyMessage = `You have hit your ChatGPT usage limit${plan}.${when}`.trim();
			}
			message = err.message || friendlyMessage || message;
		}
	} catch {}

	return { message, friendlyMessage };
}

// ============================================================================
// Auth & Headers
// ============================================================================

function extractAccountId(token: string): string {
	try {
		const parts = token.split(".");
		if (parts.length !== 3) throw new Error("Invalid token");
		const payload = JSON.parse(atob(parts[1]));
		const accountId = payload?.[JWT_CLAIM_PATH]?.chatgpt_account_id;
		if (!accountId) throw new Error("No account ID in token");
		return accountId;
	} catch {
		throw new Error("Failed to extract accountId from token");
	}
}

function buildBaseCodexHeaders(
	initHeaders: Record<string, string> | undefined,
	additionalHeaders: ProviderHeaders | undefined,
	accountId: string,
	token: string,
): Headers {
	const headers = new Headers(initHeaders);
	for (const [key, value] of Object.entries(additionalHeaders || {})) {
		if (value === null) {
			headers.delete(key);
		} else {
			headers.set(key, value);
		}
	}
	headers.set("Authorization", `Bearer ${token}`);
	headers.set("chatgpt-account-id", accountId);
	headers.set("originator", "pi");
	const userAgent = _os ? `pi (${_os.platform()} ${_os.release()}; ${_os.arch()})` : "pi (browser)";
	headers.set("User-Agent", userAgent);
	return headers;
}

function buildSSEHeaders(
	initHeaders: Record<string, string> | undefined,
	additionalHeaders: ProviderHeaders | undefined,
	accountId: string,
	token: string,
	sessionId?: string,
): Headers {
	const headers = buildBaseCodexHeaders(initHeaders, additionalHeaders, accountId, token);
	headers.set("OpenAI-Beta", "responses=experimental");
	headers.set("accept", "text/event-stream");
	headers.set("content-type", "application/json");

	if (sessionId) {
		headers.set("session-id", sessionId);
		headers.set("x-client-request-id", sessionId);
	}

	return headers;
}

function buildWebSocketHeaders(
	initHeaders: Record<string, string> | undefined,
	additionalHeaders: ProviderHeaders | undefined,
	accountId: string,
	token: string,
	requestId: string,
): Headers {
	const headers = buildBaseCodexHeaders(initHeaders, additionalHeaders, accountId, token);
	headers.delete("accept");
	headers.delete("content-type");
	headers.delete("OpenAI-Beta");
	headers.delete("openai-beta");
	headers.set("OpenAI-Beta", OPENAI_BETA_RESPONSES_WEBSOCKETS);
	headers.set("x-client-request-id", requestId);
	headers.set("session-id", requestId);
	return headers;
}
