import { existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Agent } from "@earendil-works/pi-agent-core";
import {
	type AssistantMessage,
	type AssistantMessageEvent,
	EventStream,
	getModel,
	type Model,
} from "@earendil-works/pi-ai/compat";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentSession, type AgentSessionEvent } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { AuthStorage } from "../src/core/auth-storage.ts";
import { SessionManager } from "../src/core/session-manager.ts";
import { SettingsManager } from "../src/core/settings-manager.ts";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";
import { runRpcMode } from "../src/modes/rpc/rpc-mode.ts";
import { createInMemoryModelRegistry, getModelRuntime } from "./model-runtime-test-utils.ts";
import { createTestResourceLoader } from "./utilities.ts";

const rpcIo = vi.hoisted(() => ({
	outputLines: [] as string[],
	lineHandler: undefined as ((line: string) => void) | undefined,
}));

vi.mock("../src/core/output-guard.js", () => ({
	flushRawStdout: vi.fn(async () => {}),
	takeOverStdout: vi.fn(),
	waitForRawStdoutBackpressure: vi.fn(async () => {}),
	writeRawStdout: (line: string) => {
		rpcIo.outputLines.push(line);
	},
}));

vi.mock("../src/modes/interactive/theme/theme.js", () => ({ theme: {} }));

vi.mock("../src/modes/rpc/jsonl.js", () => ({
	attachJsonlLineReader: vi.fn((_stream: NodeJS.ReadableStream, onLine: (line: string) => void) => {
		rpcIo.lineHandler = onLine;
		return () => {};
	}),
	serializeJsonLine: (value: unknown) => `${JSON.stringify(value)}\n`,
}));

class MockAssistantStream extends EventStream<AssistantMessageEvent, AssistantMessage> {
	constructor() {
		super(
			(event) => event.type === "done" || event.type === "error",
			(event) => {
				if (event.type === "done") return event.message;
				if (event.type === "error") return event.error;
				throw new Error("Unexpected event type");
			},
		);
	}
}

function createAssistantMessage(text: string): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text }],
		api: "anthropic-messages",
		provider: "anthropic",
		model: "claude-sonnet-4-5",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

type ParsedOutputLine = Record<string, unknown>;

function parseOutputLines(outputLines: string[]): ParsedOutputLine[] {
	return outputLines
		.flatMap((line) => line.split("\n"))
		.filter((line) => line.trim().length > 0)
		.map((line) => JSON.parse(line) as ParsedOutputLine);
}

function getPromptResponses(outputLines: string[], id: string): ParsedOutputLine[] {
	return parseOutputLines(outputLines).filter(
		(record) => record.id === id && record.type === "response" && record.command === "prompt",
	);
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createRuntimeHost(options: { withAuth: boolean; responseDelayMs: number; model?: Model<any> }): Promise<{
	runtimeHost: AgentSessionRuntime;
	session: AgentSession;
	cleanup: () => Promise<void>;
}> {
	const tempDir = join(tmpdir(), `pi-rpc-prompt-${Date.now()}-${Math.random().toString(36).slice(2)}`);
	mkdirSync(tempDir, { recursive: true });

	const model = options.model ?? getModel("anthropic", "claude-sonnet-4-5");
	if (!model) {
		throw new Error("Test model not found");
	}

	const agent = new Agent({
		getApiKey: () => "test-key",
		initialState: {
			model,
			systemPrompt: "Test",
			tools: [],
		},
		streamFn: (_model, _context, _options) => {
			const stream = new MockAssistantStream();
			queueMicrotask(() => {
				stream.push({ type: "start", partial: createAssistantMessage("") });
				setTimeout(() => {
					stream.push({ type: "done", reason: "stop", message: createAssistantMessage("done") });
				}, options.responseDelayMs);
			});
			return stream;
		},
	});

	const sessionManager = SessionManager.inMemory();
	const settingsManager = SettingsManager.create(tempDir, tempDir);
	const authStorage = AuthStorage.create(join(tempDir, "auth.json"));
	const modelRegistry = await createInMemoryModelRegistry(authStorage);
	if (options.withAuth) {
		await authStorage.modify("anthropic", async () => ({ type: "api_key", key: "test-key" }));
	}

	const session = new AgentSession({
		agent,
		sessionManager,
		settingsManager,
		cwd: tempDir,
		modelRuntime: getModelRuntime(modelRegistry),
		resourceLoader: createTestResourceLoader(),
	});

	const runtimeHost = {
		session,
		newSession: vi.fn(async () => ({ cancelled: true })),
		switchSession: vi.fn(async () => ({ cancelled: true })),
		fork: vi.fn(async () => ({ cancelled: true, selectedText: "" })),
		dispose: vi.fn(async () => {}),
		setRebindSession: vi.fn(),
	} as unknown as AgentSessionRuntime;

	return {
		runtimeHost,
		session,
		cleanup: async () => {
			try {
				if (session.isStreaming) {
					await session.abort();
				}
			} catch {
				// ignore test cleanup failures
			}
			session.dispose();
			if (existsSync(tempDir)) {
				rmSync(tempDir, { recursive: true });
			}
		},
	};
}

async function startRpcMode(options: { withAuth: boolean; responseDelayMs: number; model?: Model<any> }): Promise<{
	lineHandler: (line: string) => void;
	session: AgentSession;
	cleanup: () => Promise<void>;
}> {
	rpcIo.outputLines = [];
	rpcIo.lineHandler = undefined;

	const { runtimeHost, session, cleanup } = await createRuntimeHost(options);
	void runRpcMode(runtimeHost);
	await vi.waitFor(() => expect(rpcIo.lineHandler).toBeDefined());

	return { lineHandler: rpcIo.lineHandler!, session, cleanup };
}

async function sendRpcCommand(
	lineHandler: (line: string) => void,
	command: Record<string, unknown>,
): Promise<ParsedOutputLine> {
	lineHandler(JSON.stringify(command));
	await vi.waitFor(() => {
		const response = parseOutputLines(rpcIo.outputLines).find(
			(record) => record.type === "response" && record.id === command.id,
		);
		expect(response).toBeDefined();
	});
	return parseOutputLines(rpcIo.outputLines).find((record) => record.type === "response" && record.id === command.id)!;
}

type InteractiveSubmitContext = {
	defaultEditor: { onSubmit?: (text: string) => Promise<void> };
	editor: { addToHistory?: (text: string) => void; setText: (text: string) => void };
	session: AgentSession;
	flushPendingBashComponents: () => void;
	onInputCallback?: (text: string) => void;
	pendingUserInputs: string[];
};

type InteractiveInputContext = Pick<InteractiveSubmitContext, "onInputCallback" | "pendingUserInputs">;

type InteractiveModePrivate = {
	setupEditorSubmitHandler(this: InteractiveSubmitContext): void;
	getUserInput(this: InteractiveInputContext): Promise<string>;
};

const interactiveModePrototype = InteractiveMode.prototype as unknown as InteractiveModePrivate;

describe("RPC prompt response semantics", () => {
	afterEach(() => {
		rpcIo.outputLines = [];
		rpcIo.lineHandler = undefined;
	});

	it("emits one failure response when prompt preflight rejects", async () => {
		const { lineHandler, cleanup } = await startRpcMode({
			withAuth: false,
			responseDelayMs: 0,
			model: {
				id: "fake-model",
				name: "Fake Model",
				api: "openai-completions",
				provider: "fake-provider",
				baseUrl: "https://example.invalid",
				reasoning: false,
				input: [],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 0,
				maxTokens: 0,
			},
		});

		try {
			lineHandler(JSON.stringify({ id: "b1", type: "prompt", message: "Hello" }));

			await vi.waitFor(() => {
				const responses = getPromptResponses(rpcIo.outputLines, "b1");
				expect(responses).toHaveLength(1);
				expect(responses[0]).toMatchObject({
					id: "b1",
					type: "response",
					command: "prompt",
					success: false,
					error: expect.stringContaining(
						"No API key found for fake-provider.\n\nUse /login to log into a provider via OAuth or API key. See:",
					),
				});
			});
		} finally {
			await cleanup();
		}
	});

	it("emits one success response when prompt preflight succeeds", async () => {
		const { lineHandler, cleanup } = await startRpcMode({ withAuth: true, responseDelayMs: 0 });

		try {
			lineHandler(JSON.stringify({ id: "b2", type: "prompt", message: "Hello" }));

			await vi.waitFor(() => {
				const responses = getPromptResponses(rpcIo.outputLines, "b2");
				expect(responses).toHaveLength(1);
				expect(responses[0]).toMatchObject({
					id: "b2",
					type: "response",
					command: "prompt",
					success: true,
				});
			});
		} finally {
			await cleanup();
		}
	});

	it("emits one success response when prompt is queued during streaming", async () => {
		const { lineHandler, cleanup } = await startRpcMode({ withAuth: true, responseDelayMs: 100 });

		try {
			lineHandler(JSON.stringify({ id: "b3-start", type: "prompt", message: "Start" }));
			await vi.waitFor(() => {
				expect(getPromptResponses(rpcIo.outputLines, "b3-start")).toHaveLength(1);
			});

			rpcIo.outputLines = [];
			lineHandler(
				JSON.stringify({
					id: "b3",
					type: "prompt",
					message: "Queue this",
					streamingBehavior: "followUp",
				}),
			);

			await vi.waitFor(() => {
				const responses = getPromptResponses(rpcIo.outputLines, "b3");
				expect(responses).toHaveLength(1);
				expect(responses[0]).toMatchObject({
					id: "b3",
					type: "response",
					command: "prompt",
					success: true,
				});
			});

			await sleep(150);
		} finally {
			await cleanup();
		}
	});

	it("matches a TUI-submitted prompt for events, state, and history reads", async () => {
		const tui = await createRuntimeHost({ withAuth: true, responseDelayMs: 0 });
		const rpc = await startRpcMode({ withAuth: true, responseDelayMs: 0 });
		const tuiEvents: AgentSessionEvent[] = [];
		const unsubscribe = tui.session.subscribe((event) => tuiEvents.push(event));
		const tuiInput: InteractiveSubmitContext = {
			defaultEditor: {},
			editor: { addToHistory: vi.fn(), setText: vi.fn() },
			session: tui.session,
			flushPendingBashComponents: vi.fn(),
			pendingUserInputs: [],
		};

		try {
			interactiveModePrototype.setupEditorSubmitHandler.call(tuiInput);
			const submittedInput = interactiveModePrototype.getUserInput.call(tuiInput);
			await tuiInput.defaultEditor.onSubmit?.("Parity prompt");
			await tui.session.prompt(await submittedInput);

			await sendRpcCommand(rpc.lineHandler, { id: "prompt", type: "prompt", message: "Parity prompt" });
			await vi.waitFor(() => {
				expect(parseOutputLines(rpcIo.outputLines).some((record) => record.type === "agent_settled")).toBe(true);
			});

			const [state, entries, messages, lastAssistantText] = await Promise.all([
				sendRpcCommand(rpc.lineHandler, { id: "state", type: "get_state" }),
				sendRpcCommand(rpc.lineHandler, { id: "entries", type: "get_entries" }),
				sendRpcCommand(rpc.lineHandler, { id: "messages", type: "get_messages" }),
				sendRpcCommand(rpc.lineHandler, { id: "last-assistant", type: "get_last_assistant_text" }),
			]);

			const rpcEventTypes = parseOutputLines(rpcIo.outputLines)
				.filter((record) => record.type !== "response")
				.map((record) => record.type);
			expect(rpcEventTypes).toEqual(tuiEvents.map((event) => event.type));

			expect(state.data).toMatchObject({
				model: { provider: "anthropic", id: "claude-sonnet-4-5" },
				isStreaming: tui.session.isStreaming,
				isCompacting: tui.session.isCompacting,
				steeringMode: tui.session.steeringMode,
				followUpMode: tui.session.followUpMode,
				messageCount: tui.session.messages.length,
				pendingMessageCount: tui.session.pendingMessageCount,
			});
			expect(entries.data).toMatchObject({
				entries: tui.session.sessionManager.getEntries().map((entry) => ({ type: entry.type })),
			});
			expect(messages.data).toMatchObject({
				messages: tui.session.messages.map((message) => ({
					role: message.role,
					...("content" in message ? { content: message.content } : {}),
				})),
			});
			expect(lastAssistantText.data).toEqual({ text: tui.session.getLastAssistantText() });
		} finally {
			unsubscribe();
			await Promise.all([tui.cleanup(), rpc.cleanup]);
		}
	});
});
