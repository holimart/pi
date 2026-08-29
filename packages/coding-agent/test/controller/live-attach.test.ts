import { mkdtemp, rm } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test, vi } from "vitest";
import type { AgentSessionRuntime } from "../../src/core/agent-session-runtime.ts";
import { ControllerAttachClient } from "../../src/controller/controller-attach-client.ts";
import type { ControllerRuntimeEvent } from "../../src/controller/types.ts";
import { runRpcMode } from "../../src/modes/rpc/rpc-mode.ts";

vi.mock("../../src/core/output-guard.js", () => ({
	flushRawStdout: vi.fn(async () => {}),
	takeOverStdout: vi.fn(),
	waitForRawStdoutBackpressure: vi.fn(async () => {}),
	writeRawStdout: vi.fn(),
}));

vi.mock("../../src/modes/rpc/jsonl.js", () => ({
	attachJsonlLineReader: vi.fn(() => () => {}),
	serializeJsonLine: (value: unknown) => `${JSON.stringify(value)}\n`,
}));

class ActiveSession {
	readonly sessionId = "live-controller-session";
	readonly sessionName = "live controller";
	readonly model = { provider: "faux", id: "faux-model" };
	readonly thinkingLevel = "off" as const;
	readonly sessionManager = { getEntries: () => [] };
	readonly agent = { subscribe: () => () => {} };
	readonly listeners = new Set<(event: { type: "agent_settled" }) => void>();
	isStreaming = true;
	isCompacting = false;
	followUps: string[] = [];

	subscribe(listener: (event: { type: "agent_settled" }) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async followUp(text: string): Promise<void> {
		this.followUps.push(text);
		setTimeout(() => {
			this.isStreaming = false;
			for (const listener of this.listeners) listener({ type: "agent_settled" });
		}, 0);
	}

	async bindExtensions(_options: unknown): Promise<void> {}
	async steer(_text: string): Promise<void> {}
	async abort(): Promise<void> {}
	async prompt(_text: string): Promise<void> {}
}

const directories = new Set<string>();
afterEach(async () => {
	await Promise.all([...directories].map((directory) => rm(directory, { recursive: true, force: true })));
	directories.clear();
});

describe("live RPC controller attachment", () => {
	test("admits an active send_message as a follow-up, settles it, and rejects a foreign UID", async () => {
		const ownerUid = process.getuid?.();
		if (ownerUid === undefined) throw new Error("Unix UID is unavailable in this test runtime");
		const directory = await mkdtemp(join(tmpdir(), "pi-controller-live-"));
		directories.add(directory);
		let peerUid = ownerUid;
		const session = new ActiveSession();
		let disposed = false;
		const runtimeHost = {
			session,
			cwd: directory,
			setRebindSession: () => {},
			dispose: async () => {
				disposed = true;
			},
		} as unknown as AgentSessionRuntime;
		const socketPath = join(directory, "controller.sock");
		void runRpcMode(runtimeHost, { controllerSocketPath: socketPath, controllerPeerUid: () => peerUid });
		let client: ControllerAttachClient | undefined;
		await vi.waitFor(async () => {
			client = await ControllerAttachClient.connect(socketPath);
		});
		const events: ControllerRuntimeEvent[] = [];
		client!.subscribe((event) => events.push(event));
		const receipt = await client!.submit({
			sessionId: client!.snapshot.id,
			actorId: "forged-client-actor",
			idempotencyKey: "active-send-message",
			command: { target: "agent", kind: "send_message", text: "deliver after this turn", requestedDelivery: "auto" },
		});
		expect(receipt).toMatchObject({ accepted: true, delivery: "follow_up" });
		expect(session.followUps).toEqual(["deliver after this turn"]);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(events).toContainEqual(
			expect.objectContaining({
				type: "settlement",
				settlement: expect.objectContaining({ commandId: receipt.commandId, state: "settled" }),
			}),
		);
		client!.close();

		peerUid = ownerUid + 1;
		const foreign = createConnection(socketPath);
		const exposed = await new Promise<string>((resolve) => {
			let data = "";
			foreign.on("data", (chunk: Buffer) => {
				data += chunk.toString("utf8");
			});
			foreign.once("error", () => resolve(data));
			foreign.once("close", () => resolve(data));
		});
		expect(exposed).toBe("");

		const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
		try {
			process.stdin.emit("end");
			await vi.waitFor(() => expect(disposed).toBe(true));
		} finally {
			exit.mockRestore();
		}
	});
});
