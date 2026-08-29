import { mkdtemp, rm } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test } from "vitest";
import { ControllerAttachClient } from "../../src/controller/controller-attach-client.ts";
import type {
	CommandReceipt,
	ControllerEnvelope,
	ControllerRuntimeEvent,
	ControllerSessionSnapshot,
	SessionControllerProjection,
} from "../../src/controller/types.ts";
import { UnixControllerListener } from "../../src/controller/unix-controller-listener.ts";

class FakeController implements SessionControllerProjection {
	readonly submitted: ControllerEnvelope[] = [];
	snapshot(): ControllerSessionSnapshot {
		return {
			id: "controller-session",
			cwd: "/work",
			createdAt: 1,
			updatedAt: 1,
			phase: "idle",
			model: { provider: "test", id: "model" },
			thinkingLevel: "off",
			attached: true,
			locked: false,
			revision: 0,
			transcript: [],
			queuedSteer: [],
			queuedSteerCount: 0,
			controllerRevision: 0,
			controllerPhase: "idle",
			queue: [],
			rules: [],
			cursor: "cursor-0",
		};
	}
	getPhase(): "idle" {
		return "idle";
	}
	subscribe(_listener: (event: ControllerRuntimeEvent) => void): () => void {
		return () => {};
	}
	async dispose(): Promise<void> {}
	async submit(envelope: ControllerEnvelope): Promise<CommandReceipt> {
		this.submitted.push(envelope);
		return {
			commandId: "command-1",
			sessionId: envelope.sessionId,
			revision: 1,
			accepted: true,
			delivery: "start",
			queueSequence: 1,
		};
	}
}

const directories = new Set<string>();
const listeners = new Set<UnixControllerListener>();
afterEach(async () => {
	await Promise.all([...listeners].map((listener) => listener.close()));
	listeners.clear();
	await Promise.all([...directories].map((directory) => rm(directory, { recursive: true, force: true })));
	directories.clear();
});

describe("UnixControllerListener", () => {
	test("rejects a foreign UID before snapshot exposure or command admission", async () => {
		const directory = await mkdtemp(join(tmpdir(), "pi-controller-"));
		directories.add(directory);
		const ownerUid = process.getuid?.();
		if (ownerUid === undefined) throw new Error("Unix UID is unavailable in this test runtime");
		const controller = new FakeController();
		let reportedUid = ownerUid;
		const listener = new UnixControllerListener({
			path: join(directory, "controller.sock"),
			controller,
			ownerUid,
			peerUid: () => reportedUid,
		});
		listeners.add(listener);
		await listener.start();

		const owner = await ControllerAttachClient.connect(listener.path);
		const receipt = await owner.submit({
			sessionId: "controller-session",
			actorId: "forged",
			idempotencyKey: "owner-key",
			command: { target: "agent", kind: "submit", text: "owner input" },
		});
		expect(receipt.accepted).toBe(true);
		expect(controller.submitted[0]?.actorId).not.toBe("forged");
		owner.close();

		reportedUid = ownerUid + 1;
		const foreign = createConnection(listener.path);
		const received = await new Promise<string>((resolve) => {
			let data = "";
			foreign.on("data", (chunk: Buffer) => {
				data += chunk.toString("utf8");
			});
			foreign.once("error", () => resolve(data));
			foreign.once("close", () => resolve(data));
		});
		expect(received).toBe("");
		expect(controller.submitted).toHaveLength(1);
	});
});
