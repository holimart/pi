import { describe, expect, test } from "vitest";
import { SessionControllerRuntimeAdapter } from "../../src/controller/session-controller-runtime.ts";
import type {
	CommandReceipt,
	ControllerEnvelope,
	ControllerRuntimeEvent,
	ControllerSessionSnapshot,
	SessionControllerProjection,
} from "../../src/controller/types.ts";

class FakeControllerProjection implements SessionControllerProjection {
	readonly submitted: ControllerEnvelope[] = [];
	readonly listeners = new Set<(event: ControllerRuntimeEvent) => void>();
	disposed = false;

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
			revision: 7,
			transcript: [],
			queuedSteer: [],
			queuedSteerCount: 0,
			controllerRevision: 7,
			controllerPhase: "idle",
			queue: [{ commandId: "queued", queueSequence: 3, source: "human", delivery: "start" }],
			rules: [
				{
					id: "r1",
					generation: 1,
					enabled: true,
					trigger: { kind: "agent_settled" },
					action: { target: "agent", kind: "submit", text: "next" },
					maxFirings: 1,
					firedCount: 0,
					priority: "scheduler",
				},
			],
			cursor: "cursor-7",
		};
	}
	getPhase(): "idle" {
		return "idle";
	}
	subscribe(listener: (event: ControllerRuntimeEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}
	async dispose(): Promise<void> {
		this.disposed = true;
	}
	async submit(envelope: ControllerEnvelope): Promise<CommandReceipt> {
		this.submitted.push(envelope);
		return {
			commandId: "command-1",
			sessionId: envelope.sessionId,
			revision: 8,
			accepted: true,
			delivery: "start",
			queueSequence: 4,
		};
	}
}

describe("SessionControllerRuntimeAdapter", () => {
	test("has the server-runtime observation seam and projects controller queue/rule state", async () => {
		const controller = new FakeControllerProjection();
		const runtime = new SessionControllerRuntimeAdapter(controller, "server-derived-actor");
		const events: ControllerRuntimeEvent[] = [];
		const unsubscribe = runtime.subscribe((event) => events.push(event));
		const snapshot = await runtime.snapshot();
		expect(runtime.getPhase()).toBe("idle");
		expect(snapshot).toMatchObject({
			controllerRevision: 7,
			queue: [{ commandId: "queued", queueSequence: 3, source: "human" }],
			rules: [{ id: "r1", enabled: true, maxFirings: 1 }],
			cursor: "cursor-7",
		});
		controller.listeners.forEach((listener) => {
			listener({ type: "snapshot" });
		});
		expect(events).toEqual([{ type: "snapshot" }]);
		await runtime.submit({
			sessionId: "controller-session",
			actorId: "untrusted-client-actor",
			idempotencyKey: "key-1",
			command: { target: "agent", kind: "submit", text: "hello" },
		});
		expect(controller.submitted).toHaveLength(1);
		expect(controller.submitted[0]?.actorId).toBe("server-derived-actor");
		unsubscribe();
		await runtime.dispose();
		expect(controller.disposed).toBe(true);
		// Deliberately no AgentSession inheritance assertion: this is composition.
	});
});
