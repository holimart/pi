import type { SessionPhase } from "@earendil-works/pi-protocol";
import type {
	CommandReceipt,
	ControllerEnvelope,
	ControllerRuntimeEvent,
	ControllerSessionSnapshot,
	SessionControllerProjection,
} from "./types.ts";

/**
 * Composition adapter for controller attachment. It has the observation shape
 * of PiSessionRuntime, plus its deliberately structured submit path. It is
 * not, and must never become, an AgentSession subclass.
 */
export class SessionControllerRuntimeAdapter {
	readonly #controller: SessionControllerProjection;
	readonly #actorId: string;
	#disposed = false;

	constructor(controller: SessionControllerProjection, actorId: string) {
		if (!actorId) throw new TypeError("Controller actor ID must not be empty");
		this.#controller = controller;
		this.#actorId = actorId;
	}

	snapshot(): Promise<ControllerSessionSnapshot> {
		this.#assertOpen();
		return Promise.resolve(this.#controller.snapshot());
	}

	getPhase(): SessionPhase {
		this.#assertOpen();
		return this.#controller.getPhase();
	}

	subscribe(listener: (event: ControllerRuntimeEvent) => void): () => void {
		this.#assertOpen();
		return this.#controller.subscribe(listener);
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		await this.#controller.dispose();
	}

	/**
	 * `actorId` in a client body is intentionally overwritten. Only the local
	 * listener creates adapters and assigns their ephemeral actor IDs.
	 */
	submit(envelope: ControllerEnvelope): Promise<CommandReceipt> {
		this.#assertOpen();
		return this.#controller.submit({ ...envelope, actorId: this.#actorId });
	}

	#assertOpen(): void {
		if (this.#disposed) throw new Error("Controller runtime adapter is disposed");
	}
}
