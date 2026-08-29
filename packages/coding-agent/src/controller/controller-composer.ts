import { randomUUID } from "node:crypto";
import type { Input } from "@earendil-works/pi-tui";
import type { SessionControllerRuntimeAdapter } from "./session-controller-runtime.ts";
import type { CommandReceipt, ControllerDirected, ControllerEnvelope } from "./types.ts";

/**
 * The local TUI composer always emits structured envelopes. Plain text is
 * agent-directed text; controller operations require the separate explicit
 * method and can never arise from parsing a prompt.
 */
export class ControllerComposer {
	readonly #runtime: SessionControllerRuntimeAdapter;
	readonly #sessionId: string;
	readonly #idempotencyKey: () => string;

	constructor(runtime: SessionControllerRuntimeAdapter, sessionId: string, idempotencyKey = randomUUID) {
		this.#runtime = runtime;
		this.#sessionId = sessionId;
		this.#idempotencyKey = idempotencyKey;
	}

	submitText(text: string, onActive: "reject" | "steer" | "follow_up" = "reject"): Promise<CommandReceipt> {
		return this.#runtime.submit(this.#envelope({ target: "agent", kind: "submit", text, onActive }));
	}

	submitController(command: ControllerDirected): Promise<CommandReceipt> {
		return this.#runtime.submit(this.#envelope(command));
	}

	#envelope(command: ControllerEnvelope["command"]): ControllerEnvelope {
		return {
			sessionId: this.#sessionId,
			// The adapter replaces this placeholder with its server-derived actor.
			actorId: "client-body-is-not-authority",
			idempotencyKey: this.#idempotencyKey(),
			command,
		};
	}
}

/** Reuses Pi's existing Input component while keeping admission in the adapter. */
export function installControllerComposer(
	input: Input,
	composer: ControllerComposer,
	onReceipt: (receipt: CommandReceipt) => void,
	onError: (error: Error) => void,
): void {
	input.onSubmit = (text) => {
		if (!text.trim()) return;
		input.setValue("");
		void composer
			.submitText(text)
			.then(onReceipt, (error: unknown) => onError(error instanceof Error ? error : new Error(String(error))));
	};
}
