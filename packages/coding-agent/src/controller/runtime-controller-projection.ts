import type { TranscriptItem, UserTranscriptItem } from "@earendil-works/pi-protocol";
import type { AgentSessionRuntime } from "../core/agent-session-runtime.ts";
import type {
	CommandReceipt,
	CommandSettlement,
	ControllerEnvelope,
	ControllerQueueEntry,
	ControllerRuntimeEvent,
	ControllerSessionSnapshot,
	Delivery,
	ReceiptReason,
	SessionControllerProjection,
} from "./types.ts";

/**
 * The live controller seam for the coding-agent runtime. It deliberately only
 * admits commands and projects state; policy, scheduling, and LLM work remain
 * in the AgentSession.
 */
export class RuntimeControllerProjection implements SessionControllerProjection {
	readonly #runtimeHost: AgentSessionRuntime;
	readonly #createdAt = Date.now();
	readonly #listeners = new Set<(event: ControllerRuntimeEvent) => void>();
	readonly #idempotency = new Map<string, { fingerprint: string; receipt: CommandReceipt }>();
	readonly #queue = new Map<string, ControllerQueueEntry>();
	#queuedSteer: readonly string[] = [];
	#unsubscribeSession: (() => void) | undefined;
	#revision = 0;
	#nextQueueSequence = 0;
	#disposed = false;

	constructor(runtimeHost: AgentSessionRuntime) {
		this.#runtimeHost = runtimeHost;
		this.rebindSession();
	}

	snapshot(): ControllerSessionSnapshot {
		const session = this.#runtimeHost.session;
		const now = Date.now();
		return {
			id: session.sessionId,
			name: session.sessionName,
			cwd: this.#runtimeHost.cwd,
			createdAt: this.#createdAt,
			updatedAt: now,
			phase: this.getPhase(),
			model: { provider: session.model?.provider ?? "unknown", id: session.model?.id ?? "unknown" },
			thinkingLevel: session.thinkingLevel,
			attached: true,
			locked: false,
			revision: this.#revision,
			transcript: this.#transcript(),
			queuedSteer: this.#queuedSteer.map((text, index) => ({
				id: `queued-steer-${index + 1}`,
				role: "user",
				content: [{ type: "text", text }],
				timestamp: now,
			})) as UserTranscriptItem[],
			queuedSteerCount: this.#queuedSteer.length,
			controllerRevision: this.#revision,
			controllerPhase: this.#controllerPhase(),
			queue: [...this.#queue.values()],
			rules: [],
			cursor: `${session.sessionId}:${this.#revision}`,
		};
	}

	getPhase(): "idle" | "turn" | "compaction" {
		const session = this.#runtimeHost.session;
		if (session.isCompacting) return "compaction";
		return session.isStreaming ? "turn" : "idle";
	}

	subscribe(listener: (event: ControllerRuntimeEvent) => void): () => void {
		this.#assertOpen();
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#unsubscribeSession?.();
		this.#unsubscribeSession = undefined;
		this.#listeners.clear();
	}

	/** Reattach observation after AgentSessionRuntime replaces its session. */
	rebindSession(): void {
		if (this.#disposed) return;
		this.#unsubscribeSession?.();
		this.#unsubscribeSession = this.#runtimeHost.session.subscribe((event) => {
			if (event.type === "queue_update") this.#queuedSteer = event.steering;
			if (event.type === "agent_settled") this.#settleQueuedCommands();
			this.#emit({ type: "snapshot" });
		});
	}

	async submit(envelope: ControllerEnvelope): Promise<CommandReceipt> {
		this.#assertOpen();
		const idempotencyId = `${envelope.actorId}:${envelope.idempotencyKey}`;
		const fingerprint = JSON.stringify({ sessionId: envelope.sessionId, command: envelope.command });
		const previous = this.#idempotency.get(idempotencyId);
		if (previous) {
			return previous.fingerprint === fingerprint
				? previous.receipt
				: this.#reject(envelope, "idempotency_mismatch");
		}

		if (envelope.sessionId !== this.#runtimeHost.session.sessionId) return this.#reject(envelope, "invalid");
		if (envelope.command.target !== "agent") return this.#reject(envelope, "unsupported_lifecycle");

		const command = envelope.command;
		let delivery: Delivery | undefined;
		try {
			switch (command.kind) {
				case "submit":
					if (!this.#hasText(command.text)) return this.#reject(envelope, "invalid");
					// An active submit is always a follow-up. In particular, do not
					// turn the caller's optional reject preference into command_rejected.
					delivery = this.getPhase() === "idle" ? "start" : "follow_up";
					await this.#deliver(command.text, delivery);
					break;
				case "send_message":
					if (!this.#hasText(command.text)) return this.#reject(envelope, "invalid");
					delivery = this.#deliveryForMessage(command.requestedDelivery);
					await this.#deliver(command.text, delivery);
					break;
				case "steer":
					if (!this.#hasText(command.text) || this.getPhase() === "idle") return this.#reject(envelope, "not_active");
					delivery = "steer";
					await this.#runtimeHost.session.steer(command.text);
					break;
				case "follow_up":
					if (!this.#hasText(command.text) || this.getPhase() === "idle") return this.#reject(envelope, "not_active");
					delivery = "follow_up";
					await this.#runtimeHost.session.followUp(command.text);
					break;
				case "interrupt":
					if (this.getPhase() === "idle") return this.#reject(envelope, "not_active");
					await this.#runtimeHost.session.abort();
					break;
				default:
					return this.#reject(envelope, "unsupported_lifecycle");
			}
		} catch (error) {
			this.#emit({ type: "error", error: error instanceof Error ? error : new Error(String(error)) });
			return this.#reject(envelope, "invalid");
		}

		const receipt: CommandReceipt = {
			commandId: `controller-${++this.#revision}`,
			sessionId: envelope.sessionId,
			revision: this.#revision,
			accepted: true,
			delivery,
			queueSequence: delivery === undefined ? undefined : ++this.#nextQueueSequence,
		};
		this.#idempotency.set(idempotencyId, { fingerprint, receipt });
		if (delivery !== undefined && receipt.queueSequence !== undefined) {
			this.#queue.set(receipt.commandId, {
				commandId: receipt.commandId,
				queueSequence: receipt.queueSequence,
				source: "human",
				delivery,
			});
		}
		this.#emit({ type: "receipt", receipt });
		this.#emit({ type: "snapshot" });
		return receipt;
	}

	async #deliver(text: string, delivery: Delivery): Promise<void> {
		if (delivery === "start") {
			// prompt() resolves when the turn settles. Admission must not wait for
			// a model response, exactly like RPC mode's prompt command.
			void this.#runtimeHost.session
				.prompt(text, { streamingBehavior: "followUp", source: "rpc" })
				.catch((error: unknown) =>
					this.#emit({ type: "error", error: error instanceof Error ? error : new Error(String(error)) }),
				);
			return;
		}
		if (delivery === "steer") return this.#runtimeHost.session.steer(text);
		return this.#runtimeHost.session.followUp(text);
	}

	#deliveryForMessage(requested: "auto" | "steer" | "follow_up" | undefined): Delivery {
		if (this.getPhase() === "idle") return "start";
		return requested === "steer" ? "steer" : "follow_up";
	}

	#transcript(): TranscriptItem[] {
		return this.#runtimeHost.session.sessionManager
			.getEntries()
			.flatMap((entry): TranscriptItem[] => {
				if (entry.type !== "message") return [];
				const message = entry.message;
				if (message.role === "custom") return [];
				return [
					{
						...message,
						id: entry.id,
						timestamp: Date.parse(entry.timestamp),
						...(message.role === "assistant"
							? {
									model: { provider: message.provider, id: message.model },
									status: message.stopReason === "aborted" ? "aborted" : message.stopReason === "error" ? "error" : "complete",
								}
							: {}),
					} as unknown as TranscriptItem,
				];
			});
	}

	#settleQueuedCommands(): void {
		for (const entry of this.#queue.values()) {
			const settlement: CommandSettlement = {
				commandId: entry.commandId,
				queueSequence: entry.queueSequence,
				state: "settled",
				outcome: "agent_settled",
			};
			this.#queue.delete(entry.commandId);
			this.#emit({ type: "settlement", settlement });
		}
	}

	#reject(envelope: ControllerEnvelope, reason: ReceiptReason): CommandReceipt {
		return {
			commandId: `controller-${++this.#revision}`,
			sessionId: envelope.sessionId,
			revision: this.#revision,
			accepted: false,
			reason,
		};
	}

	#controllerPhase(): "idle" | "active" | "recovering" | "stopped" {
		if (this.#disposed) return "stopped";
		return this.getPhase() === "idle" ? "idle" : "active";
	}

	#hasText(text: string): boolean {
		return text.trim().length > 0;
	}

	#emit(event: ControllerRuntimeEvent): void {
		for (const listener of this.#listeners) listener(event);
	}

	#assertOpen(): void {
		if (this.#disposed) throw new Error("Runtime controller projection is disposed");
	}
}
