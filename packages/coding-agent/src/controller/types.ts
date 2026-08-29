import type { ModelRef, SessionPhase, SessionSnapshot, ThinkingLevel } from "@earendil-works/pi-protocol";

export type Delivery = "start" | "steer" | "follow_up";
export type ReceiptReason =
	| "not_active"
	| "active_requires_delivery"
	| "invalid"
	| "denied"
	| "idempotency_mismatch"
	| "revision_conflict"
	| "unsupported_lifecycle"
	| "unsafe_active_lifecycle"
	| "backend_unsupported"
	| "stopped"
	| "recovery_required";

export type AgentDirected =
	| { target: "agent"; kind: "submit"; text: string; onActive?: "reject" | "steer" | "follow_up" }
	| { target: "agent"; kind: "send_message"; text: string; requestedDelivery?: "auto" | "steer" | "follow_up" }
	| { target: "agent"; kind: "steer"; text: string }
	| { target: "agent"; kind: "follow_up"; text: string }
	| { target: "agent"; kind: "interrupt" }
	| { target: "agent"; kind: "set_model"; model: ModelRef; expectedRevision: number }
	| { target: "agent"; kind: "set_thinking"; level: ThinkingLevel; expectedRevision: number }
	| {
			target: "agent";
			kind: "lifecycle";
			action: "compact" | "fork" | "switch" | "new_session";
			expectedRevision: number;
	  };

export type ControllerDirected =
	| { target: "controller"; kind: "scheduler.pause" | "scheduler.resume" }
	| { target: "controller"; kind: "scheduler.stop" }
	| { target: "controller"; kind: "scheduler.set_limit"; ruleId: string; maxFirings: number }
	| { target: "controller"; kind: "queue.inspect" };

export type ControllerEnvelope = {
	sessionId: string;
	actorId: string; // server-derived, never trusted from a client body
	idempotencyKey: string;
	command: AgentDirected | ControllerDirected;
};

export interface CommandReceipt {
	commandId: string;
	sessionId: string;
	revision: number; // controller revision after recording this result
	accepted: boolean;
	delivery?: Delivery; // present only for accepted agent-directed content
	queueSequence?: number; // present only for accepted agent-directed content
	emulated?: boolean; // true when a capability matrix E path was selected
	backend?: string; // present on backend_unsupported
	command?: AgentDirected["kind"]; // present on backend_unsupported
	reason?: ReceiptReason; // present only when accepted is false
}

export interface CommandSettlement {
	commandId: string;
	queueSequence?: number;
	state: "delivered" | "settled" | "cancelled" | "recovery_required";
	outcome?: "accepted_by_agent" | "agent_settled" | "aborted" | "emulated_safe_turn";
	causedBy?: { ruleId: string; generation: number; firing: number };
}

export interface ScheduleRule {
	id: string;
	generation: number;
	enabled: boolean;
	trigger: { kind: "agent_settled" | "at" | "idle_for" };
	action: Extract<AgentDirected, { kind: "submit" }>;
	maxFirings: number;
	firedCount: number;
	priority: "scheduler";
}

export interface ControllerQueueEntry {
	commandId: string;
	queueSequence: number;
	source: "human" | "scheduler";
	delivery: Delivery;
}

/**
 * A successor of the protocol's SessionSnapshot. The base fields preserve the
 * attachment rendering contract; controller fields are authoritative state.
 */
export interface ControllerSessionSnapshot extends SessionSnapshot {
	controllerRevision: number;
	controllerPhase: "idle" | "active" | "recovering" | "stopped";
	queue: readonly ControllerQueueEntry[];
	rules: readonly ScheduleRule[];
	cursor: string;
}

export type ControllerRuntimeEvent =
	| { type: "snapshot" }
	| { type: "receipt"; receipt: CommandReceipt }
	| { type: "settlement"; settlement: CommandSettlement }
	| { type: "error"; error: Error };

/**
 * The only seam required from the composed SessionController implementation.
 * It intentionally does not mention AgentSession or inherit from it.
 */
export interface SessionControllerProjection {
	snapshot(): ControllerSessionSnapshot | Promise<ControllerSessionSnapshot>;
	getPhase(): SessionPhase;
	subscribe(listener: (event: ControllerRuntimeEvent) => void): () => void;
	dispose(): Promise<void>;
	submit(envelope: ControllerEnvelope): Promise<CommandReceipt>;
}
