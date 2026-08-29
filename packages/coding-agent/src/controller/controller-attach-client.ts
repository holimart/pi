import { createConnection, type Socket } from "node:net";
import type { CommandReceipt, ControllerEnvelope, ControllerRuntimeEvent, ControllerSessionSnapshot } from "./types.ts";

type ServerMessage =
	| { type: "controller_snapshot"; snapshot: ControllerSessionSnapshot }
	| { type: "controller_receipt"; receipt: CommandReceipt }
	| { type: "controller_event"; event: ControllerRuntimeEvent }
	| { type: "error"; message: string };

/** Local client for a controller socket. It deliberately serializes submits. */
export class ControllerAttachClient {
	readonly #socket: Socket;
	#snapshot: ControllerSessionSnapshot | undefined;
	#pending: { resolve(receipt: CommandReceipt): void; reject(error: Error): void } | undefined;
	#listeners = new Set<(event: ControllerRuntimeEvent) => void>();
	#pendingText = "";
	#closed = false;

	private constructor(socket: Socket) {
		this.#socket = socket;
		socket.on("data", (chunk: Buffer) => this.#onData(chunk));
		socket.once("error", (error) => this.#fail(error));
		socket.once("close", () => this.#fail(new Error("Controller socket closed")));
	}

	static connect(path: string): Promise<ControllerAttachClient> {
		return new Promise((resolve, reject) => {
			const socket = createConnection(path);
			const client = new ControllerAttachClient(socket);
			const onSnapshot = (_snapshot: ControllerSessionSnapshot): void => {
				client.#listeners.delete(onEvent);
				resolve(client);
			};
			const onEvent = (event: ControllerRuntimeEvent): void => {
				if (event.type === "snapshot" && client.#snapshot) onSnapshot(client.#snapshot);
			};
			client.#listeners.add(onEvent);
			socket.once("error", reject);
			setTimeout(() => reject(new Error("Controller socket did not provide a snapshot")), 5000).unref();
		});
	}

	get snapshot(): ControllerSessionSnapshot {
		if (!this.#snapshot) throw new Error("Controller snapshot is unavailable");
		return this.#snapshot;
	}

	subscribe(listener: (event: ControllerRuntimeEvent) => void): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	submit(envelope: ControllerEnvelope): Promise<CommandReceipt> {
		if (this.#closed) return Promise.reject(new Error("Controller socket is closed"));
		if (this.#pending) return Promise.reject(new Error("Controller client permits one submit at a time"));
		return new Promise<CommandReceipt>((resolve, reject) => {
			this.#pending = { resolve, reject };
			this.#socket.write(`${JSON.stringify({ type: "submit", envelope })}\n`, (error) => {
				if (error) this.#fail(error);
			});
		});
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#socket.destroy();
	}

	#onData(chunk: Buffer): void {
		this.#pendingText += chunk.toString("utf8");
		const lines = this.#pendingText.split("\n");
		this.#pendingText = lines.pop() ?? "";
		for (const line of lines) {
			if (!line) continue;
			let message: ServerMessage;
			try {
				message = JSON.parse(line) as ServerMessage;
			} catch {
				this.#fail(new Error("Controller socket returned invalid JSON"));
				return;
			}
			if (message.type === "controller_snapshot") {
				this.#snapshot = message.snapshot;
				this.#emit({ type: "snapshot" });
			} else if (message.type === "controller_receipt") {
				const pending = this.#pending;
				this.#pending = undefined;
				pending?.resolve(message.receipt);
			} else if (message.type === "controller_event") {
				this.#emit(message.event);
			} else {
				this.#fail(new Error(message.message));
			}
		}
	}

	#emit(event: ControllerRuntimeEvent): void {
		for (const listener of this.#listeners) listener(event);
	}

	#fail(error: Error): void {
		if (this.#closed) return;
		this.#closed = true;
		const pending = this.#pending;
		this.#pending = undefined;
		pending?.reject(error);
		this.#socket.destroy();
	}
}

/** Narrow non-interactive sender; its receipt is the only stdout-worthy result. */
export async function sendControllerEnvelope(path: string, envelope: ControllerEnvelope): Promise<CommandReceipt> {
	const client = await ControllerAttachClient.connect(path);
	try {
		return await client.submit(envelope);
	} finally {
		client.close();
	}
}
