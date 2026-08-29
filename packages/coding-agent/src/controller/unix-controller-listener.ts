import { randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, rm } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import { dirname } from "node:path";
import { SessionControllerRuntimeAdapter } from "./session-controller-runtime.ts";
import type {
	CommandReceipt,
	ControllerEnvelope,
	ControllerRuntimeEvent,
	SessionControllerProjection,
} from "./types.ts";

export type PeerUidReader = (socket: Socket) => number | Promise<number>;

export interface UnixControllerListenerOptions {
	path: string;
	controller: SessionControllerProjection;
	/** A platform bridge over SO_PEERCRED/getpeereid. Absence is fail-closed. */
	peerUid: PeerUidReader;
	ownerUid?: number;
}

type ClientMessage = { type: "submit"; envelope: ControllerEnvelope };
type ServerMessage =
	| { type: "controller_snapshot"; snapshot: unknown }
	| { type: "controller_receipt"; receipt: CommandReceipt }
	| { type: "controller_event"; event: ControllerRuntimeEvent }
	| { type: "error"; message: string };

/**
 * Same-UID-only local controller ingress. The Node standard library does not
 * expose Unix peer credentials, so callers must provide a native platform
 * bridge. A connection without verified credentials is closed before it can
 * receive a snapshot or submit a command.
 */
export class UnixControllerListener {
	readonly #path: string;
	readonly #controller: SessionControllerProjection;
	readonly #peerUid: PeerUidReader;
	readonly #ownerUid: number;
	readonly #server: Server;
	readonly #sockets = new Set<Socket>();
	#unsubscribe: (() => void) | undefined;

	constructor(options: UnixControllerListenerOptions) {
		if (process.platform === "win32") throw new Error("Controller Unix sockets are not supported on Windows");
		if (!options.path) throw new TypeError("Controller socket path must not be empty");
		const configuredOwnerUid = options.ownerUid ?? process.getuid?.();
		if (
			typeof configuredOwnerUid !== "number" ||
			!Number.isSafeInteger(configuredOwnerUid) ||
			configuredOwnerUid < 0
		) {
			throw new Error("Controller owner UID is unavailable");
		}
		this.#path = options.path;
		this.#controller = options.controller;
		this.#peerUid = options.peerUid;
		this.#ownerUid = configuredOwnerUid;
		this.#server = createServer((socket) => {
			void this.#accept(socket);
		});
	}

	get path(): string {
		return this.#path;
	}

	async start(): Promise<void> {
		await this.#prepareSocketPath();
		await new Promise<void>((resolve, reject) => {
			this.#server.once("error", reject);
			this.#server.listen(this.#path, () => {
				this.#server.off("error", reject);
				resolve();
			});
		});
		await chmod(this.#path, 0o600);
		this.#unsubscribe = this.#controller.subscribe((event) => this.#broadcastEvent(event));
	}

	async close(): Promise<void> {
		this.#unsubscribe?.();
		this.#unsubscribe = undefined;
		for (const socket of this.#sockets) socket.destroy();
		this.#sockets.clear();
		await new Promise<void>((resolve, reject) => this.#server.close((error) => (error ? reject(error) : resolve())));
		await rm(this.#path, { force: true });
	}

	async #accept(socket: Socket): Promise<void> {
		socket.pause();
		let peerUid: number;
		try {
			peerUid = await this.#peerUid(socket);
		} catch {
			socket.destroy();
			return;
		}
		if (peerUid !== this.#ownerUid) {
			socket.destroy();
			return;
		}

		const runtime = new SessionControllerRuntimeAdapter(this.#controller, randomUUID());
		this.#sockets.add(socket);
		socket.once("close", () => this.#sockets.delete(socket));
		let sessionId: string;
		try {
			const snapshot = await runtime.snapshot();
			sessionId = snapshot.id;
			this.#send(socket, { type: "controller_snapshot", snapshot });
		} catch {
			socket.destroy();
			return;
		}
		let pending = "";
		socket.on("data", (chunk: Buffer) => {
			pending += chunk.toString("utf8");
			const lines = pending.split("\n");
			pending = lines.pop() ?? "";
			for (const line of lines) void this.#handleLine(socket, runtime, sessionId, line);
		});
		socket.resume();
	}

	async #handleLine(
		socket: Socket,
		runtime: SessionControllerRuntimeAdapter,
		sessionId: string,
		line: string,
	): Promise<void> {
		if (!line || line.length > 1024 * 1024) {
			this.#send(socket, { type: "error", message: "Controller request is invalid" });
			return;
		}
		let message: ClientMessage;
		try {
			message = decodeClientMessage(JSON.parse(line));
		} catch {
			this.#send(socket, { type: "error", message: "Controller request is invalid" });
			return;
		}
		if (message.envelope.sessionId !== sessionId) {
			this.#send(socket, { type: "error", message: "Controller session does not match this attachment" });
			return;
		}
		try {
			this.#send(socket, { type: "controller_receipt", receipt: await runtime.submit(message.envelope) });
		} catch {
			this.#send(socket, { type: "error", message: "Controller request failed" });
		}
	}

	#broadcastEvent(event: ControllerRuntimeEvent): void {
		for (const socket of this.#sockets) this.#send(socket, { type: "controller_event", event });
	}

	#send(socket: Socket, message: ServerMessage): void {
		if (!socket.destroyed) socket.write(`${JSON.stringify(message)}\n`);
	}

	async #prepareSocketPath(): Promise<void> {
		const directory = dirname(this.#path);
		await mkdir(directory, { recursive: true, mode: 0o700 });
		await chmod(directory, 0o700);
		const directoryStatus = await lstat(directory);
		if (
			!directoryStatus.isDirectory() ||
			directoryStatus.uid !== this.#ownerUid ||
			(directoryStatus.mode & 0o077) !== 0
		) {
			throw new Error("Controller socket directory must be owner-only");
		}
		try {
			const status = await lstat(this.#path);
			if (!status.isSocket()) throw new Error("Controller socket path is not a socket");
			await rm(this.#path);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
	}
}

function decodeClientMessage(value: unknown): ClientMessage {
	if (!isRecord(value) || value.type !== "submit" || !isRecord(value.envelope))
		throw new TypeError("Invalid controller request");
	const envelope = value.envelope;
	if (
		typeof envelope.sessionId !== "string" ||
		typeof envelope.actorId !== "string" ||
		typeof envelope.idempotencyKey !== "string" ||
		!isRecord(envelope.command)
	) {
		throw new TypeError("Invalid controller envelope");
	}
	return { type: "submit", envelope: envelope as unknown as ControllerEnvelope };
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
