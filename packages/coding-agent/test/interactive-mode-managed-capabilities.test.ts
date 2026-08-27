import { describe, expect, it, vi } from "vitest";
import { InteractiveMode } from "../src/modes/interactive/interactive-mode.ts";

type ActionHandler = () => void;

type ManagedMode = {
	defaultEditor: {
		onAction: (action: string, handler: ActionHandler) => void;
		onSubmit?: (text: string) => Promise<void>;
		onCtrlD?: () => void;
		onExtensionShortcut?: (data: string) => boolean;
	};
	editor: { setText: (text: string) => void };
	options: { managedCapabilities: Record<string, never> };
	runtimeHost: {
		session: {
			bindExtensions: () => Promise<void>;
			extensionRunner: { getShortcuts: () => Map<string, unknown> };
			settingsManager: { setHideThinkingBlock: (hidden: boolean) => void };
		};
	};
	ui: { onDebug?: () => void };
	showWarning: (message: string) => void;
	setupAutocompleteProvider: () => void;
	showLoadedResources: (options: { force: boolean; showDiagnosticsWhenQuiet: boolean }) => void;
	createBaseAutocompleteProvider(this: ManagedMode): {
		getSuggestions: (
			lines: string[],
			cursorLine: number,
			cursorCol: number,
			options: { signal: AbortSignal; force?: boolean },
		) => Promise<unknown>;
	};
};

type InteractiveModePrototype = {
	setupKeyHandlers(this: ManagedMode): void;
	setupEditorSubmitHandler(this: ManagedMode): void;
	bindCurrentSessionExtensions(this: ManagedMode): Promise<void>;
	setupExtensionShortcuts(
		this: ManagedMode,
		extensionRunner: ManagedMode["runtimeHost"]["session"]["extensionRunner"],
	): void;
	handleRightClickPaste(this: ManagedMode): Promise<void>;
	createBaseAutocompleteProvider(this: ManagedMode): ReturnType<ManagedMode["createBaseAutocompleteProvider"]>;
	emergencyTerminalExit(this: {
		isShuttingDown: boolean;
		unregisterSignalHandlers: () => void;
		options: { managedCapabilities?: Record<string, never>; onManagedEmergencyExit?: () => void | Promise<void> };
	}): void;
	shutdown(this: {
		isShuttingDown: boolean;
		options: {
			managedCapabilities?: Record<string, never>;
			onManagedLocalQuit?: () => void | Promise<void>;
		};
	}): Promise<void>;
};

const interactiveModePrototype = InteractiveMode.prototype as unknown as InteractiveModePrototype;

function createManagedMode(): ManagedMode {
	const actions = new Map<string, ActionHandler>();
	return Object.assign(Object.create(InteractiveMode.prototype), {
		defaultEditor: {
			onAction: (action: string, handler: ActionHandler) => actions.set(action, handler),
		},
		editor: { setText: vi.fn() },
		options: { managedCapabilities: {} },
		runtimeHost: {
			session: {
				bindExtensions: vi.fn(async () => {}),
				settingsManager: { setHideThinkingBlock: vi.fn() },
				extensionRunner: { getShortcuts: vi.fn(() => new Map()) },
			},
		},
		ui: {},
		showWarning: vi.fn(),
		setupAutocompleteProvider: vi.fn(),
		showLoadedResources: vi.fn(),
		get actions() {
			return actions;
		},
	}) as ManagedMode & { actions: Map<string, ActionHandler> };
}

describe("InteractiveMode managed capabilities", () => {
	it("rejects shell input before extension or session execution", async () => {
		const mode = createManagedMode();
		interactiveModePrototype.setupEditorSubmitHandler.call(mode);

		await mode.defaultEditor.onSubmit?.("!! id");

		expect(mode.runtimeHost.session.extensionRunner.getShortcuts).not.toHaveBeenCalled();
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: shell");
	});

	it("rejects configured model and session keys before native handlers", () => {
		const mode = createManagedMode() as ManagedMode & { actions: Map<string, ActionHandler> };
		interactiveModePrototype.setupKeyHandlers.call(mode);

		mode.actions.get("app.model.cycleForward")?.();
		mode.actions.get("app.session.new")?.();
		mode.ui.onDebug?.();

		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: modelMutation");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: sessionReplacement");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: debug");
		expect(mode.runtimeHost.session.bindExtensions).not.toHaveBeenCalled();
	});

	it("does not bind extensions or resolve their shortcuts", async () => {
		const mode = createManagedMode();

		await interactiveModePrototype.bindCurrentSessionExtensions.call(mode);
		interactiveModePrototype.setupExtensionShortcuts.call(mode, mode.runtimeHost.session.extensionRunner);

		expect(mode.runtimeHost.session.bindExtensions).not.toHaveBeenCalled();
		expect(mode.runtimeHost.session.extensionRunner.getShortcuts).not.toHaveBeenCalled();
		expect(mode.defaultEditor.onExtensionShortcut).toBeUndefined();
	});

	it("rejects managed mutations and clipboard access before native handlers", async () => {
		const mode = createManagedMode() as ManagedMode & { actions: Map<string, ActionHandler> };
		interactiveModePrototype.setupKeyHandlers.call(mode);
		interactiveModePrototype.setupEditorSubmitHandler.call(mode);

		mode.actions.get("app.thinking.cycle")?.();
		mode.actions.get("app.thinking.toggle")?.();
		mode.actions.get("app.message.copy")?.();
		mode.actions.get("app.clear")?.();
		mode.defaultEditor.onCtrlD?.();
		await mode.defaultEditor.onSubmit?.("/settings");
		await mode.defaultEditor.onSubmit?.("/copy");
		await mode.defaultEditor.onSubmit?.("/name managed");
		await mode.defaultEditor.onSubmit?.("/compact");
		await mode.defaultEditor.onSubmit?.("/quit");
		await interactiveModePrototype.handleRightClickPaste.call(mode);

		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: thinkingMutation");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: settingsMutation");
		expect(mode.runtimeHost.session.settingsManager.setHideThinkingBlock).not.toHaveBeenCalled();
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: clipboard");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: quit");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: settingsMutation");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: naming");
		expect(mode.showWarning).toHaveBeenCalledWith("Unavailable in managed mode: compaction");
		expect(mode.editor.setText).not.toHaveBeenCalled();
	});

	it("uses command-only autocomplete without cwd filesystem access", async () => {
		const mode = createManagedMode() as ManagedMode & {
			fdPath?: string;
			skillCommands: Map<string, string>;
			session: {
				scopedModels: [];
				modelRuntime: { getAvailableSnapshot: () => [] };
				promptTemplates: [];
				extensionRunner: { getRegisteredCommands: () => [] };
				resourceLoader: { getSkills: () => { skills: [] } };
			};
		};
		const getCwd = vi.fn(() => "/forbidden");
		mode.skillCommands = new Map();
		Object.assign(mode.runtimeHost.session, {
			sessionManager: { getCwd },
			scopedModels: [],
			modelRuntime: { getAvailableSnapshot: () => [] },
			promptTemplates: [],
			extensionRunner: { getRegisteredCommands: () => [] },
			resourceLoader: { getSkills: () => ({ skills: [] }) },
		});
		const provider = interactiveModePrototype.createBaseAutocompleteProvider.call(mode);
		await provider.getSuggestions(["./"], 0, 2, { signal: new AbortController().signal, force: true });

		expect(getCwd).not.toHaveBeenCalled();
	});

	it("runs the managed emergency-exit hook before exiting", async () => {
		const cleanup = vi.fn(async () => {});
		const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
		const mode = Object.assign(Object.create(InteractiveMode.prototype), {
			isShuttingDown: false,
			unregisterSignalHandlers: vi.fn(),
			options: { managedCapabilities: {}, onManagedEmergencyExit: cleanup },
		});
		try {
			interactiveModePrototype.emergencyTerminalExit.call(mode);
			await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(129));
			expect(cleanup).toHaveBeenCalledOnce();
		} finally {
			exit.mockRestore();
		}
	});

	it("uses the managed local quit hook without terminal or native runtime cleanup", async () => {
		const quit = vi.fn(async () => {});
		const exit = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
		const mode = {
			isShuttingDown: false,
			options: { managedCapabilities: {}, onManagedLocalQuit: quit },
		};
		try {
			await interactiveModePrototype.shutdown.call(mode);
			expect(quit).toHaveBeenCalledOnce();
			expect(exit).toHaveBeenCalledWith(0);
		} finally {
			exit.mockRestore();
		}
	});
});
