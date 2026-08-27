import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getModel } from "@earendil-works/pi-ai/compat";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ModelRuntime } from "../src/core/model-runtime.ts";
import { DefaultResourceLoader } from "../src/core/resource-loader.ts";
import { createAgentSession } from "../src/core/sdk.ts";
import { SessionManager } from "../src/core/session-manager.ts";
import { SettingsManager } from "../src/core/settings-manager.ts";
import { createSyntheticSourceInfo } from "../src/core/source-info.ts";

const fsMocks = vi.hoisted(() => ({ readdirSync: vi.fn(), statSync: vi.fn() }));

vi.mock("node:fs", async (importOriginal) => {
	const actual = await importOriginal<typeof import("node:fs")>();
	fsMocks.readdirSync.mockImplementation(actual.readdirSync);
	fsMocks.statSync.mockImplementation(actual.statSync);
	return { ...actual, readdirSync: fsMocks.readdirSync, statSync: fsMocks.statSync };
});

describe("AgentSession managed resources", () => {
	let tempDir: string;
	let agentDir: string;

	beforeEach(() => {
		fsMocks.readdirSync.mockClear();
		fsMocks.statSync.mockClear();
		tempDir = join(tmpdir(), `pi-managed-resources-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		agentDir = join(tempDir, "agent");
		mkdirSync(agentDir, { recursive: true });
	});

	afterEach(() => {
		if (existsSync(tempDir)) {
			rmSync(tempDir, { recursive: true, force: true });
		}
	});

	it("does not bind extension authority or expand denied startup resources", async () => {
		const settingsManager = SettingsManager.create(tempDir, agentDir);
		const extensionAuthority = vi.fn();
		const resourceLoader = new DefaultResourceLoader({
			cwd: tempDir,
			agentDir,
			settingsManager,
			extensionFactories: [
				(pi) => {
					pi.on("session_start", extensionAuthority);
					pi.on("input", extensionAuthority);
				},
			],
			promptsOverride: () => ({
				prompts: [
					{
						name: "managed-template",
						description: "managed test template",
						content: "TEMPLATE_AUTHORITY $1",
						filePath: join(tempDir, "managed-template.md"),
						sourceInfo: createSyntheticSourceInfo("<managed-template>", { source: "test" }),
					},
				],
				diagnostics: [],
			}),
			skillsOverride: () => {
				const skillPath = join(tempDir, "SKILL.md");
				writeFileSync(skillPath, "# SKILL_AUTHORITY\n");
				return {
					skills: [
						{
							name: "managed-skill",
							description: "managed test skill",
							filePath: skillPath,
							baseDir: tempDir,
							sourceInfo: createSyntheticSourceInfo("<managed-skill>", { source: "test" }),
							disableModelInvocation: false,
						},
					],
					diagnostics: [],
				};
			},
			resourceCapabilities: {},
		});
		await resourceLoader.reload();

		const modelRuntime = await ModelRuntime.create({ authPath: join(agentDir, "auth.json") });
		vi.spyOn(modelRuntime, "hasConfiguredAuth").mockReturnValue(true);
		const { session } = await createAgentSession({
			cwd: tempDir,
			agentDir,
			model: getModel("anthropic", "claude-sonnet-4-5")!,
			modelRuntime,
			settingsManager,
			sessionManager: SessionManager.inMemory(tempDir),
			resourceLoader,
			resourceCapabilities: {},
		});
		const promptedTexts: string[] = [];
		const prompt = vi.fn(async (messages: Array<{ content: Array<{ text: string }> }>) => {
			promptedTexts.push(messages[0]?.content[0]?.text ?? "");
		});
		(session.agent as unknown as { prompt: typeof prompt }).prompt = prompt;

		await session.bindExtensions({});
		await session.prompt("/managed-template startup");
		await session.prompt("/skill:managed-skill startup");

		expect(extensionAuthority).not.toHaveBeenCalled();
		expect(promptedTexts).toEqual(["/managed-template startup", "/skill:managed-skill startup"]);

		session.dispose();
	});

	it("expands only resource types granted by a mixed profile", async () => {
		const settingsManager = SettingsManager.create(tempDir, agentDir);
		const resourceLoader = new DefaultResourceLoader({
			cwd: tempDir,
			agentDir,
			settingsManager,
			promptsOverride: () => ({
				prompts: [
					{
						name: "allowed-template",
						description: "allowed template",
						content: "TEMPLATE_ALLOWED $1",
						filePath: join(tempDir, "allowed-template.md"),
						sourceInfo: createSyntheticSourceInfo("<allowed-template>", { source: "test" }),
					},
				],
				diagnostics: [],
			}),
			skillsOverride: () => {
				const skillPath = join(tempDir, "mixed-SKILL.md");
				writeFileSync(skillPath, "# SKILL_DENIED\n");
				return {
					skills: [
						{
							name: "denied-skill",
							description: "denied mixed-profile skill",
							filePath: skillPath,
							baseDir: tempDir,
							sourceInfo: createSyntheticSourceInfo("<denied-skill>", { source: "test" }),
							disableModelInvocation: false,
						},
					],
					diagnostics: [],
				};
			},
			resourceCapabilities: { templates: true },
		});
		await resourceLoader.reload();
		const modelRuntime = await ModelRuntime.create({ authPath: join(agentDir, "auth.json") });
		vi.spyOn(modelRuntime, "hasConfiguredAuth").mockReturnValue(true);
		const { session } = await createAgentSession({
			cwd: tempDir,
			agentDir,
			model: getModel("anthropic", "claude-sonnet-4-5")!,
			modelRuntime,
			settingsManager,
			sessionManager: SessionManager.inMemory(tempDir),
			resourceLoader,
			resourceCapabilities: { templates: true },
		});
		const promptedTexts: string[] = [];
		const prompt = vi.fn(async (messages: Array<{ content: Array<{ text: string }> }>) => {
			promptedTexts.push(messages[0]?.content[0]?.text ?? "");
		});
		(session.agent as unknown as { prompt: typeof prompt }).prompt = prompt;

		await session.prompt("/allowed-template startup");
		await session.prompt("/skill:denied-skill startup");

		expect(promptedTexts).toEqual(["TEMPLATE_ALLOWED startup", "/skill:denied-skill startup"]);
		session.dispose();
	});

	it("does not scan or load project, system, append, or AGENTS resources under all-deny", async () => {
		writeFileSync(join(tempDir, "AGENTS.md"), "AGENTS_AUTHORITY");
		writeFileSync(join(agentDir, "SYSTEM.md"), "SYSTEM_AUTHORITY");
		writeFileSync(join(agentDir, "APPEND_SYSTEM.md"), "APPEND_AUTHORITY");
		const settingsManager = SettingsManager.create(tempDir, agentDir);
		const resourceLoader = new DefaultResourceLoader({
			cwd: tempDir,
			agentDir,
			settingsManager,
			noExtensions: true,
			noSkills: true,
			noPromptTemplates: true,
			noThemes: true,
			noContextFiles: true,
			noSystemPrompt: true,
			noAppendSystemPrompt: true,
			resourceCapabilities: {},
		});
		await resourceLoader.reload();

		expect(fsMocks.readdirSync).not.toHaveBeenCalled();
		expect(fsMocks.statSync).not.toHaveBeenCalled();
		expect(resourceLoader.getAgentsFiles().agentsFiles).toEqual([]);
		expect(resourceLoader.getSystemPrompt()).toBeUndefined();
		expect(resourceLoader.getAppendSystemPrompt()).toEqual([]);
	});

	it("preserves native system, append, and AGENTS resource loading", async () => {
		writeFileSync(join(tempDir, "AGENTS.md"), "NATIVE_AGENTS");
		writeFileSync(join(agentDir, "SYSTEM.md"), "NATIVE_SYSTEM");
		writeFileSync(join(agentDir, "APPEND_SYSTEM.md"), "NATIVE_APPEND");
		const resourceLoader = new DefaultResourceLoader({ cwd: tempDir, agentDir });

		await resourceLoader.reload();

		expect(resourceLoader.getAgentsFiles().agentsFiles.map((file) => file.content)).toContain("NATIVE_AGENTS");
		expect(resourceLoader.getSystemPrompt()).toBe("NATIVE_SYSTEM");
		expect(resourceLoader.getAppendSystemPrompt()).toEqual(["NATIVE_APPEND"]);
	});

	it("rejects a supplied resource loader that was not restricted at construction", async () => {
		const settingsManager = SettingsManager.create(tempDir, agentDir);
		const resourceLoader = new DefaultResourceLoader({ cwd: tempDir, agentDir, settingsManager });
		await resourceLoader.reload();
		const modelRuntime = await ModelRuntime.create({ authPath: join(agentDir, "auth.json") });

		await expect(
			createAgentSession({
				cwd: tempDir,
				agentDir,
				model: getModel("anthropic", "claude-sonnet-4-5")!,
				modelRuntime,
				settingsManager,
				sessionManager: SessionManager.inMemory(tempDir),
				resourceLoader,
				resourceCapabilities: {},
			}),
		).rejects.toThrow("Managed session resource capabilities must be applied when constructing the resource loader");
	});
});
