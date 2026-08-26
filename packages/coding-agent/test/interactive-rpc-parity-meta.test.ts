import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const testDir = dirname(fileURLToPath(import.meta.url));
const sourceDir = join(testDir, "..", "src", "modes");

type TuiAction = {
	call: string;
	rpcCommand: string;
	rpcClientMethod: string;
};

// These are user-initiated session operations, not presentation or read-only state access.
const TUI_RPC_ACTIONS: readonly TuiAction[] = [
	{ call: "session.prompt", rpcCommand: "prompt", rpcClientMethod: "prompt" },
	{ call: "session.steer", rpcCommand: "steer", rpcClientMethod: "steer" },
	{ call: "session.followUp", rpcCommand: "follow_up", rpcClientMethod: "followUp" },
	{ call: "session.abort", rpcCommand: "abort", rpcClientMethod: "abort" },
	{ call: "runtimeHost.newSession", rpcCommand: "new_session", rpcClientMethod: "newSession" },
	{ call: "session.setModel", rpcCommand: "set_model", rpcClientMethod: "setModel" },
	{ call: "session.cycleModel", rpcCommand: "cycle_model", rpcClientMethod: "cycleModel" },
	{ call: "session.setThinkingLevel", rpcCommand: "set_thinking_level", rpcClientMethod: "setThinkingLevel" },
	{ call: "session.cycleThinkingLevel", rpcCommand: "cycle_thinking_level", rpcClientMethod: "cycleThinkingLevel" },
	{ call: "session.setSteeringMode", rpcCommand: "set_steering_mode", rpcClientMethod: "setSteeringMode" },
	{ call: "session.setFollowUpMode", rpcCommand: "set_follow_up_mode", rpcClientMethod: "setFollowUpMode" },
	{
		call: "session.setAutoCompactionEnabled",
		rpcCommand: "set_auto_compaction",
		rpcClientMethod: "setAutoCompaction",
	},
	{ call: "session.compact", rpcCommand: "compact", rpcClientMethod: "compact" },
	{ call: "session.abortRetry", rpcCommand: "abort_retry", rpcClientMethod: "abortRetry" },
	{ call: "session.executeBash", rpcCommand: "bash", rpcClientMethod: "bash" },
	{ call: "session.abortBash", rpcCommand: "abort_bash", rpcClientMethod: "abortBash" },
	{ call: "session.exportToHtml", rpcCommand: "export_html", rpcClientMethod: "exportHtml" },
	{ call: "runtimeHost.switchSession", rpcCommand: "switch_session", rpcClientMethod: "switchSession" },
	{ call: "runtimeHost.fork", rpcCommand: "fork", rpcClientMethod: "fork" },
	{ call: "runtimeHost.fork", rpcCommand: "clone", rpcClientMethod: "clone" },
	{ call: "session.setSessionName", rpcCommand: "set_session_name", rpcClientMethod: "setSessionName" },
];

// Intentional TUI-only actions and lifecycle plumbing. Adding an action here is a deliberate API decision.
const TUI_ONLY_ACTIONS = new Set([
	"session.abortBranchSummary",
	"session.abortCompaction",
	"session.bindExtensions",
	"session.clearQueue",
	"session.exportToJsonl",
	"session.navigateTree",
	"session.recordBashResult",
	"session.reload",
	"session.setScopedModels",
	"session.subscribe",
	"session.waitForIdle",
	"runtimeHost.dispose",
	"runtimeHost.importFromJsonl",
	"runtimeHost.setBeforeSessionInvalidate",
	"runtimeHost.setRebindSession",
]);

function parseSource(path: string): ts.SourceFile {
	return ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true);
}

function getThisCallTargets(sourceFile: ts.SourceFile): Set<string> {
	const calls = new Set<string>();
	const visit = (node: ts.Node): void => {
		if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)) {
			const receiver = node.expression.expression;
			if (
				ts.isPropertyAccessExpression(receiver) &&
				receiver.expression.kind === ts.SyntaxKind.ThisKeyword &&
				(receiver.name.text === "session" || receiver.name.text === "runtimeHost")
			) {
				calls.add(`${receiver.name.text}.${node.expression.name.text}`);
			}
		}
		ts.forEachChild(node, visit);
	};
	ts.forEachChild(sourceFile, visit);
	return calls;
}

function getRpcCommandCases(sourceFile: ts.SourceFile): Set<string> {
	const cases = new Set<string>();
	const visit = (node: ts.Node): void => {
		if (ts.isCaseClause(node) && ts.isStringLiteral(node.expression)) {
			cases.add(node.expression.text);
		}
		ts.forEachChild(node, visit);
	};
	ts.forEachChild(sourceFile, visit);
	return cases;
}

function getClassMethods(sourceFile: ts.SourceFile, className: string): Set<string> {
	const methods = new Set<string>();
	const visit = (node: ts.Node): void => {
		if (ts.isClassDeclaration(node) && node.name?.text === className) {
			for (const member of node.members) {
				if (ts.isMethodDeclaration(member) && member.name && ts.isIdentifier(member.name)) {
					methods.add(member.name.text);
				}
			}
		}
		ts.forEachChild(node, visit);
	};
	ts.forEachChild(sourceFile, visit);
	return methods;
}

describe("TUI/RPC action parity contract", () => {
	it("keeps every shared TUI session action backed by an RPC command and client method", () => {
		const tuiCalls = getThisCallTargets(parseSource(join(sourceDir, "interactive", "interactive-mode.ts")));
		const rpcCommands = getRpcCommandCases(parseSource(join(sourceDir, "rpc", "rpc-mode.ts")));
		const rpcClientMethods = getClassMethods(parseSource(join(sourceDir, "rpc", "rpc-client.ts")), "RpcClient");

		const classifiedTuiCalls = new Set([...TUI_RPC_ACTIONS.map((action) => action.call), ...TUI_ONLY_ACTIONS]);
		const sessionActions = [...tuiCalls].filter((call) => !call.startsWith("session.get"));
		expect(sessionActions.filter((call) => !classifiedTuiCalls.has(call))).toEqual([]);

		for (const action of TUI_RPC_ACTIONS) {
			expect(tuiCalls).toContain(action.call);
			expect(rpcCommands).toContain(action.rpcCommand);
			expect(rpcClientMethods).toContain(action.rpcClientMethod);
		}
	});
});
