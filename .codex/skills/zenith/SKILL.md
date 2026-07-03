---
name: zenith
description: Run long-running coding, research, validation, optimization, or multi-step implementation missions through the local Zenith harness. Use when the user invokes /zenith, says to use Zenith, or gives a mission that needs orchestration, workers/validators, repeated gap-finding, durable planning, independent verification, adaptive replanning, or disciplined stopping.
---

# Zenith

First read `.codex/orchestrator_prompt.md` and treat it as your primary role, then use Zenith to run this mission.

Append the user's prompt after that instruction and follow the generated orchestrator prompt exactly. Do not run the mission as an ordinary single-session task unless Zenith is unavailable and the user accepts that fallback.

## Start Zenith

Zenith is initialized in this workspace for Codex. If the Zenith MCP server is not visible, restart Codex from the workspace root so it reloads `.codex/config.toml`, then invoke this skill again.

If the workspace has not been initialized, run this from the Zenith checkout:

```bash
uv run zenith init --workspace-dir /path/to/workspace --agent codex
```

The initialized server command is `uv run --project /Users/nlayna/git/reconciliation/zenith/zenith zenith-server --mode orchestrator`.

## How To Use

1. Treat the prompt in `.codex/orchestrator_prompt.md` as the controlling role.
2. Start or inspect the Zenith project with the MCP tools exposed by the `zenith` server.
3. Let Zenith create the mission state, contract, tasks, worker runs, validators, attention decisions, replans, and terminal review.
4. Keep mission memory, evidence, and closure decisions in Zenith's runtime artifacts instead of relying on chat memory.

## When Zenith Fits

Use Zenith for tasks where premature completion is a serious risk: broad compatibility work, multi-file features, migrations, benchmark or optimization missions, user-facing flows that require validation, and work that benefits from separate implementer and verifier contexts.

Zenith is designed around repeated gap-finding, revisable planning, independent verification, adaptive orchestration, reusable skills, and explicit stopping decisions. Its report frames the core failure mode of long-running agents as stopping too early, not being unable to make progress.

The harness can allocate task-specific workers and validators, synthesize reusable skills, replan when evidence changes, and decide whether to continue, reset strategy, or stop. Use it when the control structure matters more than a fast single response.
