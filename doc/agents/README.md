# Maintaining agent guidance

Read this document before adding or modifying agent guidance, tool-specific
adapters, or skills.

This directory is the single source of truth for engineering guidance shared
by humans and AI agents. Human contributors can start at
[`CONTRIBUTING.md`](../../CONTRIBUTING.md). Codex and other compatible agents
automatically start at [`AGENTS.md`](../../AGENTS.md), whose task-routing table
says which document to read before acting.

## How this is wired up

Content lives here, in plain Markdown, once. Each tool gets a thin pointer:

| Tool | Entry point |
|---|---|
| Codex | `AGENTS.md` is loaded automatically; `.agents/skills/ar-*` exposes repo-local skills |
| Claude Code | `CLAUDE.md` imports `AGENTS.md`; `.claude/skills/ar-*` symlinks the same skills |
| Cursor | `.cursor/rules/build.mdc` points at `AGENTS.md` |

Codex discovers repo-local skills under `.agents/skills/` from the current
working directory up to the repository root. Run `/skills` to browse them.

Claude Code discovers the same skills under `.claude/skills/`, where each
`SKILL.md` is a relative symlink to its `.agents/skills/` counterpart — one file
per skill, not two copies to keep in step. Both trees sit three levels below the
repository root, so the `../../../doc/agents/…` links resolve identically
whichever path an agent reads. Skills are named `ar-*` and contain only pointers
to the canonical documents in this directory.

Invoke a skill explicitly with the tool's native prefix:

- Codex: `$ar-code-review`, `$ar-fix-issue <issue-url>`, or
  `$ar-security-triage <issue-url>`.
- Claude Code: `/ar-code-review`, `/ar-fix-issue <issue-url>`, or
  `/ar-security-triage <issue-url>`.

Codex may also invoke a skill implicitly when its policy allows it, and Claude
Code may load a skill automatically when its description matches the request.
`ar-security-triage` is explicit-only because it writes to an external issue.

Adding a skill means creating it under `.agents/skills/ar-<name>/SKILL.md` and
linking it:

```bash
mkdir -p .claude/skills/ar-<name>
ln -s ../../../.agents/skills/ar-<name>/SKILL.md \
    .claude/skills/ar-<name>/SKILL.md
```

Keep descriptions tool-neutral — one file serves every agent, so a description
naming one tool's features is wrong for the others reading it.

**Only user-invoked procedures get a skill** — `ar-security-triage`,
`ar-fix-issue`, and `ar-code-review`. Security triage must be invoked by name;
the other two may also activate when a person asks for their task. Reference
documents get none. The "Read before you act" table in `AGENTS.md` is always in
context and already routes them, so another skill would duplicate that routing.
Keep the routing keywords in the table, where every agent sees them.

A skill's `description` is the exception that needs care: it is the only part
loaded into context up front, so it has to carry enough keywords for an agent to
recognize the document applies. Keep it an index of *topics* — name what the
document covers, never what it concludes. A description that states a rule can
fall out of sync with the document that owns it.

That gives a narrow rule for keeping the two in step: **update a description when
the document's coverage changes — a section added or removed — not when a rule
inside it changes.** Adding a section without adding its keyword means the skill
won't surface for the work that section governs. If a description needs editing
every time the document changes, it is carrying content it shouldn't.

**When adding shared engineering guidance, add it here and link it from
`AGENTS.md`.** Keep tool-specific files as thin adapters.
