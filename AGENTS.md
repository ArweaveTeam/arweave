# Arweave agent instructions

Instructions for AI agents working in this repo. Shared engineering guidance
for humans and agents lives in [`doc/agents/`](doc/agents/). This file supplies
agent operating rules and the essentials that should always be in context.
Before acting on a task, read every document that matches it in the table below.

**When adding shared engineering guidance, put it in `doc/agents/` and link it
here.** Do not add guidance to a tool-specific adapter (`CLAUDE.md`,
`.claude/skills/`, `.agents/skills/`, `.cursor/rules/`); those files only route
their tools to the shared documents.

## Read before you act

| Read | Before |
|---|---|
| [doc/agents/README.md](doc/agents/README.md) | Adding or modifying agent guidance, tool-specific adapters, or skills |
| [doc/agents/erlang-style.md](doc/agents/erlang-style.md) | Writing or reviewing Erlang beyond the basics below — spacing, naming, `-spec` policy, `maybe` vs nested `case`, comments and `%% @doc`, `?LOG_*` |
| [doc/agents/testing.md](doc/agents/testing.md) | **Running, adding, modifying, or disabling any test** — sandbox permissions, `-test_category`, `-test_peers`, `ar_test_await`, `./bin/test`, `./bin/ct` |
| [doc/agents/configuration.md](doc/agents/configuration.md) | Adding or reading a node configuration option |
| [doc/agents/metrics.md](doc/agents/metrics.md) | Querying live or historical node metrics through the node API, Prometheus, or Grafana |
| [doc/agents/protocol.md](doc/agents/protocol.md) | Working on mining, packing, or recall-range code |
| [doc/agents/code-review.md](doc/agents/code-review.md) | Reviewing a pull request, branch, or diff |
| [doc/agents/security-threat-model.md](doc/agents/security-threat-model.md) | Rating the severity or exploitability of any security finding |
| [doc/agents/security-triage-external.md](doc/agents/security-triage-external.md) | Explicitly invoking the `ar-security-triage-external` skill for a security-tracker issue |
| [doc/agents/security-triage-internal.md](doc/agents/security-triage-internal.md) | Explicitly invoking the `ar-security-triage-internal` skill for an internal review report |
| [doc/agents/issue-fixing.md](doc/agents/issue-fixing.md) | Fixing or resolving any tracker issue |

## Agent operating rules

### Workspace setup

Ignore reformatting commits listed in `./.git-blame-ignore-revs` when using
`git blame`. After cloning, configure your workspace with:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

To run several agent sessions concurrently on one machine without collisions,
see [doc/workspaces.md](doc/workspaces.md).

### Scratch and committed scripts

- **`scripts/`** is for scripts intended to be committed to the repo (CI
  helpers, release tooling, etc.). Treat anything you put there as part of the
  codebase — name it clearly, give it a header comment, make it work for
  everyone.
- **`tmp/`** is for throwaway scratch — one-off diagnostic scripts, experiments,
  ad-hoc data dumps. Files under `tmp/` are gitignored.
- Planning notes and investigation writeups also go under `tmp/` unless the user
  explicitly asks to add them to the repo.
- When in doubt, start in `tmp/` and graduate to `scripts/` only once the
  script's role is permanent.

### Commits

Don't run `git commit` (or `git push`) unless the user has explicitly asked you
to. Leave the working tree dirty so the user can review the diff and stage,
message, and split commits themselves. This also applies after a multi-step
task: finish, summarize what you changed, and stop — wait for an explicit
"commit" before invoking git.

## Project structure

Erlang/OTP project built with rebar3. Multiple applications live under `apps/`;
the main one is `arweave`.

- Source: `apps/<app>/src/`
- Tests: `apps/<app>/test/`
- Includes: `apps/<app>/include/`

## Building

Use `./ar-rebar3` instead of `./rebar3`:

```bash
./ar-rebar3 test compile     # compile for tests
./ar-rebar3 prod compile     # compile for production
```

## Running tests

```bash
./bin/test <module>              # EUnit-style test module
./bin/test <module>:<test>       # a single test
./bin/ct --suite <path>          # Common Test suite
```

Give every concurrent `./bin/test` run on the same host a unique namespace:

```bash
ARWEAVE_NAMESPACE=<unique-name> ./bin/test <module>
```

Full details — CI categories, peer declarations, helper modules, how to disable
a test — are in [doc/agents/testing.md](doc/agents/testing.md). Read it before
touching tests.

## Runtime compatibility

This repo does not use hot code reloading. Do not add compatibility branches
solely for old in-memory terms or old function contracts coexisting with new
code in the same BEAM. Prefer a single current contract unless the code must
read persisted data or externally provided input from an older release.

## Configuration

All node configuration goes through `arweave_config` — it is the single source
of truth. Read a value with `arweave_config:get([group, option])`.

Do NOT use `application:get_env/2,3`, `application:set_env`, or `sys.config` /
`vm.args` application env to read or define node configuration. If you need a
knob operators can set, it must be an `arweave_config` option.

Compile-time constants that are not operator-facing (internal timeouts,
intervals, buffer sizes) stay as module `-define`s.

To add an option, see
[doc/agents/configuration.md](doc/agents/configuration.md).

## Parsing integers from external input

Never call `binary_to_integer/1` directly on a binary that came from an external
source — a JSON body, a URL path segment, an HTTP header, a peer response. Use
`ar_serialize:parse_integer/1`, or `ar_serialize:parse_integer_or_infinity/1`
where the `infinity` atom is a valid input. These reject inputs longer than
`?MAX_INTEGER_DIGITS` (155) before building a bignum out of them.

This does not cover bare (unquoted) JSON numbers — `jiffy:decode` handles those.

## Erlang style essentials

The full reference is [doc/agents/erlang-style.md](doc/agents/erlang-style.md).
These are the rules that come up in nearly every diff:

- **Single-space OTP style.** One space around `->`, `=`, `?=`. Never pad with
  extra spaces to column-align tokens across clauses.
- **Four spaces for indentation.** Never tabs.
- **80 characters per line**, aimed for and largely held across the codebase.
- **Acronyms in variables are full caps**: `PeerID`, `URL`, `JSON`,
  `TCPKeepalive` — not `PeerId`, `Url`, `Json`, `TcpKeepalive`. Atom, function,
  and module names follow normal Erlang convention.
- **`%% @doc` for function comments**, one tight sentence. Plain `%%` is not for
  documenting what a function does.
- **No `-spec` attributes.** This repo does not use them; the ones still in the
  tree are legacy and are being removed. Never add one.
- **`do_*` for private workers** a public function delegates to. Reserve
  `apply_*` for when "apply" carries domain meaning.
- **Never name anything "overlay".** Pick a word that names the concept directly.
- **Use `?LOG_INFO` / `?LOG_WARNING` / `?LOG_ERROR`** for logging, and
  `ar:console/1,2` for operator-facing console output.

```erlang
%% Good — single-space, full-caps acronym, @doc, no -spec:
%% @doc Look up the canonical option_key for a legacy field name.
option_key_for(LegacyField) when is_atom(LegacyField) ->
    case lookup(LegacyField) of
        {ok, PeerID} -> {ok, PeerID};
        Else -> {error, Else}
    end.
```
