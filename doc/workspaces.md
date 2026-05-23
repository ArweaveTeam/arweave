# Development workspaces (`bin/dev/ws`)

`bin/dev/ws` lets several engineers — or several Claude/agent sessions — work
on this repo **concurrently on one machine** without stepping on each other.

A **workspace** bundles three things under a single name:

- a **git worktree** — an isolated checkout on its own branch, with its own
  `_build/`, `.tmp/`, and compiled artifacts;
- one or more **named `screen` sessions** — each survives ssh disconnects and
  drives exactly one terminal: `ws-<name>` plus a `ws-<name>#2`, `#3`, … for
  every extra shell you open with `ws shell`;
- a **unique Erlang test namespace + private EPMD** — so concurrent test runs
  never collide.

One name (e.g. `fix-poller-race`) becomes the branch, the directory, the
screen session, and the test namespace.

## Commands

The manager lives at `bin/dev/ws`, with a thin shim at `bin/ws`. Invoke it as
`./bin/ws` from the repo. To use the bare `ws` shown below, put `bin/` on your
`PATH` or symlink the shim somewhere on it (e.g.
`ln -s "$PWD/bin/ws" ~/.local/bin/ws`).

```
ws new <name> [--no-attach]   create a workspace and attach to it
ws attach <name> [N]          attach to its primary (or #N) screen session
ws shell <name>               start a new independent shell session in it
ws current                    show details of the workspace you're in
ws ls                         list all workspaces
ws rm <name> [--force]        destroy a workspace
          [--delete-branch]
ws help                       full help
```

- **`ws new`** creates the worktree, initialises submodules, starts the screen
  session, and attaches you to it. The session has a single plain shell window
  in the worktree — run `claude`, the test suite, or whatever you like there;
  `ws` does not launch anything for you. Pass `--no-attach` to just create the
  workspace without attaching (handy when spinning up several at once).

  The worktree starts on a **detached HEAD at `master`** — `ws` creates no
  branch. Check out an existing branch (`git checkout <branch>`) or start a new
  one (`git checkout -b <branch>`) yourself once inside. This is deliberate:
  a git worktree cannot share a branch with any other worktree (including the
  main checkout), so `ws` can't simply put you "on `master`"; and a detached
  start means a stray commit can't accidentally advance a shared branch before
  you've made your own.
- **`ws attach`** attaches to the workspace's **primary** screen session
  `ws-<name>` (starting it if it died); `Ctrl-A d` detaches. `ws attach <name>
  N` attaches the extra shell session `ws-<name>#N` instead — handy for
  reattaching one you opened earlier with `ws shell` and then detached.
- **`ws shell`** starts a **new, independent** screen session — `ws-<name>#2`,
  `#3`, … — in the worktree and attaches to it. This is the way to get a
  second shell in a workspace: run `ws shell` from each terminal (or each
  agent) and every one gets its own private, disconnect-proof session. The
  sessions never share a display, resize each other, or detach one another —
  unlike attaching the *same* session from two terminals, which `screen` would
  turn into a shared, mirrored, smallest-common-size display.
- **`ws current`** prints details of the workspace you're currently in (path,
  branch, EPMD port, live screen sessions, uncommitted-change count). Run it
  from inside a workspace shell.
- **`ws rm`** kills **all** the workspace's screen sessions and its EPMD, then
  removes the worktree. It **keeps the branch** (that's your work) unless you
  pass `--delete-branch`. A worktree with uncommitted changes is not removed
  unless you pass `--force`.

Each `ws shell` session, like the primary, exports `ARWEAVE_NAMESPACE` and
`ERL_EPMD_PORT` for the workspace and starts in the worktree. They share that
one namespace, so don't run the test suite in two of them at once — the
namespace isolates *workspaces* from each other, not shells within one.

Worktrees live in `../arweave-workspaces/` next to the repo (override with
`ARWEAVE_WS_ROOT`). They are deliberately *outside* the repo so rebar3 and
erlang_ls don't recurse into them.

## Why this is needed: Erlang test isolation

Filesystem isolation alone is not enough for Erlang. The test suite starts a
distributed node, and two runs collide on machine-global resources:

- **EPMD** (the Erlang Port Mapper Daemon, normally one per machine on
  :4369) — two nodes registering the same name is a hard failure;
- **node names** — `bin/arweave test ar_block_tests` names its node
  `main-localtest-ar_block_tests`, and a second run of the *same* module
  would reuse that exact name.

A workspace fixes both:

- **`ARWEAVE_NAMESPACE=<name>`** — the core launcher scripts turn this into a
  unique Erlang node name. `ar_test_node` derives every peer node name and
  every `.tmp/data_*` directory from that node name, so all of them become
  per-workspace. (Peer HTTP ports are already collision-free — they use
  OS-assigned ephemeral ports.)
- **`ERL_EPMD_PORT=<43xx>`** — each workspace gets its own private EPMD.
  Independent EPMD instances are separate registries, so node names cannot
  collide across workspaces even if something bypasses `ARWEAVE_NAMESPACE`.

Both variables are exported automatically inside the workspace's screen
session, and written to a `.envrc` in the worktree for plain ssh shells
(`source .envrc`, or use `direnv`).

### `ARWEAVE_NAMESPACE` outside the `ws` workflow

`ARWEAVE_NAMESPACE` is honoured by all the core launcher scripts on its own —
just export it and run as usual, no `ws` required. The node name each script
uses becomes:

| Script(s)                         | Node name without / with `ARWEAVE_NAMESPACE=ns` |
|-----------------------------------|--------------------------------------------------|
| `bin/test`                        | `main-localtest` / `main-localtest-ns`           |
| `bin/e2e`                         | `main-e2e` / `main-e2e-ns`                       |
| `bin/shell`                       | `main-localtest` / `main-localtest-ns`           |
| `bin/e2e_shell`                   | `main-e2e` / `main-e2e-ns`                       |
| `bin/localnet_shell`              | `main-localnet` / `main-localnet-ns`             |
| `bin/start`, `bin/console`, `bin/stop` | `arweave` / `arweave-ns`                    |

The shared helper is `bin/dev/lib-namespace.sh`; `bin/arweave` and the dev
shells source it. When `ARWEAVE_NAMESPACE` is unset every script behaves exactly as
before. An explicit `NODE_NAME` (tests) or `ARNODE` (real node) always wins.

The interactive dev shells (`bin/shell`, `bin/e2e_shell`, `bin/localnet_shell`)
used to refuse to start if *any* BEAM was running anywhere on the host. That
guard is now scoped to the workspace's own node name, so shells in different
namespaces run concurrently — only a second shell with the *same* node name is
refused.

> Note: `ARWEAVE_NAMESPACE` isolates node names, not TCP ports. The test suite
> is fully covered (peers use ephemeral ports), but a *real* node started with
> `bin/start` still binds its configured `port` (default 1984) — run two real
> nodes side by side and you must also give them different `port` config.

## Making a PR from a workspace

A worktree shares the repo's remotes and config, so the normal flow works
from inside the workspace directory:

```bash
git add -A && git commit -m "..."
git push -u origin <branch>          # branch == workspace name by default
gh pr create --base master           # PRs go to the 'origin' (arweave-dev) remote
```

Then keep working, or tear down: `ws rm <name>` removes the worktree but
**keeps the branch**, so the PR is unaffected. Delete the branch after the PR
merges with `git branch -d <branch>` (or `ws rm <name> --delete-branch` if you
remove the workspace at the same time).

Caveats:

- A branch checked out in a worktree cannot be checked out or deleted
  elsewhere until that worktree is removed.
- Commit or push before `ws rm` — otherwise it refuses (use `--force` only if
  you really mean to discard local edits).
