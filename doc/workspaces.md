# Development workspaces (`bin/ws`)

A workspace groups **one or more Git worktrees**, independent `screen` sessions,
and an Erlang test namespace with a private EPMD port. Each checkout has its own
build artifacts. Workspaces can contain different combinations of repositories.

The manager requires Python 3.9+, Git, GNU screen, `infocmp`, and Erlang's `epmd`.
`bin/ws` and `bin/dev/ws` both invoke the implementation in `bin/dev/ws.py`.

## Commands

```bash
ws new feature                         # clones/reuses ArweaveTeam/arweave-dev
ws new feature --repo arweave-dev --repo infra --no-attach
ws new release --repo arweave --ref release/N.2.9.6
ws add feature docs.arweave.org-info
ws add feature tools --ref main
ws attach feature                      # attach the primary screen
ws attach feature 2                    # attach screen #2
ws shell feature --repo infra          # new independent shell in infra
ws shell feature --no-attach            # start a shell without attaching
ws current                             # show all repos in this workspace
ws ls                                  # list workspaces and their repos
ws rm feature                          # remove all its worktrees and sessions
ws rm feature --force --delete-branch
```

`--repo NAME` selects **`github.com/ArweaveTeam/NAME`**. If its source clone is
missing, the manager clones `git@github.com:ArweaveTeam/NAME.git` automatically,
using your GitHub SSH credentials. This also applies to the default
`arweave-dev` repository and to `ws add`. Directory paths and other organizations
are not accepted as repository names.

Source clones live under `WS_REPO_ROOT`, normally `/opt` on this machine. For
example, `--repo infra` clones or reuses `/opt/infra`, then creates a worktree at
`/opt/workspaces/<workspace>/infra`. There is no name registry or alias mapping.
An existing clone is reused only if its configured `origin` matches the selected
ArweaveTeam repository (SSH and HTTPS origins are recognized). A conflicting
local directory is left untouched; select another `WS_REPO_ROOT` to clone the
repository separately. Reused clones use their existing refs without fetching.

Source clones are shared by workspaces and retained when a workspace is removed.
A failed clone cleans up its temporary checkout; successful source clones remain
available even if subsequent workspace creation fails. Clone operations use
per-repository locks under `<WS_REPO_ROOT>/.ws-locks/` so concurrent requests do
not race to create the same source clone.

`ws new` accepts repeated `--repo` options; without them it uses `arweave-dev`.
Each worktree starts detached, and submodules are initialized recursively.
The default starting revision is the local branch corresponding to
`origin/HEAD`, then that remote ref if the local branch is absent. Without a
usable remote default, it tries local `main`, local `master`, then `HEAD`.
`--ref` overrides this choice; for `new` it applies to every selected repository.
Use `ws add --ref` to choose a different revision for an additional repository.
Create or check out your working branch inside each checkout yourself.

`ws new` starts a plain shell in the **first selected repository** and attaches
unless `--no-attach` is given or you are already inside screen. It does not
launch agents or tests. `ws add` creates another worktree without restarting,
reattaching, or changing any existing shell. You can immediately `cd` into it.

The primary screen is `ws-<name>`. Each `ws shell` creates a separate
`ws-<name>#2`, `#3`, etc. These sessions do not share displays or detach one
another. `Ctrl-A d` detaches. `ws attach` restarts a missing session, retaining a
previously selected repository for extra shells. Nested screen attachment is
refused; `ws shell` can still create the new session and print an attach command.

`ws current` works from the workspace parent or any of its repositories. It also
recognizes the workspace variables exported by screen and legacy shells. It
shows each repository's branch, revision, path, and whether it has local edits.

`ws rm` checks **every repository, including submodules**, before stopping any
sessions. Local edits or untracked files prevent removal unless `--force` is
given. Generated `.envrc` files do not count as edits, but modifications to them
do. Git-ignored build output is disposable. All workspace screen sessions are
stopped; worktrees are removed through their respective source repositories.
Branches are retained unless `--delete-branch` explicitly requests deletion.
Other files placed in the workspace parent are retained.

## Directory layout and configuration

With repositories under `/opt`, new workspaces look like this:

```text
/opt/workspaces/
  .meta/
    feature.json
    feature.rc
    feature#2.rc
  feature/
    arweave-dev/
    infra/
    docs.arweave.org-info/
```

Each versioned `.json` record stores the repository sources and paths, default
repository, namespace, and EPMD port. Screen configs are generated separately.
Creation and addition clean up their new worktrees on failure. If cleanup itself
fails, the remaining worktree is recorded so removal can be retried. Mutating
commands serialize through a lock; port allocation includes legacy workspaces
and checks whether another process already binds the candidate port.

| Variable | Default | Purpose |
|---|---|---|
| `WS_REPO_ROOT` | Parent of the main checkout containing the manager | Where GitHub source repositories are cloned and reused |
| `WS_ROOT` | `<WS_REPO_ROOT>/workspaces` | New workspace directories and metadata |
| `ARWEAVE_WS_ROOT` | Unset | Accepted as an older spelling of `WS_ROOT`; `WS_ROOT` takes precedence |
| `WS_LEGACY_ROOT` | `<WS_REPO_ROOT>/arweave-workspaces` | Existing single-worktree workspaces |

Keep the workspace root outside your source repositories so build tools and
language servers do not recurse into sibling workspaces. Use one canonical
manager and root configuration for commands on this machine.

Every new workspace shell exports `WS_NAME`, `WS_ROOT`, `ARWEAVE_NAMESPACE`, and
`ERL_EPMD_PORT`. The same exports are written to each new checkout's `.envrc`
unless the repository already supplies one. Existing `.envrc` files are preserved;
if you use one, add the namespace and port exports yourself for non-screen shells.
Otherwise, `source .envrc` or use direnv in a plain SSH shell.

Shells and repositories in the same workspace share the Erlang namespace and
EPMD port. Run only one Erlang test invocation per workspace at a time, including
when both `arweave` and `arweave-dev` are present. Separate workspaces isolate
concurrent test runs.

## Using the new manager with existing workspaces

Existing `<legacy-root>/.meta/<name>.rc` records are recognized in place. Listing
them does not rewrite metadata. Their checkout paths, session names, namespaces,
and EPMD ports remain unchanged. Updating the manager requires no running shell
restart, checkout move, or rebuild.

Adding a repository to an existing workspace records both paths, for example:

```text
arweave-dev  /opt/arweave-workspaces/testnet
infra        /opt/workspaces/testnet/infra
```

A new-format record is written when that workspace gains a repository or starts
a new session; its original screen config remains untouched. The old checkout
continues to work at its original path. `ws rm` subsequently handles both paths.
Unmanaged Git worktrees without workspace metadata are not adopted automatically.

If `ARWEAVE_WS_ROOT` currently selects the legacy root, choose a separate
`WS_ROOT` before adding repositories to legacy workspaces. The manager refuses
to nest another repository inside an existing checkout.

Run the updated manager through one stable entry point on your PATH, or an alias
pointing directly at its `bin/ws`. It can be invoked from any directory. Replace
aliases that first change directory, such as
`alias ws='cd /opt/arweave-dev; ./bin/ws'`, with a direct invocation:

```bash
alias ws='/path/to/updated/checkout/bin/ws'
```

Existing interactive shells need to redefine their alias, or source the updated
shell configuration, to use it. Old checked-out copies of the manager only
understand the old layout; use the updated entry point for workspace management.

## Testing the manager

```bash
python3 scripts/test_ws.py
```

These integration tests use disposable local Git repositories, including
submodules, and simulated screen/EPMD commands. GitHub clone URLs are redirected
to local test repositories, so tests require neither GitHub access nor SSH keys.
They exercise automatic cloning and reuse (including the default repository),
multiple repositories, additions to running and legacy workspaces, port allocation,
failed creation and addition, shell restarts, and protection of local edits.
They do not operate on existing workspaces or sessions. Local socket creation
must be permitted for the port-allocation checks.

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

Both variables are exported inside workspace screen sessions and written to
manager-generated `.envrc` files for plain SSH shells.

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
git push -u origin <branch>
gh pr create --base master           # PRs go to the 'origin' (arweave-dev) remote
```

Then keep working, or tear down: `ws rm <name>` removes all its worktrees but
**keeps the branch**, so the PR is unaffected. Delete the branch after the PR
merges with `git branch -d <branch>` (or `ws rm <name> --delete-branch` if you
remove the workspace at the same time).

Caveats:

- A branch checked out in a worktree cannot be checked out or deleted
  elsewhere until that worktree is removed.
- Commit or push before `ws rm` — otherwise it refuses (use `--force` only if
  you really mean to discard local edits).
