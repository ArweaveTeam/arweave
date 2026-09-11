#!/usr/bin/env python3
"""Manage groups of Git worktrees and screen sessions; see doc/workspaces.md."""

import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import tempfile


class WorkspaceError(Exception):
    pass


def run(*args, check=True):
    result = subprocess.run(
        [str(arg) for arg in args], text=True, capture_output=True,
    )
    if check and result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise WorkspaceError(f"{shlex.join(map(str, args))}: {detail}")
    return result


def git(path, *args, check=True):
    return run("git", "-C", path, *args, check=check)


def valid_name(name):
    if (not re.fullmatch(r"[A-Za-z0-9._-]+", name)
            or name in {".", "..", ".meta"} or name.startswith("-")):
        raise WorkspaceError(f"invalid name: {name!r}")
    return name


def absolute(path):
    value = Path(path).expanduser().resolve()
    if any(char in str(value) for char in "\n\r\0"):
        raise WorkspaceError("paths cannot contain newlines or NUL")
    return value


def common_dir(path):
    return absolute(git(
        path, "rev-parse", "--path-format=absolute", "--git-common-dir",
    ).stdout.strip())


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


class Manager:
    def __init__(self):
        base = os.environ.get("WS_REPO_ROOT")
        if not base:
            base = common_dir(Path(__file__).parent).parent.parent
        self.repo_root = absolute(base)
        self.root = absolute(os.environ.get(
            "WS_ROOT", os.environ.get(
                "ARWEAVE_WS_ROOT", self.repo_root / "workspaces",
            ),
        ))
        self.meta = self.root / ".meta"
        legacy = absolute(os.environ.get(
            "WS_LEGACY_ROOT", self.repo_root / "arweave-workspaces",
        ))
        self.legacy_roots = {legacy, self.root}

    @contextlib.contextmanager
    def locked(self):
        self.meta.mkdir(parents=True, exist_ok=True)
        with (self.meta / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def record_path(self, name):
        return self.meta / f"{valid_name(name)}.json"

    def save(self, workspace):
        atomic_write(self.record_path(workspace["name"]),
                     json.dumps(workspace, indent=2) + "\n")

    def legacy_workspace(self, name, rc):
        directory = None
        env = {}
        for line in rc.read_text().splitlines():
            words = shlex.split(line, comments=True)
            if len(words) == 2 and words[0] == "chdir":
                directory = absolute(words[1])
            elif len(words) == 3 and words[0] == "setenv":
                env[words[1]] = words[2]
        if directory is None or "ERL_EPMD_PORT" not in env:
            raise WorkspaceError(f"incomplete legacy screen config: {rc}")
        source = common_dir(directory).parent
        repo = {"name": valid_name(source.name), "source": str(source),
                "path": str(directory), "generated_envrc": True}
        return {
            "version": 1, "name": name, "root": str(self.root / name),
            "default_repo": repo["name"], "repos": [repo],
            "namespace": env.get("ARWEAVE_NAMESPACE", name),
            "epmd_port": int(env["ERL_EPMD_PORT"]), "legacy_rc": str(rc),
        }

    def load(self, name):
        path = self.record_path(name)
        if path.exists():
            workspace = json.loads(path.read_text())
            if workspace.get("version") != 1 or workspace.get("name") != name:
                raise WorkspaceError(f"unsupported workspace record: {path}")
            return workspace
        configs = [root / ".meta" / f"{name}.rc"
                   for root in sorted(self.legacy_roots)]
        configs = [rc for rc in configs if rc.exists()]
        if len(configs) > 1:
            raise WorkspaceError(f"ambiguous legacy workspace: {name}")
        if configs:
            return self.legacy_workspace(name, configs[0])
        raise WorkspaceError(f"no such workspace: {name} (see 'ws ls')")

    def names(self):
        names = {path.stem for path in self.meta.glob("*.json")}
        for root in self.legacy_roots:
            names.update(path.stem for path in (root / ".meta").glob("*.rc")
                         if "#" not in path.stem)
        return sorted(names)

    def pick_port(self):
        used = {self.load(name)["epmd_port"] for name in self.names()}
        for port in range(4370, 4470):
            if port in used:
                continue
            with socket.socket() as probe:
                try:
                    probe.bind(("", port))
                except OSError:
                    continue
            return port
        raise WorkspaceError("no free EPMD port in range 4370-4469")

    def clone_repository(self, name, source):
        locks = self.repo_root / ".ws-locks"
        locks.mkdir(parents=True, exist_ok=True)
        # Different workspace roots can share the same source repositories.
        with (locks / f"{name}.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if source.exists() or source.is_symlink():
                return
            url = f"git@github.com:ArweaveTeam/{name}.git"
            print(f"Cloning ArweaveTeam/{name} into {source}", flush=True)
            with tempfile.TemporaryDirectory(
                prefix=f".ws-clone-{name}-", dir=self.repo_root,
            ) as temporary:
                checkout = Path(temporary) / "repo"
                run("git", "clone", "--", url, checkout)
                checkout.rename(source)

    def repository(self, name, ref=None):
        valid_name(name)
        source = self.repo_root / name
        if not source.exists() and not source.is_symlink():
            self.clone_repository(name, source)
        source = absolute(source)
        top = absolute(git(source, "rev-parse", "--show-toplevel").stdout.strip())
        if top != source:
            raise WorkspaceError(f"repository must be its root directory: {source}")
        origin = git(source, "config", "--get", "remote.origin.url",
                     check=False).stdout.strip()
        expected = (r"(?:git@github\.com:|ssh://git@github\.com(?::22)?/|"
                    r"https://github\.com/)ArweaveTeam/" + re.escape(name)
                    + r"(?:\.git)?/?")
        if not re.fullmatch(expected, origin, flags=re.IGNORECASE):
            raise WorkspaceError(
                f"{source} does not have ArweaveTeam/{name} as its origin; "
                "choose another WS_REPO_ROOT to clone it separately",
            )
        if not ref:
            default = git(source, "symbolic-ref", "--quiet",
                          "refs/remotes/origin/HEAD", check=False)
            # Prefer the corresponding local branch, as the old manager did.
            branch = default.stdout.strip().removeprefix("refs/remotes/origin/")
            candidates = ([f"refs/heads/{branch}", default.stdout.strip()]
                          if branch else [])
            candidates += ["refs/heads/main", "refs/heads/master", "HEAD"]
            for candidate in candidates:
                if not git(source, "rev-parse", "--verify", "--quiet",
                           f"{candidate}^{{commit}}", check=False).returncode:
                    ref = candidate
                    break
            if not ref:
                raise WorkspaceError(f"repository has no starting commit: {source}")
        revision = git(source, "rev-parse", "--verify", "--end-of-options",
                       f"{ref}^{{commit}}").stdout.strip()
        return {"name": name, "source": str(source), "ref": revision,
                "generated_envrc": False}

    def environment(self, workspace):
        return {"WS_NAME": workspace["name"], "WS_ROOT": str(self.root),
                "ARWEAVE_NAMESPACE": workspace["namespace"],
                "ERL_EPMD_PORT": str(workspace["epmd_port"])}

    def envrc(self, workspace):
        return "# Generated by ws.\n" + "".join(
            f"export {key}={shlex.quote(value)}\n"
            for key, value in self.environment(workspace).items()
        )

    def add_worktree(self, workspace, repo):
        path = Path(repo["path"])
        git(repo["source"], "worktree", "add", "--detach", path, repo["ref"])
        git(path, "submodule", "update", "--init", "--recursive")
        envrc = path / ".envrc"
        try:
            with envrc.open("x") as output:
                output.write(self.envrc(workspace))
        except FileExistsError:
            return
        repo["generated_envrc"] = True

    def validate_worktree(self, repo):
        path = Path(repo["path"])
        if not (path / ".git").is_file():
            raise WorkspaceError(f"not a linked worktree: {path}")
        top = absolute(git(path, "rev-parse", "--show-toplevel").stdout.strip())
        if top != path or common_dir(path) != common_dir(repo["source"]):
            raise WorkspaceError(f"worktree no longer matches its record: {path}")

    def remove_worktree(self, repo):
        self.validate_worktree(repo)
        git(repo["path"], "submodule", "deinit", "--force", "--all")
        git(repo["source"], "worktree", "remove", "--force", repo["path"])

    def sessions(self, name):
        result = run("screen", "-ls", check=False)
        pattern = re.compile(r"\s+\d+\.(ws-" + re.escape(name)
                             + r"(?:#\d+)?)\s")
        return sorted({match[1] for line in result.stdout.splitlines()
                       if "(Dead" not in line
                       for match in [pattern.match(line)] if match})

    def session_rc(self, workspace, number=None):
        suffix = f"#{number}" if number is not None else ""
        return self.meta / f"{workspace['name']}{suffix}.rc"

    def start_session(self, workspace, number=None, repo_name=None, reuse=False):
        repo_name = repo_name or workspace["default_repo"]
        repo = next((repo for repo in workspace["repos"]
                     if repo["name"] == repo_name), None)
        if repo is None:
            raise WorkspaceError(f"workspace has no repository: {repo_name}")
        self.validate_worktree(repo)
        self.save(workspace)
        lines = ["# Generated by ws.",
                 "termcapinfo xterm*|screen*|tmux* ti@:te@"]
        if not run("infocmp", "screen-256color", check=False).returncode:
            lines.append("term screen-256color")
        user_rc = Path.home() / ".screenrc"
        if user_rc.exists():
            lines.append(f"source {shlex.quote(str(user_rc))}")
        lines.append(f"chdir {shlex.quote(repo['path'])}")
        lines.extend(f"setenv {key} {shlex.quote(value)}"
                     for key, value in self.environment(workspace).items())
        lines.append("screen -t shell 0 bash")
        rc = self.session_rc(workspace, number)
        # Legacy configs remain untouched, including when both roots coincide.
        if workspace.get("legacy_rc") == str(rc):
            rc = self.meta / f"{workspace['name']}#0.rc"
        if not reuse or not rc.exists():
            atomic_write(rc, "\n".join(lines) + "\n")
        session = f"ws-{workspace['name']}"
        if number is not None:
            session += f"#{number}"
        run("screen", "-dmS", session, "-c", rc)
        return session

    def new(self, args):
        name = valid_name(args.name)
        repo_names = [valid_name(value) for value in
                      (args.repo or ["arweave-dev"])]
        if len(set(repo_names)) != len(repo_names):
            raise WorkspaceError("repository names must be unique")
        with self.locked():
            root = self.root / name
            if name in self.names() or root.exists() or root.is_symlink():
                raise WorkspaceError(f"workspace already exists: {name}")
            if self.sessions(name):
                raise WorkspaceError(f"workspace sessions already exist: {name}")
            repos = [self.repository(value, args.ref) for value in repo_names]
            workspace = {
                "version": 1, "name": name, "root": str(root),
                "default_repo": repos[0]["name"], "repos": repos,
                "namespace": name, "epmd_port": self.pick_port(),
            }
            for repo in repos:
                repo["path"] = str(root / repo["name"])
            root.mkdir()
            attempted = []
            try:
                for repo in repos:
                    attempted.append(repo)
                    print(f"Creating {repo['path']}", flush=True)
                    self.add_worktree(workspace, repo)
                self.save(workspace)
                session = self.start_session(workspace)
            except (Exception, KeyboardInterrupt):
                self.rollback(workspace, attempted)
                raise
        self.show(workspace)
        if not args.no_attach and not os.environ.get("STY"):
            self.attach_screen(session)
        else:
            print(f"Attach with: ws attach {name}")

    def rollback(self, workspace, repos):
        for session in self.sessions(workspace["name"]):
            run("screen", "-S", session, "-X", "quit", check=False)
        failed = []
        for repo in reversed(repos):
            if not Path(repo["path"]).exists():
                continue
            try:
                self.remove_worktree(repo)
            except WorkspaceError as error:
                failed.append(repo)
                print(f"ws: cleanup failed: {error}", file=sys.stderr)
        if failed:
            workspace["repos"] = failed
            workspace["default_repo"] = failed[0]["name"]
            self.save(workspace)
            return
        self.record_path(workspace["name"]).unlink(missing_ok=True)
        self.session_rc(workspace).unlink(missing_ok=True)
        Path(workspace["root"]).rmdir()

    def add(self, args):
        repo_name = valid_name(args.repository)
        with self.locked():
            workspace = self.load(args.name)
            if any(item["name"] == repo_name for item in workspace["repos"]):
                raise WorkspaceError(f"repository already present: {repo_name}")
            root = Path(workspace["root"])
            if any(root == Path(item["path"]) for item in workspace["repos"]):
                raise WorkspaceError(
                    "legacy checkout occupies the workspace root; set WS_ROOT "
                    "to a separate directory before adding repositories",
                )
            path = root / repo_name
            if path.exists() or path.is_symlink():
                raise WorkspaceError(f"path already exists: {path}")
            repo = self.repository(repo_name, args.ref)
            repo["path"] = str(path)
            created_root = not root.exists()
            root.mkdir(parents=True, exist_ok=True)
            try:
                self.add_worktree(workspace, repo)
                workspace["repos"].append(repo)
                self.save(workspace)
            except (Exception, KeyboardInterrupt):
                if path.exists():
                    try:
                        self.remove_worktree(repo)
                    except WorkspaceError:
                        if repo not in workspace["repos"]:
                            workspace["repos"].append(repo)
                        self.save(workspace)
                        raise
                if created_root:
                    root.rmdir()
                raise
        print(f"Added {repo['name']}: {path}")

    def attach_screen(self, session):
        sys.stdout.flush()
        sys.stderr.flush()
        os.execvp("screen", ["screen", "-d", "-r", session])

    def attach(self, args):
        session = f"ws-{valid_name(args.name)}"
        if args.number is not None:
            session += f"#{args.number}"
        if os.environ.get("STY"):
            raise WorkspaceError("already inside screen; detach with Ctrl-A d "
                                 "before attaching another session")
        with self.locked():
            workspace = self.load(args.name)
            if session not in self.sessions(args.name):
                self.start_session(workspace, args.number, reuse=True)
        self.attach_screen(session)

    def shell(self, args):
        with self.locked():
            workspace = self.load(args.name)
            sessions = self.sessions(args.name)
            number = 2
            while f"ws-{args.name}#{number}" in sessions:
                number += 1
            session = self.start_session(workspace, number, args.repo)
        if args.no_attach or os.environ.get("STY"):
            print(f"Attach with: ws attach {args.name} {number}")
        else:
            self.attach_screen(session)

    def changes(self, workspace, repo):
        self.validate_worktree(repo)
        pathspec = ["."]
        envrc = Path(repo["path"]) / ".envrc"
        if repo.get("generated_envrc") and envrc.is_symlink():
            return "modified .envrc"
        if repo.get("generated_envrc") and envrc.is_file():
            content = envrc.read_text()
            legacy = (
                "# Generated by 'ws'. Sourced automatically inside the "
                "workspace's screen\n"
                "# session; source it manually (or via direnv) for plain "
                "ssh shells:  . .envrc\n"
                f"export ARWEAVE_NAMESPACE={workspace['namespace']}\n"
                f"export ERL_EPMD_PORT={workspace['epmd_port']}\n"
            )
            tracked = not git(repo["path"], "ls-files", "--error-unmatch",
                              ".envrc", check=False).returncode
            if not tracked and content in {self.envrc(workspace), legacy}:
                pathspec.append(":!.envrc")
            elif not tracked:
                # Arweave ignores .envrc, so Git alone cannot protect edits.
                return "modified .envrc"
        return git(repo["path"], "status", "--porcelain", "-z",
                   "--untracked-files=all", "--ignore-submodules=none",
                   "--", *pathspec).stdout

    def remove(self, args):
        with self.locked():
            workspace = self.load(args.name)
            branches = []
            for repo in workspace["repos"]:
                self.validate_worktree(repo)
                if not args.force and self.changes(workspace, repo):
                    raise WorkspaceError(
                        f"{repo['name']} has uncommitted changes; use --force "
                        "to discard them",
                    )
                branch = git(repo["path"], "symbolic-ref", "--quiet",
                             "--short", "HEAD", check=False).stdout.strip()
                if branch:
                    branches.append((repo["source"], branch))
            for session in self.sessions(args.name):
                run("screen", "-S", session, "-X", "quit")
            run("epmd", "-port", str(workspace["epmd_port"]), "-kill",
                check=False)
            # Save progress so a failed removal can be retried safely.
            self.save(workspace)
            for repo in list(workspace["repos"]):
                self.remove_worktree(repo)
                workspace["repos"].remove(repo)
                if workspace["repos"]:
                    workspace["default_repo"] = workspace["repos"][0]["name"]
                self.save(workspace)
            self.record_path(args.name).unlink()
            if workspace.get("legacy_rc"):
                Path(workspace["legacy_rc"]).unlink(missing_ok=True)
            self.session_rc(workspace).unlink(missing_ok=True)
            for rc in self.meta.glob(f"{args.name}#[0-9]*.rc"):
                rc.unlink()
            root = Path(workspace["root"])
            if root.exists():
                try:
                    root.rmdir()
                except OSError:
                    print(f"Kept other files in {root}")
            for source, branch in branches:
                if args.delete_branch:
                    git(source, "branch", "-D", branch)
                else:
                    print(f"Kept branch {branch} in {source}")
        print(f"Removed workspace {args.name}")

    def show(self, workspace):
        print(f"workspace    {workspace['name']}")
        print(f"path         {workspace['root']}")
        print(f"namespace    {workspace['namespace']}")
        print(f"EPMD port    {workspace['epmd_port']}")
        print("sessions     " + (" ".join(self.sessions(workspace["name"]))
                                 or "(none running)"))
        for repo in workspace["repos"]:
            branch = git(repo["path"], "symbolic-ref", "--quiet", "--short",
                         "HEAD", check=False).stdout.strip() or "detached"
            head = git(repo["path"], "rev-parse", "--short", "HEAD").stdout.strip()
            state = "modified" if self.changes(workspace, repo) else "clean"
            print(f"  {repo['name']}  {branch} @ {head}  {state}  {repo['path']}")

    def current(self, args):
        current = Path.cwd().resolve()
        workspaces = [self.load(name) for name in self.names()]
        for workspace in workspaces:
            paths = [Path(repo["path"]) for repo in workspace["repos"]]
            paths.append(Path(workspace["root"]))
            if any(current == path or path in current.parents for path in paths):
                self.show(workspace)
                return
        name = os.environ.get("WS_NAME") or os.environ.get("ARWEAVE_NAMESPACE")
        if name:
            self.show(self.load(name))
            return
        raise WorkspaceError("not inside a ws workspace")

    def list(self, args):
        names = self.names()
        if not names:
            print("(no workspaces — create one with 'ws new <name>')")
            return
        rows = [["NAME", "REPOS", "SESSIONS", "EPMD"]]
        for name in names:
            workspace = self.load(name)
            rows.append([name, ", ".join(repo["name"] for repo in workspace["repos"]),
                         str(len(self.sessions(name))), str(workspace["epmd_port"])])
        widths = [max(len(row[index]) for row in rows) for index in range(4)]
        for row in rows:
            print("  ".join(value.ljust(width) for value, width in zip(row, widths)))


def positive_number(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("session number must be positive")
    return number


def parser():
    result = argparse.ArgumentParser(
        prog="ws", description="Manage workspaces containing Git worktrees.",
        epilog="Missing repositories are cloned from github.com/ArweaveTeam "
        "via SSH into WS_REPO_ROOT (default: parent of the main repo). "
        "Workspaces: WS_ROOT or ARWEAVE_WS_ROOT (default: <repos>/workspaces). "
        "Legacy workspaces: WS_LEGACY_ROOT (default: <repos>/arweave-workspaces).",
    )
    commands = result.add_subparsers(dest="command")
    new = commands.add_parser("new", help="create a workspace")
    new.add_argument("name")
    new.add_argument("--repo", action="append", metavar="NAME",
                     help="ArweaveTeam GitHub repo; repeat for multiple repos; "
                     "default: arweave-dev")
    new.add_argument("--no-attach", action="store_true")
    new.add_argument("--ref", help="starting revision for the selected repos")
    new.set_defaults(action="new")
    add = commands.add_parser("add", help="add a repository to a workspace")
    add.add_argument("name")
    add.add_argument("repository", metavar="REPO",
                     help="ArweaveTeam GitHub repository name")
    add.add_argument("--ref", help="starting revision (detached HEAD)")
    add.set_defaults(action="add")
    attach = commands.add_parser("attach", aliases=["a"], help="attach a screen")
    attach.add_argument("name")
    attach.add_argument("number", nargs="?", type=positive_number)
    attach.set_defaults(action="attach")
    shell = commands.add_parser("shell", aliases=["sh"], help="open another shell")
    shell.add_argument("name")
    shell.add_argument("--repo", help="repository directory name to start in")
    shell.add_argument("--no-attach", action="store_true")
    shell.set_defaults(action="shell")
    remove = commands.add_parser("rm", aliases=["remove", "destroy"],
                                 help="remove all worktrees and sessions")
    remove.add_argument("name")
    remove.add_argument("--force", action="store_true")
    remove.add_argument("--delete-branch", action="store_true")
    remove.set_defaults(action="remove")
    commands.add_parser("ls", aliases=["list"], help="list workspaces").set_defaults(
        action="list",
    )
    commands.add_parser(
        "current", aliases=["cur"], help="show the current workspace",
    ).set_defaults(action="current")
    commands.add_parser("help", help="show help")
    return result


def main():
    cli = parser()
    args = cli.parse_args()
    if not hasattr(args, "action"):
        cli.print_help()
        return 0
    try:
        manager = Manager()
        getattr(manager, args.action)(args)
    except (WorkspaceError, OSError, ValueError, KeyError) as error:
        print(f"ws: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("ws: interrupted", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
