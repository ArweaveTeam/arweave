#!/usr/bin/env python3
"""Integration tests for bin/ws using temporary Git repos and fake screen/EPMD.

Run with python3 scripts/test_ws.py. No host workspaces or sessions are touched.
"""

import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest


WS = Path(__file__).resolve().parents[1] / "bin" / "ws"

FAKE_SCREEN = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

state_path = Path(os.environ["WS_TEST_STATE"])
state = json.loads(state_path.read_text()) if state_path.exists() else {}
args = sys.argv[1:]
if args == ["-ls"]:
    for index, name in enumerate(state, 100):
        print(f"\t{index}.{name}\t(Detached)")
    sys.exit(0 if state else 1)
with Path(os.environ["WS_TEST_EVENTS"]).open("a") as events:
    events.write(json.dumps(args) + "\n")
if args[0] == "-dmS":
    if os.environ.get("WS_TEST_FAIL_SCREEN"):
        sys.exit(1)
    state[args[1]] = Path(args[3]).read_text()
elif args[0] == "-S":
    state.pop(args[1], None)
temporary = state_path.with_suffix(".tmp")
temporary.write_text(json.dumps(state))
temporary.replace(state_path)
'''


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ws-test-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.repos = self.base / "repos with spaces"
        self.repos.mkdir()
        self.root = self.base / "workspaces with spaces"
        self.legacy = self.base / "legacy"
        self.fake_bin = self.base / "bin"
        self.fake_bin.mkdir()
        self.state_path = self.base / "screen-state.json"
        self.events_path = self.base / "screen-events.jsonl"
        self.git_config = self.base / "gitconfig"
        self.env = dict(os.environ)
        for key in ("WS_ROOT", "ARWEAVE_WS_ROOT", "WS_NAME", "STY",
                    "ARWEAVE_NAMESPACE", "ERL_EPMD_PORT", "GIT_DIR",
                    "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            self.env.pop(key, None)
        self.env.update({
            "WS_REPO_ROOT": str(self.repos), "WS_ROOT": str(self.root),
            "WS_LEGACY_ROOT": str(self.legacy),
            "WS_TEST_STATE": str(self.state_path),
            "WS_TEST_EVENTS": str(self.events_path),
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": str(self.git_config),
            "GIT_AUTHOR_NAME": "WS tests", "GIT_AUTHOR_EMAIL": "ws@example.org",
            "GIT_COMMITTER_NAME": "WS tests",
            "GIT_COMMITTER_EMAIL": "ws@example.org",
            "GIT_ALLOW_PROTOCOL": "file",
        })
        self.executable("screen", FAKE_SCREEN)
        self.executable("epmd", "#!/bin/sh\nexit 0\n")
        self.executable("infocmp", "#!/bin/sh\nexit 0\n")
        self.repo("arweave-dev")
        self.repo("infra", branch="main")

    def executable(self, name, content):
        path = self.fake_bin / name
        path.write_text(content)
        path.chmod(0o755)
        return path

    def command(self, *args, cwd=None, env=None, input=None):
        return subprocess.run(
            [str(arg) for arg in args], cwd=cwd or self.base,
            env=env or self.env, input=input, text=True, capture_output=True,
        )

    def git(self, path, *args, input=None):
        result = self.command("git", "-C", path, *args, input=input)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def snapshot(self, path, branch="master"):
        # Create fixture history using plumbing, without committing in this repo.
        self.git(path, "add", "--all")
        tree = self.git(path, "write-tree")
        head = self.git(path, "commit-tree", tree, input="fixture\n")
        self.git(path, "update-ref", f"refs/heads/{branch}", head)
        return head

    def repo(self, name, branch="master", parent=None):
        path = (parent or self.repos) / name
        path.mkdir(parents=True)
        self.git(path, "init", "--initial-branch", branch)
        (path / "tracked.txt").write_text(f"{name}\n")
        if name == "arweave-dev":
            (path / ".gitignore").write_text(".envrc\n")
        self.snapshot(path, branch)
        self.git(path, "remote", "add", "origin",
                 f"git@github.com:ArweaveTeam/{name}.git")
        return path

    def remote_repo(self, name):
        path = self.repo(name, parent=self.base / "remote")
        # Exercise Git's real clone path without contacting GitHub or using keys.
        self.git(path, "config", "--file", self.git_config,
                 f"url.{path}.insteadOf", f"git@github.com:ArweaveTeam/{name}.git")
        return path

    def ws(self, *args, cwd=None, env=None):
        return self.command(WS, *args, cwd=cwd, env=env)

    def record(self, name):
        return json.loads((self.root / ".meta" / f"{name}.json").read_text())

    def sessions(self):
        return json.loads(self.state_path.read_text())

    def legacy_workspace(self, name="old", port=4370):
        path = self.legacy / name
        self.git(self.repos / "arweave-dev", "worktree", "add",
                 "--detach", path, "HEAD")
        meta = self.legacy / ".meta"
        meta.mkdir(exist_ok=True)
        rc = (f"chdir {shlex.quote(str(path))}\n"
              f"setenv ARWEAVE_NAMESPACE {name}\n"
              f"setenv ERL_EPMD_PORT {port}\n"
              "screen -t shell 0 bash\n")
        (meta / f"{name}.rc").write_text(rc)
        (path / ".envrc").write_text(
            "# Generated by 'ws'. Sourced automatically inside the workspace's screen\n"
            "# session; source it manually (or via direnv) for plain ssh shells:  . .envrc\n"
            f"export ARWEAVE_NAMESPACE={name}\n"
            f"export ERL_EPMD_PORT={port}\n"
        )
        self.state_path.write_text(json.dumps({f"ws-{name}": rc}))
        return path

    def test_multiple_repos_live_add_and_repo_shell(self):
        head = self.git(self.repos / "arweave-dev", "rev-parse", "HEAD")
        result = self.ws("new", "feature", "--repo", "arweave-dev",
                         "--repo", "infra", "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        record = self.record("feature")
        self.assertEqual([repo["name"] for repo in record["repos"]],
                         ["arweave-dev", "infra"])
        self.assertEqual(self.git(self.repos / "arweave-dev", "rev-parse", "HEAD"),
                         head)
        detached = self.command("git", "-C", self.root / "feature" / "infra",
                                "symbolic-ref", "HEAD")
        self.assertNotEqual(detached.returncode, 0)
        before = self.sessions()
        self.repo("docs")
        added = self.ws("add", "feature", "docs")
        self.assertEqual(added.returncode, 0, added.stderr)
        self.assertEqual(self.sessions(), before)
        shell = self.ws("shell", "feature", "--repo", "infra", "--no-attach")
        self.assertEqual(shell.returncode, 0, shell.stderr)
        rc = self.sessions()["ws-feature#2"]
        chdir = next(shlex.split(line)[1] for line in rc.splitlines()
                     if line.startswith("chdir "))
        self.assertEqual(chdir, str(self.root / "feature" / "infra"))
        self.assertIn("setenv ARWEAVE_NAMESPACE feature", rc)
        for cwd in (self.root / "feature", self.root / "feature" / "docs"):
            current = self.ws("current", cwd=cwd)
            self.assertEqual(current.returncode, 0, current.stderr)
            self.assertIn("docs", current.stdout)
        self.assertIn("arweave-dev, infra, docs", self.ws("ls").stdout)

    def test_legacy_add_preserves_paths_sessions_config_and_port(self):
        old = self.legacy_workspace()
        rc = self.legacy / ".meta" / "old.rc"
        previous_rc = rc.read_bytes()
        previous_sessions = self.sessions()
        listing = self.ws("ls")
        self.assertEqual(listing.returncode, 0, listing.stderr)
        self.assertFalse(self.root.exists())
        added = self.ws("add", "old", "infra")
        self.assertEqual(added.returncode, 0, added.stderr)
        record = self.record("old")
        self.assertEqual(record["repos"][0]["path"], str(old))
        self.assertEqual(record["repos"][1]["path"],
                         str(self.root / "old" / "infra"))
        self.assertEqual(record["epmd_port"], 4370)
        self.assertEqual(self.sessions(), previous_sessions)
        self.assertEqual(rc.read_bytes(), previous_rc)
        self.assertEqual(self.ws("current", cwd=old).returncode, 0)
        created = self.ws("new", "other", "--no-attach")
        self.assertEqual(created.returncode, 0, created.stderr)
        self.assertNotEqual(self.record("other")["epmd_port"], 4370)
        removed = self.ws("rm", "old")
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertFalse(old.exists())
        self.assertFalse(rc.exists())
        self.assertIn("ws-other", self.sessions())

    def test_dirty_second_repo_prevents_all_removal(self):
        created = self.ws("new", "dirty", "--repo", "arweave-dev",
                          "--repo", "infra", "--no-attach")
        self.assertEqual(created.returncode, 0, created.stderr)
        (self.root / "dirty" / "infra" / "untracked.txt").write_text("work\n")
        before = self.events_path.read_bytes()
        removed = self.ws("rm", "dirty")
        self.assertNotEqual(removed.returncode, 0)
        self.assertIn("infra has uncommitted changes", removed.stderr)
        self.assertEqual(self.events_path.read_bytes(), before)
        self.assertTrue((self.root / "dirty" / "arweave-dev").exists())
        forced = self.ws("rm", "dirty", "--force")
        self.assertEqual(forced.returncode, 0, forced.stderr)
        self.assertFalse((self.root / "dirty").exists())
        self.assertNotIn("ws-dirty", self.sessions())

    def test_failed_screen_start_rolls_back_all_new_worktrees(self):
        env = dict(self.env, WS_TEST_FAIL_SCREEN="1")
        result = self.ws("new", "broken", "--repo", "arweave-dev",
                         "--repo", "infra", "--no-attach", env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("screen -dmS", result.stderr)
        self.assertFalse((self.root / "broken").exists())
        self.assertFalse((self.root / ".meta" / "broken.json").exists())
        for name in ("arweave-dev", "infra"):
            trees = self.git(self.repos / name, "worktree", "list", "--porcelain")
            self.assertNotIn("broken", trees)

    def test_submodule_failure_on_add_keeps_existing_workspace(self):
        broken = self.repo("broken")
        self.git(broken, "submodule", "add", self.repos / "infra", "dependency")
        self.git(broken, "config", "-f", ".gitmodules", "submodule.dependency.url",
                 str(self.base / "missing-repository"))
        self.snapshot(broken)
        self.git(broken, "submodule", "deinit", "--all")
        self.assertEqual(self.ws("new", "feature", "--no-attach").returncode, 0)
        before = (self.root / ".meta" / "feature.json").read_bytes()
        events = self.events_path.read_bytes()
        result = self.ws("add", "feature", "broken")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "feature" / "broken").exists())
        self.assertEqual((self.root / ".meta" / "feature.json").read_bytes(), before)
        self.assertEqual(self.events_path.read_bytes(), events)
        self.assertNotIn("workspaces with spaces", self.git(
            broken, "worktree", "list", "--porcelain"))

    def test_submodule_edits_are_protected_and_clean_removal_works(self):
        source = self.repos / "arweave-dev"
        self.git(source, "submodule", "add", self.repos / "infra", "dependency")
        self.snapshot(source)
        created = self.ws("new", "submodules", "--no-attach")
        self.assertEqual(created.returncode, 0, created.stderr)
        file = self.root / "submodules" / "arweave-dev" / "dependency" / "tracked.txt"
        original = file.read_text()
        file.write_text("changed\n")
        before = self.events_path.read_bytes()
        refused = self.ws("rm", "submodules")
        self.assertNotEqual(refused.returncode, 0)
        self.assertEqual(self.events_path.read_bytes(), before)
        file.write_text(original)
        removed = self.ws("rm", "submodules")
        self.assertEqual(removed.returncode, 0, removed.stderr)

    def test_repo_envrc_is_preserved_and_generated_edits_are_protected(self):
        source = self.repos / "infra"
        (source / ".envrc").write_text("export CUSTOM=value\n")
        self.snapshot(source, "main")
        result = self.ws("new", "env", "--repo", "infra", "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "env" / "infra" / ".envrc").read_text(),
                         "export CUSTOM=value\n")
        self.assertEqual(self.ws("rm", "env").returncode, 0)
        self.assertEqual(self.ws("new", "env", "--no-attach").returncode, 0)
        envrc = self.root / "env" / "arweave-dev" / ".envrc"
        envrc.write_text(envrc.read_text() + "export CUSTOM=value\n")
        self.assertNotEqual(self.ws("rm", "env").returncode, 0)
        (source / ".envrc").unlink()
        target = self.base / "nonexistent-envrc"
        (source / ".envrc").symlink_to(target)
        self.snapshot(source, "main")
        result = self.ws("new", "symlink", "--repo", "infra", "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "symlink" / "infra" / ".envrc").is_symlink())
        self.assertFalse(target.exists())

    def test_github_clone_ref_and_invalid_requests(self):
        remote = self.remote_repo("docs")
        revision = self.git(remote, "rev-parse", "HEAD")
        result = self.ws("new", "paths", "--repo", "docs",
                         "--ref", revision, "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        source = self.repos / "docs"
        self.assertEqual(self.record("paths")["repos"][0]["source"], str(source))
        self.assertEqual(self.git(source, "config", "--get", "remote.origin.url"),
                         "git@github.com:ArweaveTeam/docs.git")
        self.assertEqual(self.git(self.root / "paths" / "docs", "rev-parse", "HEAD"),
                         revision)
        for arguments in (("add", "paths", "infra", "--ref", "missing"),
                          ("add", "paths", "docs"),
                          ("add", "paths", source),
                          ("new", "local-path", "--repo", source),
                          ("new", "relative-path", "--repo", "../infra"),
                          ("new", "paths", "--no-attach"),
                          ("new", "..", "--no-attach"),
                          ("new", "duplicate", "--repo", "infra", "--repo", "infra"),
                          ("shell", "paths", "--repo", "missing", "--no-attach")):
            with self.subTest(arguments=arguments):
                before = self.events_path.read_bytes()
                result = self.ws(*arguments)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.events_path.read_bytes(), before)

    def test_default_repo_is_cloned_and_reused(self):
        shutil.rmtree(self.repos / "arweave-dev")
        remote = self.remote_repo("arweave-dev")
        revision = self.git(remote, "rev-parse", "HEAD")
        first = self.ws("new", "first", "--no-attach")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertIn("Cloning ArweaveTeam/arweave-dev", first.stdout)
        source = self.repos / "arweave-dev"
        self.assertEqual(self.git(source, "config", "--get", "remote.origin.url"),
                         "git@github.com:ArweaveTeam/arweave-dev.git")
        remote.rename(remote.with_name("offline"))
        second = self.ws("new", "second", "--no-attach")
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotIn("Cloning", second.stdout)
        self.assertEqual(self.git(self.root / "second" / "arweave-dev",
                                  "rev-parse", "HEAD"), revision)
        removed = self.ws("rm", "first")
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertTrue(source.is_dir())
        self.assertTrue((self.root / "second" / "arweave-dev").is_dir())

    def test_add_clones_repo_without_disrupting_legacy_workspace(self):
        old = self.legacy_workspace()
        self.remote_repo("tools")
        before = self.sessions()
        result = self.ws("add", "old", "tools")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.sessions(), before)
        self.assertTrue((self.repos / "tools" / ".git").is_dir())
        self.assertTrue((self.root / "old" / "tools" / ".git").is_file())
        self.assertEqual(self.record("old")["repos"][0]["path"], str(old))
        self.assertEqual(self.record("old")["epmd_port"], 4370)

    def test_clone_failure_cleans_up_and_invalid_workspace_does_not_clone(self):
        self.remote_repo("tools")
        self.git(self.repos / "infra", "config", "--file", self.git_config,
                 f"url.{self.base / 'missing'}.insteadOf",
                 "git@github.com:ArweaveTeam/missing.git")
        nonexistent = self.ws("add", "nonexistent", "tools")
        self.assertNotEqual(nonexistent.returncode, 0)
        self.assertFalse((self.repos / "tools").exists())
        failed = self.ws("new", "failure", "--repo", "missing", "--no-attach")
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("git clone", failed.stderr)
        self.assertFalse((self.repos / "missing").exists())
        self.assertEqual(list(self.repos.glob(".ws-clone-*")), [])
        self.assertFalse((self.root / "failure").exists())
        self.assertFalse((self.root / ".meta" / "failure.json").exists())
        self.assertFalse(self.events_path.exists())

    def test_existing_repo_must_match_github_origin(self):
        source = self.repos / "infra"
        original = (source / "tracked.txt").read_bytes()
        for origin in ("git@github.com:OtherTeam/infra.git",
                       "https://github.com/ArweaveTeam/different.git"):
            with self.subTest(origin=origin):
                self.git(source, "remote", "set-url", "origin", origin)
                result = self.ws("new", "mismatch", "--repo", "infra", "--no-attach")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("does not have ArweaveTeam/infra", result.stderr)
                self.assertEqual((source / "tracked.txt").read_bytes(), original)
                self.assertFalse((self.root / "mismatch").exists())
        for origin in ("https://github.com/ArweaveTeam/infra.git",
                       "ssh://git@github.com/ArweaveTeam/infra.git"):
            with self.subTest(origin=origin):
                self.git(source, "remote", "set-url", "origin", origin)
                result = self.ws("new", "match", "--repo", "infra", "--no-attach")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.ws("rm", "match").returncode, 0)

    def test_restart_selected_shell_and_legacy_primary(self):
        old = self.legacy_workspace()
        self.state_path.write_text("{}")
        result = self.ws("attach", "old")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((old / "tracked.txt").exists())
        self.assertEqual(self.ws("ls").returncode, 0)
        self.assertEqual(self.ws("add", "old", "infra").returncode, 0)
        result = self.ws("shell", "old", "--repo", "infra", "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        state = self.sessions()
        expected = state.pop("ws-old#2")
        self.state_path.write_text(json.dumps(state))
        result = self.ws("attach", "old", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.sessions()["ws-old#2"], expected)

    def test_concurrent_creation_reserves_distinct_ports(self):
        processes = [subprocess.Popen(
            [str(WS), "new", name, "--no-attach"], cwd=self.base,
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ) for name in ("one", "two")]
        results = [process.communicate(timeout=30) for process in processes]
        for process, (stdout, stderr) in zip(processes, results):
            self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertNotEqual(self.record("one")["epmd_port"],
                            self.record("two")["epmd_port"])

    def test_legacy_root_override_and_exact_session_names(self):
        self.legacy_workspace("old.name")
        env = dict(self.env)
        env.pop("WS_ROOT")
        env["ARWEAVE_WS_ROOT"] = str(self.legacy)
        self.assertEqual(self.ws("ls", env=env).returncode, 0)
        state = self.sessions()
        state.update({"ws-oldXname": "unrelated", "ws-old.name-extra": "unrelated",
                      "ws-old.name#2": "related"})
        self.state_path.write_text(json.dumps(state))
        refused = self.ws("add", "old.name", "infra", env=env)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("separate directory", refused.stderr)
        removed = self.ws("rm", "old.name", env=env)
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertEqual(self.sessions(), {"ws-oldXname": "unrelated",
                                           "ws-old.name-extra": "unrelated"})

    def test_branches_kept_unless_explicitly_deleted(self):
        for name, delete in (("keep", False), ("delete", True)):
            with self.subTest(name=name):
                result = self.ws("new", name, "--no-attach")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.git(self.root / name / "arweave-dev", "checkout", "-b", name)
                result = self.ws("rm", name, *(["--delete-branch"] if delete else []))
                self.assertEqual(result.returncode, 0, result.stderr)
                branch = self.command("git", "-C", self.repos / "arweave-dev",
                                      "show-ref", "--verify", f"refs/heads/{name}")
                self.assertEqual(branch.returncode == 0, not delete)

    def test_status_failure_prevents_removal_and_partial_removal_is_retryable(self):
        result = self.ws("new", "retry", "--repo", "arweave-dev",
                         "--repo", "infra", "--no-attach")
        self.assertEqual(result.returncode, 0, result.stderr)
        real_git = shutil.which("git")
        wrapper = self.executable("git", "#!/bin/sh\n"
            'if [ "$3" = status ]; then exit 128; fi\n'
            f'exec {shlex.quote(real_git)} "$@"\n')
        events = self.events_path.read_bytes()
        refused = self.ws("rm", "retry")
        self.assertNotEqual(refused.returncode, 0)
        self.assertEqual(self.events_path.read_bytes(), events)
        self.assertTrue((self.root / "retry" / "arweave-dev").exists())
        wrapper.write_text("#!/bin/sh\n"
            'if [ "$3" = worktree ] && [ "$4" = remove ]; then\n'
            '  case "$6" in */infra) exit 128 ;; esac\n'
            'fi\n'
            f'exec {shlex.quote(real_git)} "$@"\n')
        failed = self.ws("rm", "retry")
        self.assertNotEqual(failed.returncode, 0)
        self.assertFalse((self.root / "retry" / "arweave-dev").exists())
        self.assertEqual([repo["name"] for repo in self.record("retry")["repos"]],
                         ["infra"])
        wrapper.unlink()
        retried = self.ws("rm", "retry")
        self.assertEqual(retried.returncode, 0, retried.stderr)
        self.assertFalse((self.root / "retry").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
