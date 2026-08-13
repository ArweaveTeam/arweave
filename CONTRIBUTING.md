# Contributing

This is a quick overview of what you should know when contributing to this Git
repository.

- **Shared engineering guidance for humans and AI agents lives in
  [`doc/agents/`](doc/agents/)** — build and test commands, configuration,
  protocol details, and coding conventions.
- [`AGENTS.md`](AGENTS.md) is the automatically loaded entry point for AI agents.
  It adds agent operating rules and routes agents to the shared guidance.
- The code style guide is [`doc/agents/erlang-style.md`](doc/agents/erlang-style.md).
  We use four spaces for indentation, not tabs.
- Make sure the tests pass — see [`doc/agents/testing.md`](doc/agents/testing.md)
  for how to run them.
- To discuss development and get help from the Arweave organization and
  community, start at the
  [developer documentation](https://docs.arweave.org/developers).

## Repository model

Arweave maintains two source repositories:

- [`arweave`](https://github.com/ArweaveTeam/arweave) is public. It contains
  published source and is the repository external contributors should fork and
  submit pull requests to.
- `arweave-dev` is the private core-team development repository. Core-team work
  is based there, and it is normally ahead of the public repository. It may
  contain features still in development or sensitive changes held until a
  coordinated release.

Changes are published regularly from `arweave-dev` to `arweave` when they are
ready for public release. Core-team members with access should use
`arweave-dev` as the current development source; other contributors should use
the public workflow below.

## Public contribution workflow

1. Fork the main Git repo `https://github.com/ArweaveTeam/arweave.git`
2. Branch out from `master`.
3. Add your changes.
4. Run the tests (see above).
5. Rebase your branch on the upstream `master` if it has moved since you branched
   out.
6. Create a PR back to the upstream `master`.

## Version control conventions

### Code published to public master must work

All code published to `arweave` `master` should be fully functioning. This is a
**strict** requirement — the public branch is where end users obtain the
software to join and participate in the network.

### Commits should be as atomic as possible

A commit should aim to be a single logical change. Where that isn't possible,
explain each logical alteration in the commit message, comma separated.

### Commit message syntax

- The first character is capitalized.
- The message is succinct.
- The message is in the imperative mood.
- Multiple actions are comma separated.

### Commit description

In addition to a message, a commit should have a description focusing on *why*
the change was made rather than *what* was changed.

```
Add arweave style guide

Inconsistent styling made it hard for us to view, comprehend, and edit the
code so we had a discussion and agreed on the common style.
```

Happy hacking! :)
