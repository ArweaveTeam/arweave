# Arweave security threat model

Apply these trust assumptions when rating any security finding, whether it came
from an external report or an internal review — see
[security-triage-external.md](security-triage-external.md) and
[security-triage-internal.md](security-triage-internal.md).

## Trust assumptions

- Assume the attacker has no access to the host machine or the Erlang VM. A
  finding contingent on either form of access is non-exploitable.
- Treat the following actors as trusted and non-malicious while they serve the
  named role: peers used to join the network, configured trusted peers,
  coordinated-mining peers and exit nodes, VDF servers, and local peers. This
  assumption does not apply to pool mining, where the pool client is untrusted.
- A denial of service caused by a trusted actor is not critical. Receiving the
  wrong version of the chain from a peer used to join the network is also an
  accepted risk.
- Exception: even a trusted peer must not be able to take remote control of the
  machine, steal funds, or get the node's mining address banned; those findings
  remain in scope. Peers used to join the network must retain no trust of any
  kind after the node has joined.

## What counts as a finding

The bar is whether the triggering condition can occur, not whether the code
could be more robust. Pure defense in depth is not a finding.

Errors the node meets in normal operation are expected input: an IO error such
as a failed database read will happen, so crashing on one is a real bug
([#1317][pr-1317]), whereas guarding a call that cannot fail is not
([#1319][pr-1319], [#1310][issue-1310]).

Unexpected input from a trusted actor should still be rejected cleanly rather
than crashing. [`AI: Reject mismatched coordinated-mining publish preimage
instead of crashing` (#1320)][pr-1320] is the model for this case: the denial
of service is non-critical because the coordinated-mining peer is trusted, but
the input should be handled safely.

The following are examples of non-critical robustness issues:

- [`AI: Fix new CLI parser aborting boot on type-less options`
  (#1316)][pr-1316].
- [`AI: Release entropy semaphore on exception in record_chunk`
  (#1319)][pr-1319], because the race is very difficult to reproduce.

Very unlikely race conditions are generally not triage targets unless an
attacker can trigger them with practical reliability or their impact crosses a
trust boundary described above.

[issue-1310]: https://github.com/ArweaveTeam/arweave-dev/issues/1310
[pr-1316]: https://github.com/ArweaveTeam/arweave-dev/pull/1316
[pr-1317]: https://github.com/ArweaveTeam/arweave-dev/pull/1317
[pr-1319]: https://github.com/ArweaveTeam/arweave-dev/pull/1319
[pr-1320]: https://github.com/ArweaveTeam/arweave-dev/pull/1320
