This is a development prerelease.

It is based on N.2.9.8 and includes everything merged into master up to 2026-11-25.

**Coordinated mining:**

A miner in a coordinated mining set could see a constant stream of `mining_worker_failed_to_process_chunk1` errors with the reason `cache_limit_exceeded`, even with plenty of free memory. The node did not reserve mining cache space for the second recall range it read on behalf of a peer, so those reads used up the space reserved for the node's own recall ranges. The node now reserves the space before it reads a range for a peer. When the cache is full, the peer request waits, and it is dropped once its VDF step is too old.

**Pool mining:**

- Fixed a crash of the pool process on a solution event with no pending request, for example for a rebased solution.
- Updated the partition number check for partial solutions.

**ECDSA:**

- `GET /tx/{id}/ecdsa_owner` returns the Base64URL-encoded compressed public key of an ECDSA transaction.
- The new `ecdsa_settled_checksum` metric shows whether nodes agree on the ECDSA settled set.
- When a node joins the network and downloads the ECDSA settled set, it compares the set with what the other trusted peers report and stops if they disagree.

**Other:**

- Fixed a crash of the block pre-validator that lost the blocks queued for pre-validation.
- Block index lookups by height use a binary search.
