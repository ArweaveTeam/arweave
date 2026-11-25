**This is a development prerelease containing changes under active development. It may not work correctly, and some changes may not be included in a regular release.**

Based on N.2.9.8, the release patches a mining cache reservation bug in coordinated mining:

A miner in a coordinated mining set could see a constant stream of `mining_worker_failed_to_process_chunk1` errors with the reason `cache_limit_exceeded`, even with plenty of free memory. The node did not reserve mining cache space for the second recall range it read on behalf of a peer, so those reads used up the space reserved for the node's own recall ranges. The node now reserves the space before it reads a range for a peer. When the cache is full, the peer request waits, and it is dropped once its VDF step is too old.

Thanks to Discord user Qwinn for reporting the bug.
