This release brings various stability improvements, ECDSA TX-form binding, and improved replica 2.9 syncing. We recommend installing it as soon as you can to address potential node stability issues.

**Configuration Note:** the first time you start the node after upgrading to N.2.9.8, do not use the start_from_local_state or start_from_block configuration options.

**Syncing:**

Syncing could slow down to the pace of the slowest peer, even when much faster peers were available. Each storage module limits how much sync work it keeps in progress at once. When the node reached data that only a slow peer had, that peer's requests could take up the whole limit, and the node stopped moving on to the rest of the data. The faster peers that had the rest sat idle until the slow peer was done, which could take days.

Restarts made this more likely. The node kept its sync position in memory only, so after a restart every storage module started over from the beginning. If the first missing data there was only available from slow peers, the storage module synced at their pace.

We reproduced both cases in the sync simulator and fixed them:

- A single peer can now take up at most half of a storage module's sync work limit. The node puts aside the data that peer cannot take yet and comes back to it every second. The slow peer stays busy, and the fast peers get on with the rest of the data.
- Each storage module now saves its sync position and picks up from there after a restart.

The first fix handles up to 8 GB in a row per storage module that only slow peers have. If such a stretch is longer, the storage module still syncs at the slow peers' pace until it gets through it.

Special thanks to Discord users JF, lawso, and Vidiot (Tom) for their feedback.

**Other highlights:**

- Fixed a crash in the sync process near the end of the weave.
- Improved the stability of block and transaction processing.
- Removed the localnet tooling.
