**This is a substantial update. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

This release is primarily a bug fix, stability, and performance release. It includes all changes from all of the 2.9.5 alpha releases. Full details can be found in the release notes for each alpha:
- [alpha1](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha1)
- [alpha2](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha2)
- [alpha3](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha3)
- [alpha4](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha4)
- [alpha5](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha5)
- [alpha6](https://github.com/ArweaveTeam/arweave/releases/tag/N.2.9.5-alpha6)

Some of the changes described above were to address regressions introduced in a prior alpha. The full set of changes you can expect when upgrading from 2.9.4.1 are described below.

A special call out to the mining community members who installed and tested each of the alpha releases. Their help was critical in addressing regressions, fixing bugs, and implementing imrprovements. Thank you! Full list of contributors [here](#community-involvement).

## New Binaries

This release includes an updated set of pre-built binaries:
- Ubuntu 22:04, erlang R26
- Ubuntu 24:04, erlang R26
- rocky9, erlang R26
- MacOS, erlang R26

The default `linux` release refers to Ubuntu 22:04, erlang R26.

Going forward we recommend Arweave be built with erlang R26 rather than erlang R24.

The MacOS binaries are intended to be used for VDF Servers. Packing and mining on MacOS is still unsupported.

## Changes to miner config

- Several changes to options related to repack-in-place. See [below](#support-for-repack-in-place-from-the-replica29-format).
- `vdf`: see [below](#optimized-vdf).
- Several changes to options related to the `verify` tool. See [below](#verify-tool-improvements).
- `disable_replica_2_9_device_limit`: Disable the device limit for the replica.2.9 format. By default, at most one worker will be active per physical disk at a time, setting this flag removes this limit allowing multiple workers to be active on a given physical disk.
- Several options to manually configure low level network performance. See help for options starting with `network.`, `http_client.` and `http_api.`.
- `mining_cache_size_mb`: the default is set to 100MiB per partition being mined (e.g. if you leave `mining_cache_size_mb` unset while mining 64 partitions, your mining cache will be set to 6,400 MiB). 
- The process for running multiple nodes on a single server has changed. Each instance will need to set distinct/unique values for the `ARNODE` and `ARCOOKIE` environment variables. Here is an example script to launch 2 nodes one named `exit` and one named `miner`:
```
#!/usr/bin/env bash
ARNODE=exit@127.0.0.1 \
ARCOOKIE=exit \
screen -dmSL arweave.exit -Logfile ./screenlog.exit \
    ./bin/start config_file config.exit.json;

ARNODE=miner@127.0.0.1 \
ARCOOKIE=miner \
screen -dmSL arweave.miner -Logfile ./screenlog.miner \
    ./bin/start config_file config.miner.json
```

## Optimized VDF

This release includes the optimized VDF algorithm developed by Discord user `hihui`. 

To use this optimized VDF algorithm set the `vdf hiopt_m4` config option. By default the node will run with the legacy `openssl` implementation.

## Support for repack-in-place from the `replica.2.9` format

This release introduces support for repack-in-place from `replica.2.9` to `unpacked` or to a different `replica.2.9` address. In addition we've made several performance improvements and fixed a number of edge case bugs which may previously have caused some chunks to be skipped by the repack process.

### Performance
Due to how replica.2.9 chunks are processed, the parameters for tuning the repack-in-place performance have changed. There are 4 main considerations:
- **Repack footprint size**: `replica.2.9` chunks are grouped in footprints of chunks. A full footprint is 1024 chunks distributed evenly across a partition.
- **Repack batch size**: The repack-in-place process reads some number of chunks, repacks them, and then writes them back to disk. The batch size controls how many contiguous chunks are read at once. Previously a batch size of 10 would mean that 10 chunks would be read, repacked, and written. However in order to handle `replica.2.9` data efficiently, a batch size indicates the number of *footprints* to process at once. So a batch size of 10 means that 10 footprints will be read, repacked, and written. Since a full footprint is 1024 chunks, the amount of memory required to process a batch size of 10 is now 10,240 chunks or roughly 2.5 GiB. 
- **Available RAM**: The footprint size and batch size drive how much RAM is required by the repack in place process. And if you're repacking multiple partitions at once, the RAM requirements can grow quickly.
- **Disk IO**: If you determine that disk IO is your bottleneck, you'd want to increase the batch size as much as you can as reading contiguous chunks are generally much faster than reading non-contiguous chunks.
- **CPU**: However in some cases you may find that CPU is your bottleneck - this can happen when repacking from a legacy format like `spora_2_6`, or can happen when repacking many partitions between 2 `replica.2.9` addresses. The saving grace here is that if CPU is your bottleneck, you can reduce your batch size or footprint size to ease off on your memory utilization.

To control all these factors, repack-in-place has 2 config options:
- `repack_batch_size`: controls the batch size - i.e. the number of footprints processed at once
- `repack_cache_size_mb`: sets the total amount of memory to allocate to the repack-in-place process *per* partition. So if you set `repack_cache_size_mb` to `2000` and are repacking 4 partitions, you can expect the repack-in-place process to consume roughly 8 GiB of memory. Note: the node will automatically set the footprint size based on your configured batch and cache sizes - this typically means that it will *reduce* the footprint size as much as needed. A smaller footprint size will *increase* your CPU load as it will result in your node generating the same entropy multiple times. For example, if your footprint size is 256 the node will need to generate teh same entropy 4 times in order to process all 1024 chunks in the full footprint.

### Debugging

This release also includes a new option on the `data-doctor inspect` tool that may help with debugging packing issues. 

```
/bin/data-doctor inspect bitmap <data_dir> <storage_module>
```

Example: `/bin/data-doctor inspect bitmap /opt/data 36,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI.replica.2.9`

Will generate a bitmap where every pixel represents the packing state of a specific chunk. The bitmap is laid out so that each *vertical* column of pixels is a complete entropy footprint. Here is an example of bitmap:

![bitmap_storage_module_5_En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI replica 2 9](https://github.com/user-attachments/assets/d84728c6-6cc0-447d-8f98-2db793fd66a4)

This bitmap shows the state of one node's partition 5 that has been repacked to replica.2.9. The green pixels are chunks that are in the expected replica.2.9 format, the black pixels are chunks that are missing from the miner's dataset, and the pink pixels are chunks that are too small to be packed (prior to partition ~9, users were allowed to pay for chunks that were smaller than 256KiB - these chunks are stored `unpacked` and can't be packed).

## Performance Improvements

- Improvements to both syncing speed and memory use while syncing
- In our tests using solo as well as coordinated miners configured to mine while syncing many partitions, we observed steady memory use and full expected hashrate. This improves on 2.9.4.1 performance. Notably: the same tests run on 2.9.4.1 showed growing memory use, ultimately causing an OOM.
- Reduce the volume of unnecessary network traffic due to a flood of `404` requests when trying to sync chunks from a node which only serves `replica.2.9` data. Note: the benefit of this change will only be seen when most of the nodes in the network upgrade.
- Performance improvements to HTTP handling that should improve performance more generally.
- Optimization to speed up the collection of peer intervals when syncing. This can improve syncing performance in some situations.
- Fix a bug which could cause syncing to occasionally stall out.
- Optimize the shutdown process. This should help with, but not fully address, the slow node shutdown issues.
- Fix a bug where a VDF client might get pinned to a slow or stalled VDF server.
- Several updates to the mining cache logic. These changes address a number of edge case performance and memory bloat issues that can occur while mining.
- Improve the transaction validation performance, this should reduce the frequency of "desyncs". I.e. nodes should now be able to handle a higher network transaction volume without stalling
  - Do not delay ready_for_mining on validator nodes
  - Make sure identical tx-status pairs do not cause extra mempool updates
  - Cache the owner address once computed for every TX
- Reduce the time it takes for a node to join the network:
  - Do not re-download local blocks on join
  - Do not re-write written txs on join
  - Reduce per peer retry budget on join 10 -> 5
- Fix edge case that could occasionally cause a mining pool to reject a replica.2.9 solution.
- Fix edge case crash that occurred when a coordinated miner timed out while fetching partitions from peers
- Fix bug where storage module crossing weave end may cause syncing stall
- Fix bug where crash during peer interval collection may cause syncing stall
- Fix race condition where we may not detect double-signing
- Optionally fix broken chunk storage records on the fly
  - Set `enable fix_broken_chunk_storage_record` to turn the feature on.
- Several fixes to improve the Arweave shutdown process
  - Close hanging HTTP connections
  - Prevent new HTTP connections from being created during shutdown
  - Fix a timeout issue

In addition to the above fixes, we have some guidance on how to interpret some new logging and metrics:

### Guidance: 2.9.5 hashrate appears to be slower than 2.9.4.1

(Reported by discord users EvM, Lawso2517, Qwinn)

#### Symptoms
- 2.9.4.1 hashrate is higher than 2.9.5
- 2.9.4.1 hashrate when solo mining might even be higher than the "ideal" hashrate listed in the mining report or grafana metrics

#### Resolution
The 2.9.4.1 hashrate included invalid hashes and the 2.9.5 hashrate, although lower, includes only valid hashes.

#### Root Cause
2.9.4.1 (and earlier releases) had a bug which caused miners to generate hashes off of entropy in addition to valid packed data. The replica.2.9 data format lays down a full covering of entropy in each storage module before adding packed chunks. The result that is that for any storage module with less than 3.6TB of packed data, there is some amount of data on disk that is just entropy. A bug in the 2.9.4.1 mining algorithm generated hashes off of this entropy causing an inflated hashrate. Often the 2.9.4.1 hashrate is above the estimated "ideal" hashrate even when solo mining.

Another symptom of this bug is the `chunk_not_found` error occasionally reported by miners. This occurs under 2.9.4.1 (and earlier releases) when the miner hashes a range of entropy and generates a hash that exceeds the network difficulty. The miner believes this to be a valid solution and begins to build a block. At some point in the block building process the miner has to validate and include the packed chunk data. However since no packed chunk data exists (only entropy), the solution fails and the error is printed.

2.9.5 fixes this bug so that miners correctly exclude entropy data when mining. This means that under 2.9.5 and later releases miners spend fewer resources hashing entropy data, and generate fewer failed solution errors. The reported hashrate on 2.9.5 is lower than 2.9.4.1 because the invalid hashes are no longer being counted.

### Guidance: `cache_limit_exceeded` warning during solo and coordinated mining

(Reported by discord users BerryCZ, mousetu, radion_nizametdinov, qq87237850, Qwinn, Vidiot)

#### Symptoms
- Logs show `mining_worker_failed_to_reserve_cache_space` warnings, with reason set to `cache_limit_exceeded`

#### Resolution
The warning, if seen periodically, is expected and safe to ignore.

#### Root Cause
All VDF servers - even those with the exact same VDF time - will be on slightly different steps. This is because new VDF epochs are opened roughly every 20 minutes and are opened when a block is added to the chain. Depending on when your VDF server receives that block it may start calculating the new VDF chain earlier or later than other VDF servers.

This can cause there to be a gap in the VDF steps generated by two different servers even if they are able to compute new VDF steps at the exact same speed.

When a VDF server receives a block that is ahead of it in the VDF chain, it is able to quickly validate and use all the new VDF steps. This can cause the associated miners to receive a batch of VDF steps all at once. In these situations, the miner may exceed its mining cache causing the `cache_limit_exceeded` warning.

However this ultimately does not materially impact the miner's true hashrate. A miner will process VDF steps in reverse order (latest steps first) as those are the most valuable steps. The steps being dropped from the cache will be the oldest steps. Old steps *may* still be useful, but there is a far greater chance that any solution mined off an old step will be orphaned. The older the VDF step, the less useful it is.

**TLDR:** the warning, if seen periodically, is expected and safe to ignore.

**Exception:** If you are continually seeing the warning (i.e. not in periodic batches, but constantly and all the time) it may indicate that your miner is not able to keep up with its workload. This can indicate a hardware configuration issue (e.g. disk read rates are too slow), or perhaps a hardware capacity issue (E.g. CPU not fast enough to run hashes on all attached storage module), or some other performance-related issue.


## Prometheus metrics

- `ar_mempool_add_tx_duration_milliseconds`: The duration in milliseconds it took to add a transaction to the mempool.
- `reverify_mempool_chunk_duration_milliseconds`: The duration in milliseconds it took to reverify a chunk of transactions in the mempool.
- `drop_txs_duration_milliseconds`: The duration in milliseconds it took to drop a chunk of transactions from the mempool
- `del_from_propagation_queue_duration_milliseconds`: The duration in milliseconds it took to remove a transaction from the propagation queue after it was emitted to peers.
- `chunk_storage_sync_record_check_duration_milliseconds`: The time in milliseconds it took to check the fetched chunk range is actually registered by the chunk storage.
- `fixed_broken_chunk_storage_records`: The number of fixed broken chunk storage records detected when reading a range of chunks.
- `mining_solution`: allows tracking mining solutions. Uses labels to differentiate the mining solution state.
- `chunks_read`: The counter is incremented every time a chunk is read from `chunk_storage`
- `chunk_read_rate_bytes_per_second`: The rate, in bytes per second, at which chunks are read from storage. The type label can be 'raw' or 'repack'.
- `chunk_write_rate_bytes_per_second`: The rate, in bytes per second, at which chunks are written to storage.
- `repack_chunk_states`: The count of chunks in each state. 'type' can be 'cache' or 'queue'.
- `replica_2_9_entropy_generated`: The number of bytes of replica.2.9 entropy generated.
- `mining_server_chunk_cache_size`: now includes additional label `type` which can take the value `total` or `reserved`.
- `mining_server_tasks`: Incremented each time the mining server adds a task to the task queue.
- `mining_vdf_step`: Incremented each time the mining server processes a VDF step.
- `kryder_plus_rate_multiplier`: Kryder+ rate multiplier.
- `endowment_pool_take`: Value we take from endowment pool to miner to compensate difference between expected and real reward.
- `endowment_pool_give`: Value we give to endowment pool from transaction fees.


## `verify` Tool Improvements

This release contains several improvements to the `verify` tool. Several miners have reported block failures due to invalid or missing chunks. The hope is that the `verify` tool improvements in this release will either allow those errors to be healed, or provide more information about the issue.

### New `verify` modes

The `verify` tool can now be launched in `log` or `purge` modes. In `log` mode the tool will log errors but will not flag the chunks for healing. In `purge` mode all bad chunks will be marked as invalid and flagged to be resynced and repacked.

To launch in `log` mode specify the `verify log` flag. To launch in `purge` mode specify the `verify purge` flag. Note: `verify true` is no longer valid and will print an error on launch.

### Chunk sampling

The `verify` tool will now sample 1,000 chunks and do a full unpack and validation of the chunk. This sampling mode is intended to give a statistical measure of how much data might be corrupt. To change the number of chunks sampled you can use the the `verify_samples` option. E.g. `verify_samples 500` will have the node sample 500 chunks.

### More invalid scenarios tested

This latest version of the `verify` tool detects several new types of bad data. The first time you run the `verify` tool we recommend launching it in `log` mode and running it on a single partition. This should avoid any surprises due to the more aggressive detection logic. If the results are as you expect, then you can relaunch in `purge` mode to clean up any bad data. In particular, if you've misnamed your `storage_module` the `verify` tool will invalidate *all* chunks and force a full repack - running in `log` mode first will allow you to catch this error and rename your `storage_module` before purging all data.


## Miscellaneous

- Fix several issues which could cause a node to "desync". Desyncing occurs when a node gets stuck at one block height and stops advancing.
- Add TX polling so that a node will pull missing transactions in addition to receiving them via gossip
- Add support for DNS pools (multiple IPs behind a single DNS address).
- Add webhooks for the entire mining solution lifecycle. New `solution` webhook added with multiple states `solution_rejected`, `solution_stale`, `solution_partial`, `solution_orphaned`, `solution_accepted`, and `solution_confirmed`.
- Add a `verify` flag to the `benchmark-vdf` script
  - When running `benchmark-vdf` you can specify the `verify true` flag to have the script verify the VDF output against a slower "debug" VDF algorithm.
- Support CMake 4 on MacOS
- Bug fixes to address `chunk_not_found` and `sub_chunk_mismatch` errors. 

## Community involvement

A huge thank you to all the Mining community members who contributed to this release by testing the alpha releases, providing feedback, and helping us debug issues!

Discord users (alphabetical order):
- AraAraTime
- BerryCZ
- bigbang
- BloodHunter
- Butcher_
- core_1_
- dlmx
- doesn't stay up late
- dzeto
- edzo
- Evalcast
- EvM
- Fox Malder
- grumpy.003
- hihui
- Iba Shinu
- JanP
- JamsJun
- JF
- jimmyjoe7768
- lawso2517
- MaSTeRMinD
- MCB
- Merdi Kim
- metagravity
- Methistos
- Michael | Artifact
- mousetu
- Niiiko
- qq87237850
- Qwinn
- radion_nizametdinov
- RedMOoN
- sam
- sk
- smash
- sumimi
- T777
- tashilo
- Thaseus
- U genius
- Vidiot
- Wednesday
- wybiacx
