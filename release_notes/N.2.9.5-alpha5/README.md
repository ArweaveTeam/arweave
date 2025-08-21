**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**

This release includes several syncing and mining performance improvements. It passes all automated tests and has undergone a base level of internal testing, but is not considered production ready. We only recommend upgrading if you wish to take advantage of the new performance improvements.

## Performance improvements

In all cases we ran tests on a full-weave solo miner, as well as a full-weave coordinated mining cluster. We believe the observed performance improvements are generalizable to other miners, but, as always, the performance observed by a given miner is often influenced by many factors that we are not been able to test for. TLDR: your mileage may vary.

### Syncing

Improvements to both syncing speed and memory use while syncing. The improvements address some regressions that were reported in the 2.9.5 alphas, but also improve on 2.9.4.1 performance.

### Mining

This release addresses the significant hashrate loss that was observed during Coordinated Mining on the 2.9.5 alphas.

### Syncing + Mining

In our tests using solo as well as coordinated miners configured to mine while syncing many partitions, we observed steady memory use and full expected hashrate. This addresses some regressions that were reported in the 2.9.5 alphas, but also improves on 2.9.4.1 performance. Notably: the same tests run on 2.9.4.1 showed growing memory use, ultimately causing an OOM.

## Community involvement

A huge thank you to all the Mining community members who contributed to this release by identifying and investigating bugs, sharing debug logs and node metrics, and providing guidance on performance tuning!

Discord users (alphabetical order):
- BerryCZ
- Butcher_
- edzo
- Evalcast
- EvM
- JF
- lawso2517
- MaSTeRMinD
- qq87237850
- Qwinn
- radion_nizametdinov
- RedMOoN
- smash
- T777
- Vidiot