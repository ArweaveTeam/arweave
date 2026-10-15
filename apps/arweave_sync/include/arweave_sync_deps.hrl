%% Call a host dependency through arweave_sync_deps, for example
%% ?DEP(clock):monotonic_ms().
-define(DEP(Name), (arweave_sync_deps:Name())).
