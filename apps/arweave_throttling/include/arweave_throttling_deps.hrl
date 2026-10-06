%% Call a host dependency through arweave_throttling_deps, for example
%% ?DEP(metrics):counter_inc(arweave_throttling_requests_total,
%%     [atom_to_list(GroupID)]).
-define(DEP(Name), (arweave_throttling_deps:Name())).
