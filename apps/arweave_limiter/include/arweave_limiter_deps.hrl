%% Call a host dependency through arweave_limiter_deps, for example
%% ?DEP(config):get([limiter, GroupID, number_of_workers]).
-define(DEP(Name), (arweave_limiter_deps:Name())).
