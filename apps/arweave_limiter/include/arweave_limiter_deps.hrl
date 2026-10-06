-include_lib("arweave_lib/include/arweave_lib_constants.hrl").
%% Call a host dependency through arweave_limiter_deps, for example
%% ?DEP(config):get([limiter, GroupID, number_of_workers]).
-define(DEP(Name), (arweave_limiter_deps:Name())).


