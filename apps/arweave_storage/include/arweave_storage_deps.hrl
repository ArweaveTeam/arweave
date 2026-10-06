-include_lib("arweave_lib/include/arweave_lib_constants.hrl").
%% Call a host dependency through arweave_storage_deps, for example
%% ?DEP(kv):get(Database, Key).
-define(DEP(Name), (arweave_storage_deps:Name())).


