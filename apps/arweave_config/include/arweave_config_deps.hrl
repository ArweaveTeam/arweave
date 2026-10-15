%% Call a host dependency through arweave_config_deps, for example
%% ?DEP(logger):start_handler(arweave_debug).
-define(DEP(Name), (arweave_config_deps:Name())).
