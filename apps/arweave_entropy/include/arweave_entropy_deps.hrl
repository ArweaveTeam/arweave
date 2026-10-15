%% Call a host dependency through arweave_entropy_deps, for example
%% ?DEP(packing):get_packing_state().
-define(DEP(Name), (arweave_entropy_deps:Name())).
