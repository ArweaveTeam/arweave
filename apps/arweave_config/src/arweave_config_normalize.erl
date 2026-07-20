%%% @doc Configuration normalization pass.
%%%
%%% Walks `arweave_config_options_spec:option_modules/0` and runs
%%% each contributor's optional `normalize/0` callback. Each pass
%%% massages the assembled options registry state: promote legacy shapes,
%%% mirror dependent flags, fill in derived defaults, etc.
%%%
%%% == Semantics ==
%%%
%%% Normalizer return values are ignored. Exceptions are not caught:
%%% a crashing normalizer aborts the normalization pass and propagates
%%% to the caller.
%%%
%%% == When normalization fires ==
%%%
%%% Once at boot, after every input source has been parsed and
%%% before `arweave_config:runtime/0` transitions the system into
%%% runtime mode (which runs `arweave_config_validate:run/0`).
-module(arweave_config_normalize).
-export([run/0]).

-spec run() -> ok.
run() ->
    lists:foreach(
        fun normalize_one/1,
        arweave_config_options_spec:option_modules()).

normalize_one(Module) ->
    case erlang:function_exported(Module, normalize, 0) of
        true -> _ = Module:normalize(), ok;
        false -> ok
    end.
