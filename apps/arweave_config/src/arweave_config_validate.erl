%%% @doc Configuration validation pass.
%%%
%%% Walks the `arweave_config_options_spec:option_modules/0` (the same list used by spec
%%% collection) and runs each contributor's `validate/0` callback.
%%% Each spec-contributor module implements `arweave_config_options`,
%%% which requires both `specs/0` and `validate/0`. Validators with no
%%% cross-cutting checks return `ok`.
%%%
%%% == Semantics ==
%%%
%%% Fail-fast. The first `{error, Reason}` aborts and is returned;
%%% subsequent modules do not run. `Reason` is operator-friendly
%%% (binary or list) and ready to print.
%%%
%%% == When validation fires ==
%%%
%%% 1. Once when `arweave_config:runtime/0` transitions the system
%%%    out of load mode. Every input source has finished, so the
%%%    validators see the assembled state before anything else
%%%    proceeds.
%%%
%%% 2. Before every `set` to a `runtime => true` option commits, once
%%%    the system is in runtime mode. The validators run against the
%%%    candidate value (exposed to them as a process-local candidate); if the
%%%    resulting state is invalid the set is rejected and the committed
%%%    store is left untouched.
%%%
%%% Sets during load (before `runtime/0` is called) do not trigger
%%% validation. The post-load check covers them.
-module(arweave_config_validate).
-export([run/0]).

-include_lib("kernel/include/logger.hrl").

-spec run() -> ok | {error, term()}.
run() ->
	run_each(arweave_config_options_spec:option_modules()).

run_each([]) ->
	ok;
run_each([Module | Rest]) ->
	case run_one(Module) of
		ok ->
			run_each(Rest);
		{error, _} = Err ->
			Err
	end.

run_one(Module) ->
	try Module:validate() of
		ok ->
			ok;
		{error, Reason} ->
			{error, Reason};
		Other ->
			?LOG_WARNING(
				"validator ~p returned unexpected value: ~p",
				[Module, Other]),
			{error, {validator_returned, Module, Other}}
	catch
		E:R:S ->
			?LOG_ERROR(
				"validator ~p crashed: ~p:~p ~p",
				[Module, E, R, S]),
			{error, {validator_crash, Module, E, R}}
	end.
