%%% @doc Checks the application boundary rule: an Arweave application calls
%%% arweave_lib directly and reaches every other Arweave application through
%%% its own deps module. The host (arweave), the tools built on it
%%% (arweave_tools) and the simulator (arweave_sim) wire the deps together,
%%% so they are not checked.
-module(arweave_lib_boundaries_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 120}}].

all() ->
    [
        apps_call_other_apps_through_deps
    ].

%% Applications that wire the deps together rather than going through them.
-define(EXEMPT_APPS, [arweave, arweave_tools, arweave_sim, arweave_lib]).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc No checked application calls a module of another Arweave application
%% other than arweave_lib.
apps_call_other_apps_through_deps(_) ->
    Apps = project_apps(),
    ModuleApps = maps:from_list([{Module, App} || App <- Apps,
        Module <- app_modules(App)]),
    Violations = lists:append([direct_calls(App, ModuleApps)
        || App <- Apps -- ?EXEMPT_APPS]),
    ?assertEqual([], Violations).

%%====================================================================
%% Helpers
%%====================================================================

%% The Arweave applications built alongside arweave_lib.
project_apps() ->
    LibRoot = filename:dirname(code:lib_dir(arweave_lib)),
    lists:sort([list_to_atom(Name) || Name <- filelib:wildcard("arweave*",
        LibRoot), filelib:is_dir(filename:join([LibRoot, Name, "ebin"]))]).

app_modules(App) ->
    [list_to_atom(filename:basename(Beam, ".beam"))
        || Beam <- filelib:wildcard(filename:join(ebin(App), "*.beam"))].

%% Return {Caller, Callee} for every call from App into a module of another
%% Arweave application other than arweave_lib.
direct_calls(App, ModuleApps) ->
    {ok, Server} = xref:start([{xref_mode, functions}]),
    try
        xref:set_default(Server, [{verbose, false}, {warnings, false}]),
        {ok, _} = xref:add_directory(Server, ebin(App)),
        {ok, Calls} = xref:q(Server, "XC"),
        lists:usort([{From, To} || {From, {Module, _, _} = To} <- Calls,
            is_foreign(App, maps:get(Module, ModuleApps, none))])
    after
        xref:stop(Server)
    end.

ebin(App) ->
    filename:join(code:lib_dir(App), "ebin").

is_foreign(_App, none) -> false;
is_foreign(_App, arweave_lib) -> false;
is_foreign(App, App) -> false;
is_foreign(_App, _Other) -> true.
