%%% @doc Checks that arweave_lib stays a library that any application may call
%%% without a deps module: it starts no processes, includes no header from
%%% another Arweave application, and makes no call with side effects.
-module(arweave_lib_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        app_dependencies,
        no_processes,
        no_foreign_headers,
        no_side_effect_calls
    ].

init_per_suite(Config) ->
    case application:load(arweave_lib) of
        ok -> ok;
        {error, {already_loaded, arweave_lib}} -> ok
    end,
    Config.

end_per_suite(_Config) ->
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% @doc The library depends only on OTP and the pure third-party libraries it
%% wraps.
app_dependencies(_) ->
    ?assertEqual(
        {ok, [kernel, stdlib, crypto, jiffy, b64fast]},
        application:get_key(arweave_lib, applications)
    ).

%% @doc The library has no application callback and registers no processes.
no_processes(_) ->
    ?assertEqual({ok, []}, application:get_key(arweave_lib, mod)),
    ?assertEqual({ok, []}, application:get_key(arweave_lib, registered)).

%% @doc Every header a library module includes comes from arweave_lib or OTP.
no_foreign_headers(_) ->
    {ok, Modules} = application:get_key(arweave_lib, modules),
    OTPDir = code:lib_dir(),
    Foreign = [{Module, Path} || Module <- Modules,
        Path <- included_files(Module),
        not lists:prefix(OTPDir, Path),
        not is_lib_header(Path)],
    ?assertEqual([], Foreign).

%% @doc No library module calls a function with side effects, or a module
%% chosen at runtime.
no_side_effect_calls(_) ->
    {ok, Server} = xref:start(?MODULE),
    try
        xref:set_default(Server, [{verbose, false}, {warnings, false}]),
        Ebin = filename:join(code:lib_dir(arweave_lib), "ebin"),
        {ok, _} = xref:add_directory(Server, Ebin, [{builtins, true}]),
        {ok, Calls} = xref:q(Server, "XC"),
        ?assertEqual([], [Call || Call <- Calls, is_denied(Call)])
    after
        xref:stop(Server)
    end.

%%====================================================================
%% Helpers
%%====================================================================

included_files(Module) ->
    Beam = code:where_is_file(atom_to_list(Module) ++ ".beam"),
    {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(Beam, [abstract_code]),
    lists:usort([Path || {attribute, _, file, {Path, _}} <- Forms,
        filename:extension(Path) == ".hrl"]).

%% Include paths may be relative to the checkout or point into _build.
is_lib_header(Path) ->
    case lists:reverse(filename:split(Path)) of
        [_File, "include", "arweave_lib" | _] -> true;
        _ -> false
    end.

%% Calls through a variable module are denied, but applying a fun the caller
%% passed in is allowed.
is_denied({_From, {'$M_EXPR', '$F_EXPR', _}}) ->
    false;
is_denied({_From, {'$M_EXPR', _, _}}) ->
    true;
is_denied({{arweave_lib_ets_intervals, _, _}, {ets, _, _}}) ->
    false;
is_denied({{arweave_lib_constants, Function, _}, {persistent_term, Op, _}})
        when Op == put; Op == erase ->
    not lists:prefix("internal_", atom_to_list(Function));
is_denied({_From, {persistent_term, get, _}}) ->
    false;
is_denied({{arweave_lib_util, Function, _}, {erlang, BIF, _}})
        when BIF == spawn_link; BIF == '!' ->
    not lists:member(Function, [pmap, batch_pmap, pfilter]);
is_denied({_From, {erlang, BIF, _}}) ->
    lists:member(BIF, [send, '!', send_after, start_timer, cancel_timer,
        spawn, spawn_link, spawn_monitor, register, unregister, whereis,
        process_info, system_time, monotonic_time, timestamp, now, apply,
        open_port, halt]);
is_denied({_From, {calendar, Function, _}}) ->
    lists:member(Function, [local_time, universal_time]);
is_denied({_From, {Module, _, _}}) ->
    lists:member(Module, [file, filelib, os, ets, dets, persistent_term,
        gen_server, gen_statem, gen_event, supervisor, proc_lib, timer,
        application, inet, gen_tcp, gen_udp, ssl, httpc, io, logger, code,
        net_kernel, rpc, erpc, global]).
