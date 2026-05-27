%%% @doc
-module(arweave_config_http_server_SUITE).
-compile([export_all, nowarn_export_all]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
	application:ensure_all_started(gun),
	Config.

end_per_suite(_Config) ->
	application:stop(gun),
	ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	cleanup_config_http_server(),
	ok = arweave_config:stop().

%% The config HTTP server feature is paused — testcases are kept for when
%% it's revived. To re-enable, replace this with the testcase list below:
%%
%% all() ->
%%     [
%%         default,
%%         unix_socket,
%%         socket_cleanup_on_stop,
%%         router_root_path,
%%         router_unknown_path_404,
%%         post_with_invalid_json,
%%         config_http_enabled_starts_server
%%     ].
all() ->
	{skip, "config HTTP server feature is paused; testcases retained for future revival"}.

%%====================================================================
%% Test cases
%%====================================================================

default(_Config) ->
	% the server can be started as child (under
	% arweave_config_sup). The goal is to enable it on demand only
	% if a specific option or environment variable is present.
	arweave_config_http_server:start_as_child(),

	% the whole configuration can be seen using /v0/config
	% end-point
	{ok, 200, D1} = get_path("/v0/config"),
	{true, {success, _}} = is_jsend(D1),

	% any options can be fetched using /v0/config/${option},
	% they are separated by '/'
	{ok, 200, D2} = get_path("/v0/config/debug"),
	{true, {success, false}} = is_jsend(D2),

	% options can be set using a POST method and following the
	% same pattern. At this time, the data sent is untyped (no
	% json support)
	{ok, 200, D3} = post_path("/v0/config/debug", <<"true">>),
	{true,
		{success, #{
				<<"new">> := true,
				<<"old">> := false
			}
		}
	} = is_jsend(D3),

	% when a option was set, the new value should be present.
	{ok, 200, D4} = get_path("/v0/config/debug"),
	{true, {success, true}} = is_jsend(D4),

	% if a bad value is given by the client, an error must be
	% returned, if possible with a message containing the reason.
	{ok, 400, D5} = post_path("/v0/config/debug", <<"random">>),
	{true, {error, _}} = is_jsend(D5),

	% if a option is not present, an error should be returned
	% with the reason
	{ok, 404, D6} = get_path("/v0/config/option/not/found"),
	{true, {error, _}} = is_jsend(D6),

	% arweave environment should be available to the client, at
	% this time, all environment variables are displayed.
	{ok, 200, D7} = get_path("/v0/environment"),
	{true, {success, _}} = is_jsend(D7),

	arweave_config_http_server:stop_as_child(),

	ok.

unix_socket(_Config) ->
	SocketPath = filename:join("/tmp", "./arweave.sock"),
	arweave_config:set([config,http,listen,address], SocketPath),

	arweave_config_http_server:start_link(),
	timer:sleep(500),
	{ok, _} = file:read_file_info(SocketPath),

	arweave_config_http_server:stop(),
	timer:sleep(500),
	{error, enoent} = file:read_file_info(SocketPath),

	{command, "unix socket feature tested"}.

socket_cleanup_on_stop(_Config) ->
	SocketPath = filename:join("/tmp", "arweave_cleanup.sock"),
	%% Make sure no stale file is lying around from a prior run.
	_ = file:delete(SocketPath),

	arweave_config:set([config,http,listen,address], SocketPath),

	{ok, _Pid} = arweave_config_http_server:start_link(),
	timer:sleep(500),
	{ok, _} = file:read_file_info(SocketPath),

	ok = arweave_config_http_server:stop(),
	timer:sleep(500),
	{error, enoent} = file:read_file_info(SocketPath),

	ok.

router_root_path(_Config) ->
	{ok, _Pid} = arweave_config_http_server:start_as_child(),
	Result = get_path("/v0"),
	{ok, Status, _Body} = Result,
	true = (Status =:= 200 orelse Status =:= 404),
	arweave_config_http_server:stop_as_child(),
	ok.

router_unknown_path_404(_Config) ->
	{ok, _Pid} = arweave_config_http_server:start_as_child(),
	{ok, 404, _Body} = get_path("/v0/some/unknown/path"),
	arweave_config_http_server:stop_as_child(),
	ok.

post_with_invalid_json(_Config) ->
	{ok, _Pid} = arweave_config_http_server:start_as_child(),
	{ok, 400, Body} = post_path("/v0/config/debug", <<"{not valid json">>),
	{true, {error, _}} = is_jsend(Body),
	arweave_config_http_server:stop_as_child(),
	ok.

%% Exercises the operator-facing input path end-to-end: bootstrap
%% parses the `--config.http.*' CLI flags, writes them through the
%% options registry, then `runtime/0' flips the lifecycle. The supervisor
%% child registration is explicit because no production code wires
%% the spec value to a running server today — see
%% `arweave_config_http_server:start_as_child/0'.
config_http_enabled_starts_server(_Config) ->
	ok = arweave_config_bootstrap:start([
		"--config.http.listen.port", "0",
		"--config.http.enabled"
	]),
	ok = arweave_config:runtime(),
	{ok, _Pid} = arweave_config_http_server:start_as_child(),
	Children = supervisor:which_children(arweave_config_sup),
	?assertMatch(
		{arweave_config_http_server, _, _, _},
		lists:keyfind(arweave_config_http_server, 1, Children)).

%%====================================================================
%% Helpers
%%====================================================================

get_path(Path) ->
	get_path(Path, #{}).

get_path(Path, Opts) ->
	Host = maps:get(host, Opts, "127.0.0.1"),
	Port = maps:get(port, Opts, 4891),
	% @todo host and port should be defined as macros
	{ok, Pid} = gun:open(Host, Port),
	StreamRef = gun:get(Pid, Path),
	body(Pid, StreamRef).

post_path(Path, Data) ->
	post_path(Path, Data, #{}).

post_path(Path, Data, Opts) ->
	Host = maps:get(host, Opts, "127.0.0.1"),
	Port = maps:get(port, Opts, 4891),
	% @todo host and port should be defined as macros
	{ok, Pid} = gun:open(Host, Port),
	StreamRef = gun:post(Pid, Path, #{}, Data),
	body(Pid, StreamRef).

body(ConnPid, MRef) ->
	receive
		{gun_response, ConnPid, StreamRef, fin, Status, Headers} ->
			{ok, Status, no_data};
		{gun_response, ConnPid, StreamRef, nofin, Status, Headers} ->
			receive_data(
				ConnPid,
				MRef,
				Status,
				StreamRef,
				<<>>
			);
		{'DOWN', MRef, process, ConnPid, Reason} ->
			{error, Reason}
	after 1000 ->
		timeout
	end.

receive_data(ConnPid, MRef, Status, StreamRef, Buffer) ->
	receive
		{gun_data, ConnPid, StreamRef, nofin, Data} ->
			receive_data(
				ConnPid,
				MRef,
				Status,
				StreamRef,
				<<Buffer/binary, Data/binary>>
			 );
		{gun_data, ConnPid, StreamRef, fin, Data} ->
			{ok, Status, <<Buffer/binary, Data/binary>>};
		{'DOWN', MRef, process, ConnPid, Reason} ->
			{error, Reason}
	after 1000 ->
		timeout
	end.

cleanup_config_http_server() ->
	case catch supervisor:which_children(arweave_config_sup) of
		Children when is_list(Children) ->
			case lists:keyfind(arweave_config_http_server, 1, Children) of
				false ->
					ok;
				_ ->
					_ = catch arweave_config_http_server:stop_as_child(),
					ok
			end;
		_ ->
			ok
	end,
	ok.

is_jsend(Data) ->
	try
		jiffy:decode(Data, [return_maps])
	of
		#{
			<<"status">> := <<"success">>,
			<<"data">> := D
		} -> {true, {success, D}};
		#{
			<<"status">> := <<"fail">>,
			<<"data">> := D
		} -> {true, {fail, D}};
		#{
			<<"status">> := <<"error">>,
			<<"message">> := M
		} -> {true, {error, M}};
		_ -> false
	catch
		_:_ -> false
	end.
