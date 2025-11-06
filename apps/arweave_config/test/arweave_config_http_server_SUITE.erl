%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_http_server_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	default/1,
	unix_socket/1
]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config http api interface"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_suite(Config) ->
	application:ensure_all_started(gun),
	Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
	application:stop(gun),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		default,
		unix_socket
	].

%%--------------------------------------------------------------------
%% @doc test `arweave_config' main interface.
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	% the server can be started as child (under
	% arweave_config_sup). The goal is to enable it on demand only
	% if a specific parameter or environment variable is present.
	ct:pal(test, 1, "start arweave config http server"),
	arweave_config_http_server:start_as_child(),

	% the whole configuration can be seen using /v0/config
	% end-point
	ct:pal(test, 1, "fetch the whole configuration"),
	{ok, 200, D1} = get_path("/v0/config"),
	{true, {success, _}} = is_jsend(D1),

	% any parameters can be fetched using /v0/config/${parameter},
	% they are separated by '/'
	ct:pal(test, 1, "fetch debug parameter"),
	{ok, 200, D2} = get_path("/v0/config/debug"),
	{true, {success, false}} = is_jsend(D2),

	% parameters can be set using a POST method and following the
	% same pattern. At this time, the data sent is untyped (no
	% json support)
	ct:pal(test, 1, "set debug parameter"),
	{ok, 200, D3} = post_path("/v0/config/debug", <<"true">>),
	{true,
		{success, #{
				<<"new">> := true,
				<<"old">> := false
			}
		}
	} = is_jsend(D3),

	% when a parameter was set, the new value should be present.
	ct:pal(test, 1, "fetch debug parameter"),
	{ok, 200, D4} = get_path("/v0/config/debug"),
	{true, {success, true}} = is_jsend(D4),

	% if a bad value is given by the client, an error must be
	% returned, if possible with a message containing the reason.
	ct:pal(test, 1, "set bad value on parameter"),
	{ok, 400, D5} = post_path("/v0/config/debug", <<"random">>),
	{true, {error, _}} = is_jsend(D5),

	% if a parameter is not present, an error should be returned
	% with the reason
	ct:pal(test, 1, "check unknown parameter"),
	{ok, 404, D6} = get_path("/v0/config/parameter/not/found"),
	{true, {error, _}} = is_jsend(D6),

	% arweave environment should be available to the client, at
	% this time, all environment variables are displayed.
	ct:pal(test, 1, "fetch arweave config environment"),
	{ok, 200, D7} = get_path("/v0/environment"),
	{true, {success, _}} = is_jsend(D7),

	ct:pal(test, 1, "stop config http server"),
	arweave_config_http_server:stop_as_child(),

	{comment, "arweave_config_http_server tested "}.

unix_socket(Config) ->
	SocketPath = filename:join("/tmp", "./arweave.sock"),
	ct:pal(test, 1, "set socket to ~p", [SocketPath]),
	arweave_config:set([config,http,api,listen,address], SocketPath),

	ct:pal(test, 1, "start arweave config http server"),
	arweave_config_http_server:start_link(),
	timer:sleep(500),
	{ok, _} = file:read_file_info(SocketPath),

	ct:pal(test, 1, "stop arweave config http server"),
	arweave_config_http_server:stop(),
	timer:sleep(500),
	{error, enoent} = file:read_file_info(SocketPath),

	{command, "unix socket feature tested"}.

%%--------------------------------------------------------------------
%% simple http client for get request.
%%--------------------------------------------------------------------
get_path(Path) ->
	get_path(Path, #{}).

get_path(Path, Opts) ->
	Host = maps:get(host, Opts, "127.0.0.1"),
	Port = maps:get(port, Opts, 4891),
	% @todo host and port should be defined as macros
	{ok, Pid} = gun:open(Host, Port),
	StreamRef = gun:get(Pid, Path),
	body(Pid, StreamRef).

%%--------------------------------------------------------------------
%% simple http client for post request.
%%--------------------------------------------------------------------
post_path(Path, Data) ->
	post_path(Path, Data, #{}).

post_path(Path, Data, Opts) ->
	Host = maps:get(host, Opts, "127.0.0.1"),
	Port = maps:get(port, Opts, 4891),
	% @todo host and port should be defined as macros
	{ok, Pid} = gun:open(Host, Port),
	StreamRef = gun:post(Pid, Path, #{}, Data),
	body(Pid, StreamRef).

%%--------------------------------------------------------------------
%% from https://ninenines.eu/docs/en/gun/2.1/guide/http/
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% from https://ninenines.eu/docs/en/gun/2.1/guide/http/
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
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
