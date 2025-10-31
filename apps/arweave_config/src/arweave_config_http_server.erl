%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Configuration HTTP Server Interface.
%%%
%%% This module is using cowboy to handle configuration requests. The
%%% goal is to configure dynamically some values using a web
%%% interface. The default TCP port used is `4891'.
%%%
%%% The interface is using a RESTful like module, where a path is
%%% representing an object.
%%%
%%% A configuration path is converted to a parameter:
%%%
%%% ```
%%% v1/config/debug
%%%
%%% % becomes
%%%
%%% [debug]
%%% '''
%%%
%%% This API is also versionned, the `v0' version is mostly a draft to
%%% see how the different methods are behaving.
%%%
%%% JSON data returned try to follow jsend format.
%%%
%%% see: https://github.com/omniti-labs/jsend
%%%
%%% == Examples ==
%%%
%%% By default, the values being used and returned are raw:
%%%
%%% ```
%%% # get the value of global.debug parameter
%%% $ curl localhost:4891/v1/config/debug
%%% {"status":"success","data":true}
%%%
%%% # set the value of global.debug parameter
%%% $ curl localhost:4891/v1/config/global/debug -d false
%%% {"status":"success","data":false}
%%% '''
%%%
%%% === Unix Socket Support ===
%%%
%%% Arweave Configuration HTTP API can listen to an unix socket
%%% instead of an IP address. If a valid path is given instead of an
%%% IP address, cowboy will listen on this file. When the server is
%%% stopped, this file should be removed.
%%%
%%% One can then use an HTTP client (e.g. curl) to send HTTP requests,
%%% here an example
%%%
%%% ```
%%% curl \
%%%   --unix-socket ${WORKDIR}/arweave.sock \
%%%   http://localhost/v1/config/...
%%% '''
%%%
%%% Enabling the usage of an unix socket restrict the surface attack,
%%% and limit the configuration access to only the user with
%%% read/write access to it. The "authentication" is then based on
%%% UNIX credentials.
%%%
%%% Note: it can also be a good way to offer an interface for a GUI.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_http_server).
-export([start_link/0, stop/0]).
-export([start_as_child/0, stop_as_child/0]).
-export([init/2]).
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc start cowboy as `arweave_config_sup' child.
%% @end
%%--------------------------------------------------------------------
start_as_child() ->
	Spec = #{
			id => ?MODULE,
			start => {?MODULE, start_link, []},
			type => worker,
			restart => temporary
	},
	supervisor:start_child(arweave_config_sup, Spec).

%%--------------------------------------------------------------------
%% @doc stop cowboy from `arweave_config_sup'.
%% @end
%%--------------------------------------------------------------------
stop_as_child() ->
	stop(),
	supervisor:terminate_child(arweave_config_sup, ?MODULE),
	supervisor:delete_child(arweave_config_sup, ?MODULE).

%%--------------------------------------------------------------------
%% @doc start arweave config http api interface.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	{ok, DefaultHost} = arweave_config:get([config,http,api,listen,address]),
	{ok, DefaultPort} = arweave_config:get([config,http,api,listen,port]),
	TransportOpts =
		case inet:parse_address(binary_to_list(DefaultHost)) of
			{ok, Address} ->
				[
					{port, DefaultPort},
					{ip, Address}
				];
			{error, _} ->
				% if it's not an ip address, this is
				% an unix socket.
				[
					{ip, {local, DefaultHost}}
				]
		end,
	ProtocolOpts = #{
		env => #{ dispatch => dispatch() }
	},
	cowboy:start_clear(?MODULE, TransportOpts, ProtocolOpts).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
stop() ->
	ListenAddress = ranch:get_addr(?MODULE),
	cowboy:stop_listener(?MODULE),
	case ListenAddress of
		{local, Address} ->
			?LOG_DEBUG("remove ~p", [Address]),
			file:delete(Address),
			ok;
		_ ->
			ok
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
dispatch() ->
	cowboy_router:compile([
		{'_', router()}
	]).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
headers() ->
	#{ <<"content-type">> => <<"application/json">> }.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
router() ->
	[
		{"/v0", ?MODULE, #{}},
		{"/v0/config", ?MODULE, #{}},
		{"/v0/config/[...]", ?MODULE, #{}},
		{"/v0/environment", ?MODULE, #{}}
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
% @todo returns API specifications
% init(Req = #{ path := <<"/v0">> }, State) ->
% 	Reply = cowboy_req:reply(200, #{}, <<>>, Req),
% 	{ok, Reply, State};
% @todo returns arweave and beam arguments
% init(Req = #{ path := <<"/v0/arguments">> }, State) -> ok;
init(Req = #{ path := <<"/v0/environment">>, method := <<"GET">> }, State) ->
	Environment = arweave_config_environment:get(),
	AsMap = maps:from_list(Environment),
	Headers = headers(),
	Body = encode(
		jsend(
			success,
			AsMap
		)
	),
	Reply = cowboy_req:reply(200, Headers, Body, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v0/config">> }, State) ->
	% @todo: add the configuration from spec (with default value)
	% should return the full configuration using different format.
	Config = arweave_config_store:to_map(),
	Headers = headers(),
	Body = encode(
		jsend(
			success,
			Config
		)
	),
	Reply = cowboy_req:reply(200, Headers, Body, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v0/config/">> }, State) ->
	init(Req#{ path => <<"/v0/config">> }, State);
init(Req = #{ path := <<"/v0/config/", Key/binary>> }, State)
	when Key =/= <<>> ->
		apply_config(Key, Req, State);
init(Req, State) ->
	?LOG_INFO("~p", [{Req, State}]),
	Headers = headers(),
	Body = encode(
		jsend(
			error,
			<<"not found">>
		)
	),
	Reply = cowboy_req:reply(404, Headers, Body, Req),
	{ok, Reply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
apply_config(Key, Req, State) ->
	case config(Key, Req, State) of
		{ok, #{
			status := Status,
			body := Body,
			req := NewReq
		}} ->
			Reply = cowboy_req:reply(
				Status,
				headers(),
				encode(Body),
				NewReq
			),
			{ok, Reply, State};
		_Else ->
			Reply = cowboy_req:reply(
				400,
				headers(),
				encode(
					jsend(
						error,
						<<"configuration error">>
					)
				),
				Req
			),
			{ok, Reply, State}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% config end-point
%%--------------------------------------------------------------------
config(Key, Req, State) ->
	io:format("~p~n", [{Key, Req, State}]),
	Key2 = re:replace(Key, <<"/">>, <<".">>, [global]),
	Key3 = case Key2 of
		_ when is_list(Key2) -> list_to_binary(Key2);
		_ when is_binary(Key2) -> Key2
	end,
	case arweave_config_parser:key(Key3) of
		{ok, Parameter} ->
			config1(Parameter, Req, State);
		_ ->
			NewState = #{
				status => 400,
				headers => headers(),
				body => jsend(
					error,
					<<"bad data">>
				),
				req => Req
			},
			{ok, NewState}
	end.

config1(Parameter, Req = #{ method := <<"GET">> }, State) ->
	case arweave_config:get(Parameter) of
		{ok, Value} ->
			NewState = State#{
				status => 200,
				headers => headers(),
				body => jsend(
					success,
					Value
				),
				req => Req
			},
			{ok, NewState};
		_ ->
			NewState = State#{
				status => 404,
				headers => headers(),
				body => jsend(
					error,
					<<"not_found">>
				),
				req => Req
			},
			{ok, NewState}
	end;
config1(Parameter, Req = #{ method := <<"POST">> }, State) ->
	case cowboy_req:has_body(Req) of
		true ->
			config_post(Parameter, Req, State);
		false ->
			NewState = State#{
				status => 400,
				headers => headers(),
				body => jsend(
					error,
					<<"missing body">>
				),
				req => Req
			},
			{ok, NewState}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
config_post(Parameter, Req, State) ->
	case cowboy_req:read_body(Req) of
		{ok, Data, Req0} ->
			config_post1(Data, Parameter, Req0, State);
		_ ->
			NewState = State#{
				status => 400,
				headers => headers(),
				body => jsend(
					error,
					<<"bad data">>
				),
				req => Req
			},
			{ok, NewState}
	end.

config_post1(Data, Parameter, Req, State) ->
	case arweave_config_spec:set(Parameter, Data) of
		{ok, NewValue, OldValue} ->
			NewState = State#{
				status => 200,
				headers => headers(),
				body => jsend(
					success,
					#{
						new => NewValue,
						old => OldValue
					}
				),
				req => Req
			},
			{ok, NewState};
		_ ->
			NewState = State#{
				status => 400,
				headers => headers(),
				body => jsend(
					error,
					<<"bad data">>
				),
				req => Req
			},
			{ok, NewState}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec jsend(Status, Data) -> Return when
	Status :: success | fail | error,
	Data :: binary() | map() | list() | integer(),
	Return :: #{
		status => success | fail | error,
		data => Data,
		message => Data
	}.

jsend(success, Data) ->
	#{
		status => success,
		data => Data
	};
jsend(fail, Data) ->
	#{
		status => fail,
		data => Data
	};
jsend(error, Message) ->
	#{
		status => error,
		message => Message
	}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
encode(Data) ->
	jiffy:encode(Data).

