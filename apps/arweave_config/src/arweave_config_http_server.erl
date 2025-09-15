%%%===================================================================
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
%%% v1/config/global.debug
%%%
%%% % becomes
%%%
%%% [global, debug]
%%% '''
%%%
%%% This API is also versionned, the `v0' version is mostly a draft to
%%% see how the different methods are behaving.
%%%
%%% == Examples ==
%%%
%%% By default, the values being used and returned are raw:
%%%
%%% ```
%%% # get the value of global.debug parameter
%%% $ curl localhost:4891/v1/config/global/debug
%%% true
%%%
%%% # set the value of global.debug parameter
%%% $ curl localhost:4891/v1/config/global/debug -d false
%%% false
%%% '''
%%%
%%% == TODO ==
%%%
%%% === JSON Support ==
%%%
%%% ```
%%% $ curl -H 'accept: application/json' \
%%%   localhost:4891/v1/config/global/debug
%%% {
%%%   "status": "ok",
%%%   "value": true
%%% }
%%%
%%% $ curl -H 'accept: application/json' \
%%%   -H 'content-type: application/json'
%%%   -X POST \
%%%   -d '{"value": true}'
%%%   localhost:4891/v1/config/global/debug
%%% '''
%%%
%%% or
%%%
%%% ```
%%% $ curl -H 'accept: application/json' \
%%%   -H 'content-type: application/json' \
%%%   -X POST \
%%%   -d '{ "parameter": "global.debug",
%%%         "value": "true" }' \
%%%   localhost:4891/v1/config/global/debug
%%% '''
%%%
%%% Note: Using JSON-RPC could be perhaps a better idea.
%%%
%%% === RESTful API ===
%%%
%%% Cowboy is supporting a RESTful API behavior called `cowboy_rest'.
%%% It could be interesting to set it for this module instead of doing
%%% a complex routing scheme.
%%%
%%% === Unix Socket Support ===
%%%
%%% If the system is running on Unix/Linux systems, the listener
%%% should listen on unix socket by default. This configuration can be
%%% overwritten by setting explicitly the TCP port or the listen
%%% address to a valid ipv4/ipv6 address.
%%%
%%% ```
%%% ${WORKDIR}/config.sock
%%% chmod 600 ${WORKDIR}/config.sock
%%% chown arweave:arweave ${WORKDIR}/config.sock
%%% curl --unix-socket ${WORKDIR}/config.sock localhost/v1/config/...
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
-export([init/2]).
-include_lib("kernel/include/logger.hrl").

start_link() ->
	Opts = [{port, 4891}],
	Env = #{ env => #{ dispatch => dispatch() }},
	cowboy:start_clear(?MODULE, Opts, Env).

stop() ->
	cowboy:stop_listener(?MODULE).

dispatch() -> 
	cowboy_router:compile([
		{'_', router()}
	]).

router() ->
	[
		{"/v0", ?MODULE, #{}},
	 	{"/v0/config", ?MODULE, #{}},
	 	{"/v0/config/[...]", ?MODULE, #{}}
	].

init(Req = #{ path := <<"/v0">> }, State) ->
	% should display the API specifications.
	Reply = cowboy_req:reply(200, #{}, <<>>, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v0/config">> }, State) ->
	% should return the full configuration using different format.
	Reply = cowboy_req:reply(200, #{}, <<>>, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v0/config/", Key/binary>> }, State) ->
	config(Key, Req, State);
init(Req, State) ->
	?LOG_INFO("~p", [{Req, State}]),
	Reply = cowboy_req:reply(404, #{}, <<"not found">>, Req),
	{ok, Reply, State}.

% config end-point
config(Key, Req, State) ->
	Key2 = re:replace(Key, <<"/">>, <<".">>, [global]),
	Key3 = list_to_binary(Key2),
	case arweave_config_parser:key(Key3) of
		{ok, Parameter} ->
			config1(Parameter, Req, State);
		_ ->
			Reply = cowboy_req:reply(400, #{}, <<>>, Req),
			{ok, Reply, State}
	end.

config1(Parameter, Req = #{ method := <<"GET">> }, State) ->
	case arweave_config_store:get(Parameter) of
		{ok, Value} ->
			Encoded = jiffy:encode(Value),
			Reply = cowboy_req:reply(200, #{}, Encoded, Req),
			{ok, Reply, State};
		_ ->
			Reply = cowboy_req:reply(404, #{}, <<"not_found">>, Req),
			{ok, Reply, State}
	end;
config1(Parameter, Req = #{ method := <<"POST">> }, State) ->
	case cowboy_req:has_body(Req) of
		true ->
			config_post(Parameter, Req, State);
		false ->
			Reply = cowboy_req:reply(400, #{}, <<>>, Req),
			{ok, Reply, State}
	end.

config_post(Parameter, Req, State) ->
	case cowboy_req:read_body(Req) of
		{ok, Data, Req0} ->
			config_post1(Data, Parameter, Req, State);
		_ ->
			Reply = cowboy_req:reply(400, #{}, <<>>, Req),
			{ok, Reply, State}
	end.

config_post1(Data, Parameter, Req, State) ->
	case arweave_config_spec:set(Parameter, Data) of
		{ok, V, O} ->
			Encoded = jiffy:encode(V),
			Reply = cowboy_req:reply(200, #{}, Encoded, Req),
			{ok, Reply, State};
		_ ->
			Reply = cowboy_req:reply(400, #{}, <<>>, Req),
			{ok, Reply, State}
	end.

