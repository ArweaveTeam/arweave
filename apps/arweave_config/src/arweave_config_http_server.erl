%%%===================================================================
%%%
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
		{"/v1", ?MODULE, #{}},
	 	{"/v1/config", ?MODULE, #{}},
	 	{"/v1/config/[...]", ?MODULE, #{}}
	].

init(Req = #{ path := <<"/v1">> }, State) ->
	% should display the API specifications.
	Reply = cowboy_req:reply(200, #{}, <<>>, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v1/config">> }, State) ->
	% should return the full configuration using different format.
	Reply = cowboy_req:reply(200, #{}, <<>>, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v1/config/", Key/binary>> }, State) ->
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

