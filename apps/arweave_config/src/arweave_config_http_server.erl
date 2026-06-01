%%% @doc Configuration HTTP Server Interface.
%%%
%%% Cowboy-based REST API that reads and writes configuration options
%%% at runtime. Default TCP port is `4891`. URL paths map to option
%%% keys (`/v1/config/debug` -> `[debug]`). Responses follow the
%%% jsend format (see https://github.com/omniti-labs/jsend).
%%%
%%% == Examples ==
%%%
%%% ```
%%% # get the value of global.debug option
%%% $ curl localhost:4891/v1/config/debug
%%% {"status":"success","data":true}
%%%
%%% # set the value of global.debug option
%%% $ curl localhost:4891/v1/config/global/debug -d false
%%% {"status":"success","data":false}
%%% '''
%%%
%%% === Unix Socket Support ===
%%%
%%% When the configured listen address is a filesystem path instead of
%%% an IP, cowboy listens on a unix socket. This restricts the attack
%%% surface and limits access to users with read/write permission on
%%% the socket file.
%%%
%%% ```
%%% curl \
%%%   --unix-socket ${WORKDIR}/arweave.sock \
%%%   http://localhost/v1/config/...
%%% '''
-module(arweave_config_http_server).
-export([start_link/0, stop/0]).
-export([start_as_child/0, stop_as_child/0]).
-export([init/2]).
-include_lib("kernel/include/logger.hrl").

%% @doc Start cowboy as a child of `arweave_config_sup`.
%%
%% The body is gated behind `-ifdef(AR_TEST)' so production builds
%% cannot launch the configuration HTTP server. Re-enable by removing
%% the guard (and flipping the `[config, http, ...]` specs in
%% `arweave_config_options_config' back to `enabled => true').
-ifdef(AR_TEST).
start_as_child() ->
	Spec = #{
			id => ?MODULE,
			start => {?MODULE, start_link, []},
			type => worker,
			restart => temporary
	},
	supervisor:start_child(arweave_config_sup, Spec).
-else.
start_as_child() ->
	?LOG_WARNING(
		"config HTTP server is not production-ready; "
		"refusing to start (arweave_config_http_server:start_as_child/0)"),
	{error, not_ready_for_launch}.
-endif.

%% @doc Stop cowboy and remove it from `arweave_config_sup`.
stop_as_child() ->
	stop(),
	supervisor:terminate_child(arweave_config_sup, ?MODULE),
	supervisor:delete_child(arweave_config_sup, ?MODULE).

%% @doc Start the arweave config HTTP API.
%%
%% Gated alongside `start_as_child/0' — see the note above. Production
%% callers (including any future static supervisor child_spec wiring
%% via `{?MODULE, start_link, []}') get refused with a log warning.
-ifdef(AR_TEST).
start_link() ->
	DefaultHost = arweave_config:get([config,http,listen,address]),
	DefaultPort = arweave_config:get([config,http,listen,port]),
	TransportOpts =
		case inet:parse_address(binary_to_list(DefaultHost)) of
			{ok, Address} ->
				[
					{port, DefaultPort},
					{ip, Address}
				];
			{error, _} ->
				%% Non-IP address — treat as a unix socket path.
				[
					{ip, {local, DefaultHost}}
				]
		end,
	ProtocolOpts = #{
		env => #{ dispatch => dispatch() }
	},
	cowboy:start_clear(?MODULE, TransportOpts, ProtocolOpts).
-else.
start_link() ->
	?LOG_WARNING(
		"config HTTP server is not production-ready; "
		"refusing to start (arweave_config_http_server:start_link/0)"),
	{error, not_ready_for_launch}.
-endif.

%% @doc Stop the cowboy listener and remove the unix socket if used.
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

dispatch() ->
	cowboy_router:compile([
		{'_', router()}
	]).

headers() ->
	#{ <<"content-type">> => <<"application/json">> }.

router() ->
	[
		{"/v0", ?MODULE, #{}},
		{"/v0/config", ?MODULE, #{}},
		{"/v0/config/[...]", ?MODULE, #{}},
		{"/v0/environment", ?MODULE, #{}}
	].

init(Req = #{ path := <<"/v0/environment">>, method := <<"GET">> }, State) ->
	AsMap = lists:foldl(
		fun(E, Acc) ->
			case re:split(E, "=", [{parts, 2}, {return, binary}]) of
				[K, V] -> Acc#{K => V};
				_ -> Acc
			end
		end,
		#{},
		os:getenv()),
	Headers = headers(),
	Body = encode(jsend(success, AsMap)),
	Reply = cowboy_req:reply(200, Headers, Body, Req),
	{ok, Reply, State};
init(Req = #{ path := <<"/v0/config">> }, State) ->
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

%% @doc Config endpoint.
config(Key, Req, State) ->
	io:format("~p~n", [{Key, Req, State}]),
	Key2 = re:replace(Key, <<"/">>, <<".">>, [global]),
	Key3 = case Key2 of
		_ when is_list(Key2) -> list_to_binary(Key2);
		_ when is_binary(Key2) -> Key2
	end,
	case arweave_config_parser:key(Key3) of
		{ok, Option} ->
			config1(Option, Req, State);
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

config1(Option, Req = #{ method := <<"GET">> }, State) ->
	case arweave_config:get(Option) of
		undefined ->
			NewState = State#{
				status => 404,
				headers => headers(),
				body => jsend(
					error,
					<<"not_found">>
				),
				req => Req
			},
			{ok, NewState};
		Value ->
			NewState = State#{
				status => 200,
				headers => headers(),
				body => jsend(
					success,
					Value
				),
				req => Req
			},
			{ok, NewState}
	end;
config1(Option, Req = #{ method := <<"POST">> }, State) ->
	case cowboy_req:has_body(Req) of
		true ->
			config_post(Option, Req, State);
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

config_post(Option, Req, State) ->
	case cowboy_req:read_body(Req) of
		{ok, Data, Req0} ->
			config_post1(Data, Option, Req0, State);
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

config_post1(Data, Option, Req, State) ->
	OldValue = case arweave_config_options_registry:get(Option) of
		{ok, Value} -> Value;
		_ -> undefined
	end,
	case arweave_config_options_registry:set(Option, Data) of
		{ok, NewValue} ->
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

encode(Data) ->
	jiffy:encode(Data).
