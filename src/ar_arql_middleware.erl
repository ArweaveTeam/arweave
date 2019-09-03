-module(ar_arql_middleware).
-behavior(cowboy_middleware).
-include("ar.hrl").
-export([execute/2]).

%%%===================================================================
%%% Cowboy middleware callback.
%%%===================================================================

execute(Req, Env) ->
	case ar_http_iface_server:split_path(cowboy_req:path(Req)) of
		[<<"arql">>] -> handle_arql_request_1(Req, Env);
		_ -> {ok, Req, Env}
	end.

handle_arql_request_1(Req, Env) ->
	case bin_to_json(ar_http_req:body(Req)) of
		{ok, JSON} -> handle_arql_request_2(JSON, Req, Env);
		error -> use_graphql_handler(Req, Env)
	end.

handle_arql_request_2(#{<<"op">> := _}, Req, Env) ->
	{ok, Req, Env};
handle_arql_request_2(_, Req, Env) ->
	use_graphql_handler(Req, Env).

use_graphql_handler(Req, #{ arql_semaphore := Semaphore } = Env) ->
	{
		ok,
		Req,
		Env#{
			handler => ar_graphql_handler,
			handler_opts => #{ arql_semaphore => Semaphore }
		}
	}.

bin_to_json(<<>>) ->
	{ok, #{}};
bin_to_json(Bin) ->
	try
		{ok, jiffy:decode(Bin, [return_maps])}
	catch
		{error, _} -> error
	end.
