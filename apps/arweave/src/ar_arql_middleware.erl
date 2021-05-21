-module(ar_arql_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Cowboy middleware callback.
%%%===================================================================

execute(Req, Env) ->
	case ar_http_iface_server:split_path(cowboy_req:path(Req)) of
		[<<"arql">>] -> handle_arql_request_1(Req, Env);
		_ -> {ok, Req, Env}
	end.

handle_arql_request_1(Req, Env) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			case ar_http_req:body(Req, ?MAX_BODY_SIZE) of
				{ok, Body, Req2} ->
					case bin_to_json(Body) of
						{ok, JSON} -> handle_arql_request_2(JSON, Req2, Env);
						error -> use_graphql_handler(Req2, Env)
					end;
				{error, body_size_too_large} ->
					{stop, cowboy_req:reply(413, #{}, <<"Payload too large">>, Req)}
			end;
		false ->
			ErrorJSON = jiffy:encode(#{ error => endpoint_not_enabled }),
			{stop, cowboy_req:reply(421, #{}, ErrorJSON, Req)}
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
