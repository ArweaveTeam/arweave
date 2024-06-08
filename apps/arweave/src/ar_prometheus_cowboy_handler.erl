%% @doc
%% Cowboy2 handler for exporting prometheus metrics.
%% @end
-module(ar_prometheus_cowboy_handler).

%% -behaviour(cowboy_handler).

-export([init/2, terminate/3]).

-include_lib("arweave/include/ar.hrl").

%% ===================================================================
%% cowboy_handler callbacks
%% ===================================================================

init(Req, _Opts) ->
	handle(Req).

terminate(_Reason, _Req, _State) ->
	ok.

%% ===================================================================
%% Private functions
%% ===================================================================

handle(Request) ->
	Method = cowboy_req:method(Request),
	Request1 = gen_response(Method, Request),
	{ok, Request1, undefined}.

gen_response(<<"HEAD">>, Request) ->
	Registry0 = cowboy_req:binding(registry, Request, <<"default">>),
	case prometheus_registry:exists(Registry0) of
		false ->
			cowboy_req:reply(404, #{}, <<"Unknown Registry">>, Request);
		Registry ->
			gen_metrics_response(Registry, Request)
	end;
gen_response(<<"GET">>, Request) ->
	Registry0 = cowboy_req:binding(registry, Request, <<"default">>),
	case prometheus_registry:exists(Registry0) of
		false ->
			cowboy_req:reply(404, #{}, <<"Unknown Registry">>, Request);
		Registry ->
			gen_metrics_response(Registry, Request)
	end;
gen_response(_, Request) ->
	Request.

gen_metrics_response(Registry, Request) ->
	URI = true,
	GetHeader =
		fun(Name, Default) ->
			cowboy_req:header(iolist_to_binary(Name),
					Request, Default)
        end,
	{Code, RespHeaders, Body} = prometheus_http_impl:reply(
			#{ path => URI, headers => GetHeader, registry => Registry,
				standalone => false}),
	Headers = prometheus_cowboy:to_cowboy_headers(RespHeaders),
	Headers2 = maps:merge(?CORS_HEADERS, maps:from_list(Headers)),
	cowboy_req:reply(Code, Headers2, Body, Request).
