%% @doc
%% Cowboy2 handler for exporting prometheus metrics.
%%
%% Scrapes are served from the pre-rendered `ar_metrics_cache'
%% (plain and gzipped), so nothing is collected or formatted on the
%% request thread. Requests the cache can't answer (an uncached registry,
%% a non-text format, or an encoding other than identity/gzip) fall back
%% to a synchronous render.
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
	respond(Request);
gen_response(<<"GET">>, Request) ->
	respond(Request);
gen_response(_, Request) ->
	Request.

respond(Request) ->
	Registry0 = cowboy_req:binding(registry, Request, <<"default">>),
	case prometheus_registry:exists(Registry0) of
		false ->
			cowboy_req:reply(404, #{}, <<"Unknown Registry">>, Request);
		Registry ->
			gen_metrics_response(Registry, Request)
	end.

gen_metrics_response(Registry, Request) ->
	Accept = cowboy_req:header(<<"accept">>, Request, <<"text/plain">>),
	AcceptEncoding = cowboy_req:header(<<"accept-encoding">>, Request, undefined),
	case cached_response(Registry, Accept, AcceptEncoding) of
		{ok, Headers, Body} ->
			cowboy_req:reply(200, Headers, Body, Request);
		fallback ->
			synchronous_response(Registry, Request)
	end.

%% @doc Serve the pre-rendered body when the cache has this registry and
%% the client wants the text format in an encoding we keep ready
%% (identity or gzip). Anything else signals `fallback'.
cached_response(Registry, Accept, AcceptEncoding) ->
	case ar_metrics_cache:lookup(Registry) of
		not_cached ->
			fallback;
		#{content_type := ContentType, identity := Identity, gzip := Gzip} ->
			case ar_metrics_render:is_text_format(Accept)
					andalso ar_metrics_render:negotiate_encoding(AcceptEncoding) of
				<<"gzip">> ->
					{ok, response_headers(ContentType, <<"gzip">>), Gzip};
				<<"identity">> ->
					{ok, response_headers(ContentType, <<"identity">>), Identity};
				_ ->
					fallback
			end
	end.

%% @doc Original synchronous path: collect, format, encode and reply on
%% the request thread. Used for anything the cache can't answer.
synchronous_response(Registry, Request) ->
	GetHeader =
		fun(Name, Default) ->
			cowboy_req:header(iolist_to_binary(Name), Request, Default)
		end,
	{Code, RespHeaders, Body} = ar_metrics_render:reply(Registry, GetHeader),
	Headers = prometheus_cowboy:to_cowboy_headers(RespHeaders),
	Headers2 = maps:merge(?CORS_HEADERS, maps:from_list(Headers)),
	cowboy_req:reply(Code, Headers2, Body, Request).

response_headers(ContentType, Encoding) ->
	Headers = #{
		<<"content-type">> => iolist_to_binary(ContentType),
		<<"content-encoding">> => Encoding
	},
	maps:merge(?CORS_HEADERS, Headers).
