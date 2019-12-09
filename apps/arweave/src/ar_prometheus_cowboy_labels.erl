-module(ar_prometheus_cowboy_labels).

-export([label_value/2]).

%%%===================================================================
%%% Prometheus cowboy labels module callback (no behaviour)
%%%===================================================================

label_value(http_method, #{req:=Req}) ->
	normalize_method(cowboy_req:method(Req));
label_value(route, #{req:=Req}) ->
	ar_metrics:label_http_path(cowboy_req:path(Req));
label_value(_, _) ->
	undefined.

%%%===================================================================
%%% Private functions.
%%%===================================================================

normalize_method(<<"GET">>) -> 'GET';
normalize_method(<<"HEAD">>) -> 'HEAD';
normalize_method(<<"POST">>) -> 'POST';
normalize_method(<<"PUT">>) -> 'PUT';
normalize_method(<<"DELETE">>) -> 'DELETE';
normalize_method(<<"CONNECT">>) -> 'CONNECT';
normalize_method(<<"OPTIONS">>) -> 'OPTIONS';
normalize_method(<<"TRACE">>) -> 'TRACE';
normalize_method(<<"PATCH">>) -> 'PATCH';
normalize_method(_) -> undefined.
