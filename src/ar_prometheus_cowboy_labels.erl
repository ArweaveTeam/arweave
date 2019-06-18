-module(ar_prometheus_cowboy_labels).

-export([label_value/2]).

%%%===================================================================
%%% Prometheus cowboy labels module callback (no behaviour)
%%%===================================================================

label_value(http_method, #{req:=Req}) ->
	normalize_method(cowboy_req:method(Req));
label_value(route, #{req:=Req}) ->
	SplitPath = split_path(cowboy_req:path(Req)),
	name_route(SplitPath);
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

split_path(Path) ->
	lists:filter(
		fun(C) -> C /= <<>> end,
		string:split(Path, <<"/">>, all)
	).

name_route([]) ->
	"/";
name_route([<<"info">>]) ->
	"/info";
name_route([<<"block">>]) ->
	"/block";
name_route([<<"tx">>]) ->
	"/tx";
name_route([<<"peer">>|_]) ->
	"/peer/...";
name_route([<<"arql">>]) ->
	"/arql";
name_route([<<"time">>]) ->
	"/time";
name_route([<<"tx">>, <<"pending">>]) ->
	"/tx/pending";
name_route([<<"tx">>, _Hash, <<"status">>]) ->
	"/tx/{hash}/status";
name_route([<<"tx">>, _Hash]) ->
	"/tx/{hash}";
name_route([<<"tx">>, _Hash, << "data.", _/binary >>]) ->
	"/tx/{hash}/data.{ext}";
name_route([<<"wallet">>]) ->
	"/wallet";
name_route([<<"unsigned_tx">>]) ->
	"/unsigned_tx";
name_route([<<"peers">>]) ->
	"/peers";
name_route([<<"price">>, _SizeInBytes]) ->
	"/price/{bytes}";
name_route([<<"price">>, _SizeInBytes, _Addr]) ->
	"/price/{bytes}/{address}";
name_route([<<"hash_list">>]) ->
	"/hash_list";
name_route([<<"wallet_list">>]) ->
	"/wallet_list";
name_route([<<"wallet">>, _Addr, <<"balance">>]) ->
	"/wallet/{addr}/balance";
name_route([<<"wallet">>, _Addr, <<"last_tx">>]) ->
	"/wallet/{addr}/last_tx";
name_route([<<"wallet">>, _Addr, <<"txs">>]) ->
	"/wallet/{addr}/txs";
name_route([<<"wallet">>, _Addr, <<"txs">>, _EarliestTX]) ->
	"/wallet/{addr}/txs/{earliest_tx}";
name_route([<<"wallet">>, _Addr, <<"deposits">>]) ->
	"/wallet/{addr}/deposits";
name_route([<<"wallet">>, _Addr, <<"deposits">>, _EarliestDeposit]) ->
	"/wallet/{addr}/deposits/{earliest_deposit}";
name_route([<<"block">>, <<"hash">>, _IndepHash]) ->
	"/block/hash/{indep_hash}";
name_route([<<"block">>, <<"height">>, _Height]) ->
	"/block/height/{height}";
name_route([<<"block">>, _Type, _IDBin, _Field]) ->
	"/block/{type}/{id_bin}/{field}";
name_route([<<"block">>, <<"current">>]) ->
	"/block/current";
name_route([<<"current_block">>]) ->
	"/current/block";
name_route([<<"services">>]) ->
	"/services";
name_route([<<"tx">>, _Hash, _Field]) ->
	"/tx/hash/field";
name_route([<<"services">>]) ->
	"/services";
name_route([<<"height">>]) ->
	"/height";
name_route([<<_Hash:43/binary, _MaybeExt/binary>>]) ->
	"/{hash}[.{ext}]";
name_route([<<"metrics">>]) ->
	"/metrics";
name_route(_) ->
	undefined.
