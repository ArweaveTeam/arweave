%%%===================================================================
%%% @doc
%%%
%%% ```
%%% erl_call -name a@127.0.0.1 -R -timeout 60 \
%%%   -a 'arweave_config_cli t ["'${*}'"]'
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config_cli).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

init({base64, B}) ->
	Data = base64:decode(B),
	Split = re:split(Data, "[[:blank:]]"),
	parse(Split, #{ data => Data }).

parse([<<"config">>, <<"global">>|Rest], Data) ->
	arweave_config:apply(arweave_config_global, Data);
parse([<<"config">>, <<"storage">>|Rest], Data) ->
	arweave_config:apply(arweave_config_storage, Rest, Data).

