-module(ar_parser).

-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").
-export([eval/1]).


%%% Functions for parsing logical expressions

%% @doc Evaluate a logical expression. Expressions can take three forms
%% {and, EXPR1, EXPR2} - the intersection of the sets returned by 'EXRP1' and 'EXPR2'
%% {or, EXPR1, EXPR2} - the union of the sets returned by 'EXPR1' and 'EXPR2'
%% {equals, KEY, VAL} - the set of txs for which key 'KEY' equals val 'VAL'
eval(true) -> true;
eval(false) -> false;
eval([]) -> [];
eval({equals, Key, Value}) ->
    app_search:get_entries(whereis(http_search_node), Key, Value),
    receive
        Resp -> Resp
        after 3000 -> []
    end;
eval({'and',E1,E2}) ->
    sets:to_list(
        sets:intersection(
            sets:from_list(eval(E1)),
            sets:from_list(eval(E2))
        )
    );
eval({'or',E1,E2}) ->
    sets:to_list(
        sets:union(
            sets:from_list(eval(E1)),
            sets:from_list(eval(E2))
        )
    ).
	     