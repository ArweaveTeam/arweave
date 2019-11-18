-module(ar_graphql_tag).

%% API
-export([execute/4]).
-export([to_tuple/1]).

execute(_, #{ name := Name }, <<"name">>, #{}) ->
	{ok, Name};
execute(_, #{ value := Value }, <<"value">>, #{}) ->
	{ok, Value}.

to_tuple(#{ <<"name">> := Name, <<"value">> := Value }) ->
	{Name, Value}.
