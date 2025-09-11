%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_type).
-compile(export_all).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
boolean(<<"true">>) -> {ok, true};
boolean(<<"false">>) -> {ok, false};
boolean("true") -> {ok, true};
boolean("false") -> {ok, false};
boolean(0) -> {ok, false};
boolean(1) -> {ok, true};
boolean(V) -> {error, V}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
ipv4(Binary) when is_binary(Binary) ->
	ipv4(binary_to_list(Binary));
ipv4(List) when is_list(List) ->
	case inet:parse_strict_address(List, inet) of
		{ok, _} ->
			{ok, list_to_binary(List)};
		_Elsewise ->
			{error, List}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
tcp_port(Binary) when is_binary(Binary) ->
	tcp_port(binary_to_integer(Binary));
tcp_port(List) when is_list(List) ->
	tcp_port(list_to_integer(List));
tcp_port(Integer) when is_integer(Integer) ->
	case Integer of
		_ when Integer >= 0, Integer =< 65535 ->
			{ok, Integer};
		_ ->
			{error, Integer}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
path(List) when is_list(List) ->
	path(list_to_binary(List));
path(Binary) when is_binary(Binary) ->
	case filename:validate(Binary) of
		true ->
			{ok, Binary};
		false ->
			{error, Binary}
	end.
