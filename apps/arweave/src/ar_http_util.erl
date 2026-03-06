-module(ar_http_util).

-export([get_tx_content_type/1, arweave_peer/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PRINTABLE_ASCII_REGEX, "^[ -~]*$").

%%%===================================================================
%%% Public interface.
%%%===================================================================

get_tx_content_type(#tx { tags = Tags }) ->
	case lists:keyfind(<<"Content-Type">>, 1, Tags) of
		{<<"Content-Type">>, ContentType} ->
			case is_valid_content_type(ContentType) of
				true -> {valid, ContentType};
				false -> invalid
			end;
		false ->
			none
	end.

%%--------------------------------------------------------------------
%% @doc Check and valid `x-p2p-port' header.
%% @end
%%--------------------------------------------------------------------
-spec arweave_peer(Req) -> Return when
	Req :: cowboy:req(),
	Return :: {A, A, A, A, Port},
	A :: pos_integer(),
	Port :: pos_integer().

arweave_peer(Req) ->
	P2PPort = cowboy_req:header(<<"x-p2p-port">>, Req),
	{IP, _Port} = cowboy_req:peer(Req),
	arweave_peer2(P2PPort, IP).

%%--------------------------------------------------------------------
%% @private
%% @hidden
%%--------------------------------------------------------------------
-spec arweave_peer2(Binary, IP) -> Return when
	Binary :: binary() | undefined,
	IP :: {A, A, A, A},
	Return :: {A, A, A, A, Port},
	A :: pos_integer(),
	Port :: pos_integer().

arweave_peer2(Binary, {A, B, C, D}) when is_binary(Binary) ->
	try
		binary_to_integer(Binary)
	of
		P when P >= 1 andalso P =< 65535 ->
			{A, B, C, D, P};
		_ ->
			{A, B, C, D, ?DEFAULT_HTTP_IFACE_PORT}
	catch
		_:_ ->
			{A, B, C, D, ?DEFAULT_HTTP_IFACE_PORT}
	end;
arweave_peer2(_, {A, B, C, D}) ->
	{A, B, C, D, ?DEFAULT_HTTP_IFACE_PORT}.


%%%===================================================================
%%% Private functions.
%%%===================================================================

is_valid_content_type(ContentType) ->
	case re:run(
		ContentType,
		?PRINTABLE_ASCII_REGEX,
		[dollar_endonly, {capture, none}]
	) of
		match -> true;
		nomatch -> false
	end.

arweave_peer_test() ->
	[
		% an undefined x-p2p-port header should return the
		% default arweave port
		?assertEqual(
			{1,2,3,4, ?DEFAULT_HTTP_IFACE_PORT},
			arweave_peer(#{
				headers => #{},
				peer => {{1,2,3,4}, 1234}
			})
		),

		% 1/TCP port is valid
		?assertEqual(
			{1,2,3,4, 1},
			arweave_peer(#{
				headers => #{ <<"x-p2p-port">> => <<"1">> },
				peer => {{1,2,3,4}, 1234}
			})
		),

		% 65535/TCP port is valid
		?assertEqual(
			{1,2,3,4, 65535},
			arweave_peer(#{
				headers => #{ <<"x-p2p-port">> => <<"65535">> },
				peer => {{1,2,3,4}, 1234}
			})
		),

		% 0/TCP port is invalid
		?assertEqual(
			{1,2,3,4, ?DEFAULT_HTTP_IFACE_PORT},
			arweave_peer(#{
				headers => #{ <<"x-p2p-port">> => <<"0">> },
				peer => {{1,2,3,4}, 1234}
			})
		),

		% 65536/TCP port is invalid
		?assertEqual(
			{1,2,3,4, ?DEFAULT_HTTP_IFACE_PORT},
			arweave_peer(#{
				headers => #{ <<"x-p2p-port">> => <<"65536">> },
				peer => {{1,2,3,4}, 1234}
			})
		),

		% a TCP port must be an integer, if not, a default
		% port is returned.
		?assertEqual(
			{1,2,3,4, ?DEFAULT_HTTP_IFACE_PORT},
			arweave_peer(#{
				headers => #{ <<"x-p2p-port">> => <<"test">> },
				peer => {{1,2,3,4}, 1234}
			})
		)
	].
