-module(ar_http_util).
-export([get_tx_content_type/1]).

-include("ar.hrl").

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
