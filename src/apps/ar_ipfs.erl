-module(ar_ipfs).
-export([add/1, get_data_by_hash/1]).
-include("ar.hrl").

-type hash() :: binary().
-type filepath() :: string().

-spec add(filepath()) -> {ok, hash()}.
add(Path) ->
	CmdStr = "ipfs add " ++ Path,
	ar:d({add_cmd, CmdStr}),
	Hash = os:cmd(CmdStr),
	ar:d({add_ret, Hash}),
	{ok, list_to_binary(Hash)}.

-spec get_data_by_hash(hash()) -> {ok, binary()}.
get_data_by_hash(Hash) ->
	CmdStr = "ipfs cat " ++ binary_to_list(Hash),
	ar:d({cat_cmd, CmdStr}),
	Data = os:cmd(CmdStr),
	ar:d({cat_ret, Data}),
	{ok, Data}.
