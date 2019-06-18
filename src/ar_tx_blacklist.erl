-module(ar_tx_blacklist).
-export([load_from_files/1, is_blacklisted/2]).
-include("ar.hrl").

load_from_files(Files) ->
	sets:from_list(lists:flatten(lists:map(fun load_from_file/1, Files))).

is_blacklisted(TX, TXBlacklist) ->
	sets:is_element(TX#tx.id, TXBlacklist).

load_from_file(File) ->
	try
		{ok, Binary} = file:read_file(File),
		lists:filtermap(
			fun(TXID) ->
				case TXID of
					<<>> ->
						false;
					TXIDEncoded ->
						{true, ar_util:decode(TXIDEncoded)}
				end
			end,
			binary:split(Binary, <<"\n">>, [global])
		)
	catch Type:Pattern ->
		Warning = [load_transaction_blacklist, {load_file, File}, {exception, {Type, Pattern}}],
		ar:warn(Warning),
		ar:console(Warning),
		[]
	end.
