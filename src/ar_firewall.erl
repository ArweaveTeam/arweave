-module(ar_firewall).
-export([start/0, reload/0, scan_tx/1]).
-include("ar.hrl").
-include("av/av_recs.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Arweave firewall implementation.

%% Stores compiled binary scan objects.
-record(state, {
	tx_id_sigs = [], % a set of known signatures to filter transaction identifiers against
	content_sigs = [] % a set of known signatures to filter transaction content against
}).

%% @doc Start a firewall node.
start() ->
	Firewall = whereis(default_firewall),
	case Firewall of
		undefined ->
			F = spawn(
				fun() ->
					server(#state {
						tx_id_sigs = av_sigs:load(ar_meta_db:get(transaction_blacklist)),
						content_sigs = av_sigs:load(ar_meta_db:get(content_policies))
					})
				end
			),
			erlang:register(default_firewall, F);
		_ ->
			do_nothing
	end.

reload() ->
	Firewall = whereis(default_firewall),
	case Firewall of
		undefined ->
			do_nothing;
		_ ->
			erlang:unregister(default_firewall),
			exit(Firewall, {shutdown, reloading})
	end,
	start().

%% @doc Check whether a received TX matches the firewall rules.
scan_tx(TX) ->
	FwServer = whereis(default_firewall),
	FwServer ! {scan_tx, self(), TX},
	receive
		{scanned_tx, Response} ->
			Response
	after 10000 ->
		ar:err([{ar_firewall, scan_tx_timeout}, {tx, ar_util:encode(TX#tx.id)}]),
		accept
	end.

%% @doc Main firewall server loop.
%% Receives scan requests and returns whether the given transaction and its contents match
%% the set of known 'harmful'/'ignored' signatures.
server(S = #state { tx_id_sigs = TXIDSigs, content_sigs = ContentSigs } ) ->
	receive
		{scan_tx, Pid, TX} ->
			Pid ! {scanned_tx, scan_transaction(TX, TXIDSigs, ContentSigs)},
			server(S)
	end.

%% @doc Check if a transaction is in the black list and compare its contents against known bad signatures.
scan_transaction(TX, TXIDSigs, ContentSigs) ->
	Tags = lists:foldl(
		fun({K, V}, Acc) ->
			[{K, ContentSigs},{V, ContentSigs}|Acc]
		end,
		[],
		TX#tx.tags
	),
	ScanList = [{ar_util:encode(TX#tx.id), TXIDSigs},{TX#tx.data, ContentSigs},{TX#tx.target, ContentSigs}|Tags],
	case lists:any(
		fun({Data, Sigs}) ->
			case av_detect:is_infected(Data, Sigs) of
				{true, _} ->
					ar:info([{ar_firewall, reject_tx}, {tx, ar_util:encode(TX#tx.id)}, {content, Data}]),
					true;
				_ ->
					false
			end
		end,
		ScanList
	) of
		true ->
			reject;
		false ->
			accept
	end.

%% Tests: ar_firewall

scan_signatures_test() ->
	ContentSigs = {[#sig{
		name = "Test",
		type = binary,
		data = #binary_sig{
			target_type = "0",
			offset = any,
			binary = <<"badstuff">>
		}
	}], no_pattern, no_pattern},
	TXSigs = {[#sig{
		name = "Test",
		type = binary,
		data = #binary_sig{
			target_type = "0",
			offset = any,
			binary = ar_util:encode(<<"badtxid">>)
		}
	}], no_pattern, no_pattern},
	TX = ar_tx:new(),
	GoodTXs = [
		TX#tx{ data = <<"goodstuff">> },
		TX#tx{ tags = [{<<"goodstuff">>, <<"goodstuff">>}] },
		TX#tx{ target = <<"goodstuff">> }
	],
	BadTXs = [
		TX#tx{ data = <<"badstuff">> },
		TX#tx{ tags = [{<<"badstuff">>, <<"goodstuff">>}] },
		TX#tx{ tags = [{<<"goodstuff">>, <<"badstuff">>}] },
		TX#tx{ target = <<"badstuff">> },
		TX#tx{ id = <<"badtxid">>, data = <<"goodstuff">> }
	],
	lists:foreach(
		fun(BadTX) ->
			?assertEqual(reject, scan_transaction(BadTX, TXSigs, ContentSigs))
		end,
		BadTXs
	),
	lists:foreach(
		fun(GoodTX) ->
			?assertEqual(accept, scan_transaction(GoodTX, TXSigs, ContentSigs))
		end,
		GoodTXs
	).

parse_ndb_blacklist_test() ->
	ExpectedSig =
		#sig {
			name = "Test signature",
			type = binary,
			data =
				#binary_sig {
					target_type = "0",
					offset = any,
					binary = <<"BADCONTENT">>
				}
		},
	{Sigs, _, _} = av_sigs:load(["test/test_sig.ndb"]),
	?assertEqual([ExpectedSig], Sigs),
	ar_meta_db:put(content_policies, ["test/test_sig.ndb"]),
	ar_firewall:reload(),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<".. BADCONTENT ..">> })),
	?assertEqual(accept, scan_tx((ar_tx:new())#tx{ data = <<".. B A D C ONTENT ..">> })).

parse_txt_blacklist_test() ->
	ExpectedSigs = [
		#sig {
			type = binary,
			data =
				#binary_sig {
					offset = any,
					binary = <<"BADCONTENT1">>
				}
		},
		#sig {
			type = binary,
			data =
				#binary_sig {
					offset = any,
					binary = <<"BADCONTENT2">>
				}
		},
		#sig {
			type = binary,
			data =
				#binary_sig {
					offset = any,
					binary = <<"BADCONTENT3">>
				}
		}
	],
	{Sigs, _, _ } = av_sigs:load(["test/test_sig.txt"]),
	lists:foreach(
		fun(ExpectedSig) ->
			?assert(
				lists:member(ExpectedSig, Sigs),
				io:format("Expected ~p to be in ~p", [ExpectedSig, Sigs])
			)
		end,
		ExpectedSigs
	),
	ar_meta_db:put(content_policies, ["test/test_sig.txt"]),
	ar_firewall:reload(),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<".. BADCONTENT1 ..">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<"..blablaBADCONTENT2 ..">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<"..BADCONTENT3111..">> })),
	?assertEqual(accept, scan_tx((ar_tx:new())#tx{ data = <<"B A D C ONTENT1 BADCONTENT 2 BADCONTEN T3">> })).

blacklist_transaction_test() ->
	ar_meta_db:put(transaction_blacklist, ["test/test_transaction_blacklist.txt"]),
	ar_firewall:reload(),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ id = <<"badtxid1">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ id = <<"badtxid2">> })),
	?assertEqual(accept, scan_tx((ar_tx:new())#tx{ id = <<"goodtxid">> })).
