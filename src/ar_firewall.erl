-module(ar_firewall).
-export([start/0, reload/0, scan_tx/1]).
-include("ar.hrl").
-include("av/av_recs.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Arweave firewall implementation.

%% State definition.
%% Should store compiled binary scan objects.
-record(state, {
	sigs = [] % the set of known signatures to filter against
}).

%% @doc Start a firewall node.
start() ->
	case whereis(default_firewall) of
		undefined ->
			F = spawn(
				fun() ->
					server(#state {
						sigs = av_sigs:all()
					})
				end
			),
			erlang:register(default_firewall, F);
		_ ->
			do_nothing
	end.

reload() ->
	case whereis(default_firewall) of
		undefined ->
			do_nothing;
		Firewall ->
			erlang:unregister(default_firewall),
			exit(Firewall, {shutdown, reloading})
	end,
	start().

%% @doc Check that a received TX does not match the firewall rules.
scan_tx(TX) ->
	FwServer = whereis(default_firewall),
	ar:report([{scanning_object_of_type, tx}]),
	Ref = make_ref(),
	FwServer ! {scan_tx, self(), Ref, TX},
	receive
		{scanned_tx, Ref, Response} ->
			ar:report([{scanned_object_of_type, tx}, {response, Response}]),
			Response
	after 10000 ->
		ar:err("The firewall failed to scan the transaction within 10 seconds; data: ~p", [TX]),
		reject
	end.

%% @doc Main firewall server loop.
%% Receives scan requests and returns whether the given contents matches
%% the set of known 'harmful'/'ignored' signatures.
server(S = #state { sigs = Sigs } ) ->
	receive
		{scan_tx, Pid, Ref, TX} ->
			Pid ! {scanned_tx, Ref, scan_transaction(TX, Sigs)},
			server(S)
	end.

%% @doc Compare a transaction against known bad signatures.
scan_transaction(TX, Sigs) ->
	Tags = lists:foldl(
		fun({K, V}, Acc) ->
			[K,V|Acc]
		end,
		[],
		TX#tx.tags
	),
	ScanList = [TX#tx.data,TX#tx.target|Tags],
	case lists:any(
		fun(Data) ->
			case av_detect:is_infected(Data, Sigs) of
				{true, _} ->
					io:format("Infected! ~p~n", [Data]),
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

blacklist_transaction_test() ->
	Sigs = #sig{
		name = "Test",
		type = binary,
		data = #binary_sig{
			target_type = "0",
			offset = any,
			binary = <<"badstuff">>
		}
	},
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
		TX#tx{ target = <<"badstuff">> }
	],
	lists:foreach(
		fun(BadTX) ->
			?assertEqual(reject, scan_transaction(BadTX, [Sigs]))
		end,
		BadTXs
	),
	lists:foreach(
		fun(GoodTX) ->
			?assertEqual(accept, scan_transaction(GoodTX, [Sigs]))
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
	ar_meta_db:put(content_policies, ["test/test_sig.ndb"]),
	Sigs = av_sigs:all(),
	?assertEqual([ExpectedSig], Sigs).

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
	ar_meta_db:put(content_policies, ["test/test_sig.txt"]),
	Sigs = av_sigs:all(),
	lists:foreach(
		fun(ExpectedSig) ->
			?assert(
				lists:member(ExpectedSig, Sigs),
				iolist_to_string(
					io_lib:format("The binary signature ~p is missing", [(ExpectedSig#sig.data)#binary_sig.binary])
				)
			)
		end,
		ExpectedSigs
	).

iolist_to_string(IOList) ->
	binary_to_list(iolist_to_binary(IOList)).
