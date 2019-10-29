-module(ar_firewall).
-export([start/0, reload/0, scan_tx/1, scan_and_clean_disk/0]).
-include("ar.hrl").
-include("av/av_recs.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Arweave firewall implementation.

%% Stores compiled binary scan objects.
-record(state, {
	tx_blacklist = [], % a set of blacklisted transaction identifiers
	content_sigs = [] % a set of known signatures to filter transaction content against
}).

%% @doc Start a firewall node.
start() ->
	case whereis(?MODULE) of
		undefined ->
			F = do_start(),
			erlang:register(?MODULE, F);
		_ ->
			do_nothing
	end.

reload() ->
	case whereis(?MODULE) of
		undefined ->
			do_nothing;
		Firewall ->
			NewF = do_start(),
			erlang:unregister(?MODULE),
			erlang:register(?MODULE, NewF),
			exit(Firewall, {shutdown, reloading})
	end.

%% @doc Check whether a received TX matches the firewall rules.
scan_tx(TX) ->
	FwServer = whereis(?MODULE),
	Ref = make_ref(),
	FwServer ! {scan_tx, self(), Ref, TX},
	receive
		{scanned_tx, Ref, Response} ->
			Response
	after 30000 ->
		ar:err([{ar_firewall, scan_tx_timeout}, {tx, ar_util:encode(TX#tx.id)}]),
		reject
	end.

scan_and_clean_disk() ->
	PID = self(),
	Ref = make_ref(),
	spawn(
		fun() ->
			do_scan_and_clean_disk(),
			ar:console([{ar_firewall, disk_scan_complete}]),
			PID ! {scan_complete, Ref}
		end
	),
	Ref.

do_scan_and_clean_disk() ->
	TXDir = filename:join(ar_meta_db:get(data_dir), ?TX_DIR),
	case file:list_dir(TXDir) of
		{error, Reason} ->
			ar:err([
				{ar_firewall, scan_and_clean_disk},
				{listing_dir, TXDir},
				{error, Reason}
			]),
			ok;
		{ok, Files} ->
			lists:foreach(
				fun(File) ->
					Filepath = filename:join(TXDir, File),
					case scan_file(Filepath) of
						{reject, TXID} ->
							ar:info([
								{ar_firewall, scan_and_clean_disk},
								{removing_file, File}
							]),
							file:delete(Filepath),
							ok;
						{error, Reason} ->
							ar:warn([
								{ar_firewall, scan_and_clean_disk},
								{loading_file, File},
								{error, Reason}
							]),
							ok;
						{accept, _} ->
							ok
					end
				end,
				Files
			)
	end.

scan_file(File) ->
	case file:read_file(File) of
		{error, Reason} ->
			{error, Reason};
		{ok, Binary} ->
			try
				TX = ar_serialize:json_struct_to_tx(Binary),
				{scan_tx(TX), TX#tx.id}
			catch Type:Pattern ->
				{error, {exception, Type, Pattern}}
			end
	end.

do_start() ->
	spawn(
		fun() ->
			server(#state {
				tx_blacklist = ar_tx_blacklist:load_from_files(ar_meta_db:get(transaction_blacklist_files)),
				content_sigs = av_sigs:load(ar_meta_db:get(content_policy_files))
			})
		end
	).

%% @doc Main firewall server loop.
%% Receives scan requests and returns whether the given transaction and its contents match
%% the set of known 'harmful'/'ignored' signatures.
server(S = #state { tx_blacklist = TXBlacklist, content_sigs = ContentSigs } ) ->
	receive
		{scan_tx, Pid, Ref, TX} ->
			Pid ! {scanned_tx, Ref, scan_transaction(TX, TXBlacklist, ContentSigs)},
			server(S)
	end.

%% @doc Check if a transaction is in the black list and compare its contents against known bad signatures.
scan_transaction(TX, TXBlacklist, ContentSigs) ->
	case ar_tx_blacklist:is_blacklisted(TX, TXBlacklist) of
		true ->
			ar:info([{ar_firewall, reject_tx}, {tx, ar_util:encode(TX#tx.id)}, tx_is_blacklisted]),
			reject;
		false ->
			case ContentSigs of
				{[], no_pattern} ->
					accept;
				_ ->
					verify_content_sigs(TX, ContentSigs)
			end
	end.

verify_content_sigs(TX, ContentSigs) ->
	Tags = lists:foldl(
		fun({K, V}, Acc) ->
			[K, V | Acc]
		end,
		[],
		TX#tx.tags
	),
	ScanList = [TX#tx.data, TX#tx.target | Tags],
	case av_detect:is_infected(ScanList, ContentSigs) of
		{true, MatchedSigs} ->
			ar:info([
				{ar_firewall, reject_tx},
				{tx, ar_util:encode(TX#tx.id)},
				{matches, MatchedSigs}
			]),
			reject;
		false ->
			accept
	end.

%% Tests: ar_firewall

scan_signatures_test() ->
	Sig = #sig{
		name = "Test",
		type = binary,
		data = #binary_sig{
			target_type = "0",
			offset = any,
			binary = <<"badstuff">>
		}
	},
	ContentSigs = {[Sig], binary:compile_pattern([(Sig#sig.data)#binary_sig.binary])},
	TXBlacklist = sets:from_list([<<"badtxid">>]),
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
			?assertEqual(reject, scan_transaction(BadTX, TXBlacklist, ContentSigs))
		end,
		BadTXs
	),
	lists:foreach(
		fun(GoodTX) ->
			?assertEqual(accept, scan_transaction(GoodTX, TXBlacklist, ContentSigs))
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
	{Sigs, _} = av_sigs:load([fixture("test_sig.ndb")]),
	?assertEqual([ExpectedSig], Sigs),
	ar_meta_db:put(content_policy_files, [fixture("test_sig.ndb")]),
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
	{Sigs, _} = av_sigs:load([fixture("test_sig.txt")]),
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
	),
	ar_meta_db:put(content_policy_files, [fixture("test_sig.txt")]),
	ar_firewall:reload(),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<".. BADCONTENT1 ..">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<"..blablaBADCONTENT2 ..">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ data = <<"..BADCONTENT3111..">> })),
	?assertEqual(accept, scan_tx((ar_tx:new())#tx{ data = <<"B A D C ONTENT1 BADCONTENT 2 BADCONTEN T3">> })).

iolist_to_string(IOList) ->
	binary_to_list(iolist_to_binary(IOList)).

blacklist_transaction_test() ->
	ar_meta_db:put(transaction_blacklist_files, [fixture("test_transaction_blacklist.txt")]),
	ar_firewall:reload(),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ id = <<"badtxid1">> })),
	?assertEqual(reject, scan_tx((ar_tx:new())#tx{ id = <<"badtxid2">> })),
	?assertEqual(accept, scan_tx((ar_tx:new())#tx{ id = <<"goodtxid">> })).

scan_and_clean_disk_test_() ->
	{timeout, 30, fun test_scan_and_clean_disk/0}.

test_scan_and_clean_disk() ->
	GoodTXID = <<"goodtxid">>,
	BadTXID = <<"badtxid1">>,
	BadDataTXID = <<"badtxid">>,
	TagName = <<"name">>,
	TagValue = <<"value">>,
	Tags = [{TagName, TagValue}],
	%% Blacklist a transaction, write it to disk and add it to the index.
	ar_meta_db:put(transaction_blacklist_files, [fixture("test_transaction_blacklist.txt")]),
	BadTX = (ar_tx:new())#tx{ id = BadTXID, tags = Tags },
	ar_storage:write_tx(BadTX),
	?assertEqual(BadTXID, (ar_storage:read_tx(BadTXID))#tx.id),
	%% Setup a content policy, write a bad tx to disk and add it to the index.
	ar_meta_db:put(content_policy_files, [fixture("test_sig.txt")]),
	BadTX2 = (ar_tx:new())#tx{ id = BadDataTXID, data = <<"BADCONTENT1">>, tags = Tags },
	ar_storage:write_tx(BadTX2),
	?assertEqual(BadDataTXID, (ar_storage:read_tx(BadDataTXID))#tx.id),
	%% Write a good tx to disk and add it to the index.
	GoodTX = (ar_tx:new())#tx{ id = GoodTXID, tags = Tags },
	ar_storage:write_tx(GoodTX),
	?assertEqual(GoodTXID, (ar_storage:read_tx(GoodTXID))#tx.id),
	%% Write a file that is not a tx to the transaction directory.
	NotTXFile = filename:join([ar_meta_db:get(data_dir), ?TX_DIR, "not_a_tx"]),
	file:write_file(NotTXFile, <<"not a tx">>),
	%% Assert illicit txs and only they are removed by the cleanup procedure.
	reload(),
	Ref = scan_and_clean_disk(),
	receive
		{scan_complete, Ref} ->
			do_nothing
	after 30000 ->
		?assert(false, "The disk scan did not complete after 30 seconds")
	end,
	?assertEqual(GoodTXID, (ar_storage:read_tx(GoodTXID))#tx.id),
	{ok, <<"not a tx">>} = file:read_file(NotTXFile),
	?assertEqual(unavailable, ar_storage:read_tx(BadTXID)),
	?assertEqual(unavailable, ar_storage:read_tx(BadDataTXID)).

fixture(Filename) ->
	filename:dirname(?FILE) ++ "/../test/" ++ Filename.
