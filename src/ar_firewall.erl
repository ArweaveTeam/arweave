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
	Firewall = whereis(default_firewall),
	case Firewall of
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
	Firewall = whereis(default_firewall),
	case Firewall of
		undefined ->
			do_nothing;
		_ ->
			erlang:unregister(default_firewall),
			exit(Firewall, {shutdown, reloading})
	end,
	start().

%% @doc Check that a received TX does not match the firewall rules.
scan_tx(TX) ->
	FwServer = whereis(default_firewall),
	ar:report([{scanning_object_of_type, tx}]),
	FwServer ! {scan_tx, self(), TX},
	receive
		{scanned_tx, Response} ->
			ar:report([{scanned_object_of_type, tx}, {response, Response}]),
			Response
	end.

%% @doc Main firewall server loop.
%% Receives scan requests and returns whether the given contents matches
%% the set of known 'harmful'/'ignored' signatures.
server(S = #state { sigs = Sigs } ) ->
	receive
		{scan_tx, Pid, Data} ->
			Pid ! {scanned_tx, scan_transaction(Data, Sigs)},
			server(S)
	end.

%% @doc Compare a transaction against known bad signatures.
scan_transaction(TX, Sigs) ->
	case av_detect:is_infected(TX#tx.data, Sigs) of
		{true, _} -> reject;
		false -> accept
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
	?assertEqual(reject, scan_transaction(ar_tx:new(<<"badstuff">>), [Sigs])),
	?assertEqual(accept, scan_transaction(ar_tx:new(<<"goodstuff">>), [Sigs])).

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
				io:format("Expected ~p to be in ~p", [ExpectedSig, Sigs])
			)
		end,
		ExpectedSigs
	).
