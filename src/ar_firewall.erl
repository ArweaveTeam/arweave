-module(ar_firewall).
-export([start/0, scan_tx/2]).
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
	spawn(
		fun() ->
			server(#state {
				sigs = av_sigs:all()
			})
		end
	).

%% @doc Check that a received TX does not match the firewall rules.
scan_tx(FwServer, Data) ->
	ar:report([{scanning_object_of_type, tx}]),
	FwServer ! {scan_tx, self(), Data},
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

load_blacklist_test() ->
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
