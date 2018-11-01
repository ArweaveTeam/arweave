-module(ar_firewall).
-export([start/0, scan/3]).
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

%% @doc Check that a received object does not match the firewall rules.
scan(PID, Type, Data) ->
	ar:report([{scanning_object_of_type, Type}]),
	PID ! {scan, self(), Type, Data},
	receive
		{scanned, _Obj, Response} ->
			ar:report([{scanned_object_of_type, Type}, {response, Response}]),
			Response
	end.

%% @doc Main firewall server loop.
%% Receives scan requests and returns whether the given contents matches
%% the set of known 'harmful'/'ignored' signatures.
server(S = #state { sigs = Sigs } ) ->
	receive
		{scan, PID, Type, Data} ->
			Pass = case Type of
				block -> true;
				tx -> (not scan_transaction(Data, Sigs));
				_ -> false
			end,
			PID ! {scanned, Data, Pass},
			server(S)
	end.

%% @doc Compare a transaction against known bad signatures
%% return true if matched, otherwise return false.
scan_transaction(TX, Sigs) ->
	av_detect:is_infected(TX#tx.data, Sigs).


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
	{true, _} = scan_transaction(ar_tx:new(<<"badstuff">>), [Sigs]),
	false = scan_transaction(ar_tx:new(<<"goodstuff">>), [Sigs]).

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
