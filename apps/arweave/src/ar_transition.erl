-module(ar_transition).
-behaviour(gen_server).

-export([start_link/1, get_checkpoint/1, update_block_index/1, remove_checkpoint/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).
-export([transition_block/2]).

-include("ar.hrl").

%%% A module for managing the transition from Arweave v1.x to Arweave 2.x.
%%% For every historical block, it generates a transaction Merkle tree, a
%%% proof of access, and computes the independent hash in the v2 format.

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc Return checkpoint corresponding to the given block hash.
%% Wait until one is built if it's not ready yet.
get_checkpoint(BH) ->
	case gen_server:call(?MODULE, get_checkpoint) of
		[{BH, _, _} | _] = Checkpoint ->
			Checkpoint;
		_ ->
			timer:sleep(10),
			get_checkpoint(BH)
	end.

update_block_index(BI) ->
	gen_server:cast(?MODULE, {update_block_index, BI}).

remove_checkpoint() ->
	gen_server:cast(?MODULE, remove_checkpoint).

init(_Args) ->
	ar:info([{event, ar_transition_start}]),
	{ok, #{ checkpoint => load_checkpoint() }}.

handle_call(get_checkpoint, _From, #{ checkpoint := Checkpoint } = State) ->
	{reply, Checkpoint, State}.

handle_cast({update_block_index, BI}, State) ->
	#{ checkpoint := Checkpoint } = State,
	case update_to_go({BI, length(BI)}, {Checkpoint, length(Checkpoint)}, []) of
		{error, no_intersection} ->
			ar:err([
				{event, ar_transition_update_block_index_failed},
				{reason, hash_lists_do_not_intersect}
			]),
			{noreply, State};
		{ok, NewCheckpoint, ToGo} ->
			NewState = State#{
				to_go => ToGo,
				checkpoint => NewCheckpoint
			},
			gen_server:cast(?MODULE, transition_block),
			{noreply, NewState}
	end;

handle_cast(transition_block, #{ to_go := [] } = State) ->
	{noreply, State};
handle_cast(transition_block, #{ to_go := [{H, not_set, not_set} | NewToGo] } = State) ->
	#{ checkpoint := Checkpoint } = State,
	case transition_block(H) of
		error ->
			timer:apply_after(10, gen_server, cast, [?MODULE, transition_block]),
			{noreply, State};
		{ok, NewEntry} ->
			update_checkpoint(NewEntry, Checkpoint, NewToGo, State)
	end;
handle_cast(transition_block, #{ to_go := [Entry | NewToGo] } = State) ->
	#{ checkpoint := Checkpoint } = State,
	update_checkpoint(Entry, Checkpoint, NewToGo, State);

handle_cast(remove_checkpoint, State) ->
	file:delete(checkpoint_location()),
	{noreply, State#{ checkpoint => [] }}.

terminate(Reason, _State) ->
	ar:err([{event, ar_transition_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

load_checkpoint() ->
	case file:read_file(checkpoint_location()) of
		{ok, Bin} ->
			ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Bin));
		_ ->
			[]
	end.

checkpoint_location() ->
	filename:join(ar_meta_db:get(data_dir), "fork_2_0_checkpoint").

update_to_go({BI, _}, {[], _}, _) ->
	{ok, [], lists:reverse(BI)};
update_to_go({[{H, _, _} | _], _}, {[{H, _, _} | _] = Checkpoint, _}, ToGo) ->
	{ok, Checkpoint, ToGo};
update_to_go({[Entry | BI], Height}, {Checkpoint, CheckpointHeight}, ToGo) when Height > CheckpointHeight ->
	update_to_go({BI, Height - 1}, {Checkpoint, CheckpointHeight}, [Entry | ToGo]);
update_to_go({BI, Height}, {[_ | Checkpoint], CheckpointHeight}, ToGo) when Height < CheckpointHeight ->
	update_to_go({BI, Height}, {Checkpoint, CheckpointHeight - 1}, ToGo);
update_to_go({[Entry | BI], Height}, {[_ | Checkpoint], CheckpointHeight}, ToGo) ->
	update_to_go({BI, Height}, {Checkpoint, CheckpointHeight}, [Entry | ToGo]);
update_to_go({[], _}, {[], _}, _) ->
	{error, no_intersection}.

transition_block(H) ->
	Peers = ar_node:get_trusted_peers(whereis(http_entrypoint_node)),
	MaybeB = case ar_storage:read_block_shadow(H) of
		unavailable ->
			case ar_http_iface_client:get_block_shadow(Peers, H) of
				unavailable ->
					ar:err([
						{event, transition_failed},
						{reason, did_not_find_block},
						{block, ar_util:encode(H)}
					]),
					error;
				{_Peer, B} ->
					B
			end;
		B ->
			B
	end,
	case MaybeB of
		error ->
			ar:err([
				{event, transition_failed},
				{reason, did_not_find_block},
				{block, ar_util:encode(H)}
			]),
			error;
		_ ->
			case get_txs(Peers, MaybeB#block.txs) of
				{unavailable, TXID} ->
					ar:err([
						{event, transition_failed},
						{reason, did_not_find_transaction},
						{block, ar_util:encode(H)},
						{tx, ar_util:encode(TXID)}
					]),
					error;
				TXs ->
					transition_block(MaybeB, TXs)
			end
	end.

get_txs(Peers, TXIDs) ->
	lists:foldr(
		fun
			(_TXID, {unavailable, TXID}) ->
				{unavailable, TXID};
			(TXID, Acc) ->
				case ar_http_iface_client:get_tx(Peers, TXID, []) of
					unavailable ->
						{unavailable, TXID};
					TX ->
						[TX | Acc]
				end
		end,
		[],
		TXIDs
	).

transition_block(B, TXs) ->
	TXsWithDataRoot = lists:map(
		fun(TX) ->
			DataRoot = (ar_tx:generate_chunk_tree(TX))#tx.data_root,
			TX#tx{ data_root = DataRoot }
		end,
		TXs
	),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXsWithDataRoot),
	{TXRoot, _} = ar_merkle:generate_tree(SizeTaggedTXs),
	ar:info([
		{event, transitioned_block},
		{height, B#block.height},
		{hash, ar_util:encode(B#block.indep_hash)}
	]),
	{ok, {B#block.indep_hash, B#block.weave_size, TXRoot}}.

save_checkpoint(Checkpoint) ->
	save_checkpoint(checkpoint_location(), Checkpoint).

save_checkpoint(File, Checkpoint) ->
	JSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(Checkpoint)),
	write_file_atomic(File, JSON).

write_file_atomic(Filename, Data) ->
	SwapFilename = Filename ++ ".swp",
	case file:write_file(SwapFilename, Data) of
		ok ->
			file:rename(SwapFilename, Filename);
		Error ->
			Error
	end.

update_checkpoint(Entry, Checkpoint, ToGo, State) ->
	NewCheckpoint = [Entry | Checkpoint],
	ok = save_checkpoint(NewCheckpoint),
	case ToGo of
		[] ->
			noop;
		_ ->
			gen_server:cast(?MODULE, transition_block)
	end,
	NewState = State#{
		to_go => ToGo,
		checkpoint => NewCheckpoint
	},
	{noreply, NewState}.
