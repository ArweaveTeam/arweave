-module(ar_transition).
-behaviour(gen_server).

-export([start_link/1, get_new_block_index/1, update_block_index/1, remove_checkpoint/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include("ar.hrl").

%%% A module for managing the transition from Arweave v1.x to Arweave 2.x.
%%% For every historical block, it generates a transaction Merkle tree, a
%%% proof of access, and computes the independent hash in the v2 format.

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc Return a new block index corresponding to the given v1 block hash.
get_new_block_index(BH) ->
	case gen_server:call(?MODULE, get_checkpoint) of
		[{_, _, BH} | _] = Checkpoint ->
			[{H, WS} || {H, WS, _} <- Checkpoint];
		_ ->
			timer:sleep(10),
			get_new_block_index(BH)
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
handle_cast(transition_block, State) ->
	#{
		to_go := [{H, _} | NewToGo],
		checkpoint := Checkpoint
	} = State,
	case transition_block(H, [{BH, WS} || {BH, WS, _} <- Checkpoint]) of
		error ->
			timer:apply_after(10, gen_server, cast, [?MODULE, transition_block]),
			{noreply, State};
		{ok, NewEntry} ->
			NewCheckpoint = [NewEntry | Checkpoint],
			ok = save_checkpoint(NewCheckpoint),
			case NewToGo of
				[] ->
					noop;
				_ ->
					gen_server:cast(?MODULE, transition_block)
			end,
			NewState = State#{
				to_go => NewToGo,
				checkpoint => NewCheckpoint
			},
			{noreply, NewState}
	end;

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
			ar_serialize:json_struct_to_v2_transition_checkpoint(ar_serialize:dejsonify(Bin));
		_ ->
			[]
	end.

checkpoint_location() ->
	filename:join(ar_meta_db:get(data_dir), "fork_2_0_checkpoint").

update_to_go({BI, _}, {[], _}, _) ->
	{ok, [], lists:reverse(BI)};
update_to_go({[{H, _} | _], _}, {[{_, _, H} | _] = Checkpoint, _}, ToGo) ->
	{ok, Checkpoint, ToGo};
update_to_go({[Entry | BI], Height}, {Checkpoint, CheckpointHeight}, ToGo) when Height > CheckpointHeight ->
	update_to_go({BI, Height - 1}, {Checkpoint, CheckpointHeight}, [Entry | ToGo]);
update_to_go({BI, Height}, {[_ | Checkpoint], CheckpointHeight}, ToGo) when Height < CheckpointHeight ->
	update_to_go({BI, Height}, {Checkpoint, CheckpointHeight - 1}, ToGo);
update_to_go({[Entry | BI], Height}, {[_ | Checkpoint], CheckpointHeight}, ToGo) ->
	update_to_go({BI, Height}, {Checkpoint, CheckpointHeight}, [Entry | ToGo]);
update_to_go({[], _}, {[], _}, _) ->
	{error, no_intersection}.

transition_block(H, BI) ->
	Peers = ar_node:get_trusted_peers(whereis(http_entrypoint_node)),
	case ar_storage:read_block_shadow(H) of
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
					transition_block(Peers, B, BI)
			end;
		B ->
			transition_block(Peers, B, BI)
	end.

transition_block(Peers, B, BI) ->
	TXs = lists:foldr(
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
		B#block.txs
	),
	case TXs of
		{unavailable, TXID} ->
			ar:err([
				{event, transition_failed},
				{reason, did_not_find_transaction},
				{block, ar_util:encode(B#block.indep_hash)},
				{tx, ar_util:encode(TXID)}
			]),
			error;
		_ ->
			transition_block2(B, TXs, BI)
	end.

transition_block2(B, TXs, BI) ->
	TXsWithDataRoot = lists:map(
		fun(TX) ->
			DataRoot = (ar_tx:generate_chunk_tree(TX))#tx.data_root,
			TX#tx{ data_root = DataRoot }
		end,
		TXs
	),
	lists:foreach(
		fun(TX) ->
			ok = ar_storage:write_tx(TX)
		end,
		TXsWithDataRoot
	),
	BV2ToHash = ar_block:generate_tx_tree(
		B#block{
			poa = ar_poa:generate_2_0(BI, 2)
		},
		ar_block:generate_size_tagged_list_from_txs(TXsWithDataRoot)
	),
	BV2 = BV2ToHash#block {
		indep_hash = ar_weave:indep_hash_post_fork_2_0(BV2ToHash)
	},
	ok = ar_storage:write_block(BV2, do_not_write_wallet_list),
	ar:info([
		{event, transitioned_block},
		{height, B#block.height},
		{v1_hash, ar_util:encode(B#block.indep_hash)},
		{v2_hash, ar_util:encode(BV2#block.indep_hash)}
	]),
	{ok, {BV2#block.indep_hash, BV2#block.weave_size, B#block.indep_hash}}.

save_checkpoint(Checkpoint) ->
	save_checkpoint(checkpoint_location(), Checkpoint).

save_checkpoint(File, Checkpoint) ->
	JSON = ar_serialize:jsonify(ar_serialize:v2_transition_checkpoint_to_json_struct(Checkpoint)),
	write_file_atomic(File, JSON).

write_file_atomic(Filename, Data) ->
	SwapFilename = Filename ++ ".swp",
	case file:write_file(SwapFilename, Data) of
		ok ->
			file:rename(SwapFilename, Filename);
		Error ->
			Error
	end.
