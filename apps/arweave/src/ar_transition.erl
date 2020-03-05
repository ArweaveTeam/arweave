-module(ar_transition).
-behaviour(gen_server).

-export([start_link/1, get_checkpoint/1, update_block_index/1, remove_checkpoint/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).
-export([transition_block/2]).

-include("ar.hrl").

%%% A module for managing the transition from Arweave v1.x to Arweave 2.x.
%%% For every historical block, it generates a merkle root of the tree
%%% constructed from the merkle roots of the data chunk trees of its
%%% transactions and pins it to the block index (checkpoint) together
%%% with the current weave size.

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
	schedule_save_checkpoint(),
	schedule_log_status(),
	{ok, #{ checkpoint => load_checkpoint(), saved => true, logged => true }}.

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

handle_cast(save_checkpoint, #{ saved := true } = State) ->
	schedule_save_checkpoint(),
	{noreply, State};
handle_cast(save_checkpoint, #{ checkpoint := Checkpoint } = State) ->
	save_checkpoint(Checkpoint),
	schedule_save_checkpoint(),
	{noreply, State#{ saved => true }};

handle_cast(log_status, #{ logged := true } = State) ->
	schedule_log_status(),
	{noreply, State};
handle_cast(log_status, #{ checkpoint := Checkpoint } = State) ->
	log_status(Checkpoint),
	schedule_log_status(),
	{noreply, State#{ logged => true }};

handle_cast(remove_checkpoint, State) ->
	file:delete(checkpoint_location()),
	{noreply, State#{ checkpoint => [] }}.

terminate(Reason, _State) ->
	ar:err([{event, ar_transition_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

schedule_save_checkpoint() ->
	timer:apply_after(5 * 60 * 1000, gen_server, cast, [?MODULE, save_checkpoint]).

schedule_log_status() ->
	timer:apply_after(4 * 60 * 60 * 1000, gen_server, cast, [?MODULE, log_status]).

load_checkpoint() ->
	case file:read_file(checkpoint_location()) of
		{ok, Bin} ->
			ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Bin));
		{error, enoent} ->
			CommittedCheckpoint = filename:join(code:priv_dir(arweave), "fork_2_0_checkpoint"),
			{ok, _} = file:copy(CommittedCheckpoint, checkpoint_location()),
			load_checkpoint()
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
				case ar_http_iface_client:get_tx(Peers, TXID, maps:new()) of
					not_found ->
						{unavailable, TXID};
					TX ->
						[TX | Acc]
				end
		end,
		[],
		TXIDs
	).

transition_block(B, TXs) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	{TXRoot, _} = ar_merkle:generate_tree(SizeTaggedTXs),
	{ok, {B#block.indep_hash, B#block.weave_size, TXRoot}}.

log_status(Checkpoint) ->
	MR = ar_unbalanced_merkle:block_index_to_merkle_root(Checkpoint),
	ar:info([
		{event, transition_status},
		{height, length(Checkpoint) - 1},
		{merkle_root, ar_util:encode(MR)}
	]).

save_checkpoint(Checkpoint) ->
	save_checkpoint(checkpoint_location(), Checkpoint).

save_checkpoint(File, Checkpoint) ->
	JSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(Checkpoint)),
	ar_storage:write_file_atomic(File, JSON).

update_checkpoint(Entry, Checkpoint, ToGo, State) ->
	NewCheckpoint = [Entry | Checkpoint],
	case ToGo of
		[] ->
			noop;
		_ ->
			gen_server:cast(?MODULE, transition_block)
	end,
	NewState = State#{
		to_go => ToGo,
		checkpoint => NewCheckpoint,
		saved => false,
		logged => false
	},
	{noreply, NewState}.
