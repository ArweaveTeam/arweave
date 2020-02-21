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

handle_cast({update_block_index, [{H, _} | _]}, #{ block_index := [{H, _} | _] } = State) ->
	{noreply, State};
handle_cast({update_block_index, NewBI}, State) ->
	#{ checkpoint := Checkpoint } = State,
	case update_to_go(NewBI, Checkpoint, []) of
		{error, no_intersection} ->
			ar:err([
				{event, ar_transition_update_block_index_failed},
				{reason, hash_lists_do_not_intersect}
			]),
			{noreply, State};
		{ok, NewCheckpoint, ToGo} ->
			NewState = State#{
				block_index => NewBI,
				to_go => ToGo,
				checkpoint => NewCheckpoint
			},
			gen_server:cast(?MODULE, transition_block),
			{noreply, NewState}
	end;

handle_cast(transition_block, State) ->
	#{
		block_index := BI,
		to_go := [{H, _} | NewToGo],
		checkpoint := Checkpoint
	} = State,
	{ok, NewEntry} = transition_block(H, BI, [{BH, WS} || {BH, WS, _} <- Checkpoint]),
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
	{noreply, NewState};

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

update_to_go(BI, [], _) ->
	{ok, [], lists:reverse(BI)};
update_to_go([{H, _} | _], [{_, _, H} | _] = Checkpoint, ToGo) ->
	{ok, Checkpoint, ToGo};
update_to_go([Entry | Tail] = BI, Checkpoint, ToGo) when length(BI) > length(Checkpoint) ->
	update_to_go(Tail, Checkpoint, [Entry | ToGo]);
update_to_go([Entry | BI], [_ | Checkpoint], ToGo) ->
	update_to_go(BI, Checkpoint, [Entry | ToGo]);
update_to_go([], [], _) ->
	{error, no_intersection}.

transition_block(H, BI, NewBI) ->
	RawB =
		ar_node_utils:get_full_block(
			ar_bridge:get_remote_peers(whereis(http_bridge_node)),
			H,
			BI
		),
	case ar_block:verify_indep_hash(RawB) of
		false ->
			ar:err([
				{event, transition_failed},
				{reason, invalid_hash},
				{hash, ar_util:encode(H)}
			]),
			error;
		true ->
			BV2 = ar_block:generate_tx_tree(
				RawB#block{
					poa = ar_poa:generate_2_0(NewBI, 2)
				}),
			B = BV2#block {
				indep_hash = ar_weave:indep_hash_post_fork_2_0(BV2)
			},
			ar:info([
				{event, transitioning_block},
				{height, B#block.height},
				{v1_hash, ar_util:encode(H)},
				{v2_hash, ar_util:encode(B#block.indep_hash)}
			]),
			ar_storage:write_block(B, do_not_write_wallet_list),
			{ok, {B#block.indep_hash, B#block.weave_size, RawB#block.indep_hash}}
	end.

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
