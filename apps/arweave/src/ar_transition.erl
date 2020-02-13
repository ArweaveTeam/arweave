-module(ar_transition).
-behaviour(gen_server).

-export([start_link/1, get_new_block_index/1, update_block_index/1, reset/0]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include("ar.hrl").

-define(BATCH_SIZE, 1000).

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

reset() ->
	gen_server:cast(?MODULE, reset).

init(_Args) ->
	ar:info([{event, ar_transition_start}]),
	{ok, #{ block_index => not_set }}.

handle_call(get_checkpoint, _From, #{ block_index := not_set } = State) ->
	{reply, [], State};
handle_call(get_checkpoint, _From, #{ checkpoint := Checkpoint } = State) ->
	{reply, Checkpoint, State}.

handle_cast({update_block_index, NewBI}, #{ block_index := not_set } = State) ->
	{GH, _} = lists:last(NewBI),
	Checkpoint = load_checkpoint(GH),
	NewState = State#{
		block_index => NewBI,
		to_go => lists:reverse(NewBI),
		checkpoint => update_checkpoint(Checkpoint, element(1, hd(NewBI))),
		genesis_hash => GH
	},
	gen_server:cast(?MODULE, transition_batch),
	{noreply, NewState};
handle_cast({update_block_index, [{H, _} | _]}, #{ block_index := [{H, _} | _] } = State) ->
	{noreply, State};
handle_cast({update_block_index, NewBI}, State) ->
	#{ checkpoint := Checkpoint, block_index := BI, to_go := ToGo } = State,
	case get_diverged_index(BI, NewBI) of
		{error, no_intersection} ->
			ar:err([
				{event, ar_transition_update_index_failed},
				{reason, indexes_do_not_intersect}
			]),
			{noreply, State};
		{ok, {BaseH, _} = BaseEntry, DivergedIndex} ->
			NewState = State#{
				block_index => NewBI,
				to_go => update_to_go(ToGo, BaseEntry, DivergedIndex),
				checkpoint => update_checkpoint(Checkpoint, BaseH)
			},
			gen_server:cast(?MODULE, transition_batch),
			{noreply, NewState}
	end;

handle_cast(transition_batch, State) ->
	#{ block_index := BI, to_go := ToGo, checkpoint := Checkpoint, genesis_hash := GH } = State,
	{ok, NewCheckpoint, NewToGo} = transition_batch(ToGo, BI, Checkpoint),
	ok = save_checkpoint(GH, NewCheckpoint),
	case NewToGo of
		[] ->
			noop;
		_ ->
			gen_server:cast(?MODULE, transition_batch)
	end,
	NewState = State#{
		to_go => NewToGo,
		checkpoint => NewCheckpoint
	},
	{noreply, NewState};

handle_cast(reset, State) ->
	{noreply, State#{ block_index => not_set }}.

terminate(Reason, _State) ->
	ar:err([{event, ar_transition_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

load_checkpoint(GH) ->
	case file:read_file(checkpoint_location(GH)) of
		{ok, Bin} ->
			ar_serialize:json_struct_to_v2_transition_checkpoint(ar_serialize:dejsonify(Bin));
		_ ->
			[]
	end.

checkpoint_location(GH) ->
	filename:join(ar_meta_db:get(data_dir), "fork_2_0_checkpoint_" ++ binary_to_list(ar_util:encode(GH))).

get_diverged_index(BI, NewBI) when length(BI) > length(NewBI) ->
	get_diverged_index(tl(BI), NewBI);
get_diverged_index(BI, NewBI) ->
	get_diverged_index(BI, NewBI, []).

get_diverged_index(BI, [Entry | Tail] = NewBI, DivergedIndex) when length(NewBI) > length(BI) ->
	get_diverged_index(BI, Tail, [Entry | DivergedIndex]);
get_diverged_index([{H, _} = Entry | _], [{H, _} | _], DivergedIndex) ->
	{ok, Entry, DivergedIndex};
get_diverged_index([_ | BI], [Entry | NewBI], DivergedIndex) ->
	get_diverged_index(BI, NewBI, [Entry | DivergedIndex]);
get_diverged_index([], [], _) ->
	{error, no_intersection}.

update_to_go([], _, DivergedIndex) ->
	DivergedIndex;
update_to_go(ToGo, BaseEntry, DivergedIndex) ->
	lists:reverse(update_to_go_reversed(lists:reverse(ToGo), BaseEntry, DivergedIndex)).

update_to_go_reversed([{BaseH, _} | _] = ToGoReversed, {BaseH, _}, DivergedIndex) ->
	DivergedIndex ++ ToGoReversed;
update_to_go_reversed([_ | ToGoReversed], BaseEntry, DivergedIndex) ->
	update_to_go_reversed(ToGoReversed, BaseEntry, DivergedIndex);
update_to_go_reversed([], _, DivergedIndex) ->
	DivergedIndex.

update_checkpoint([], _) ->
	[];
update_checkpoint([{_, _, BaseH} | _] = Checkpoint, BaseH) ->
	Checkpoint;
update_checkpoint([_ | Checkpoint], BaseH) ->
	update_checkpoint(Checkpoint, BaseH).

transition_batch(_ToGo, [{H, _} | _], [{_, _, H} | Checkpoint]) ->
	{ok, Checkpoint, []};
transition_batch([{H, _} | ToGo], BI, [{_, _, H} | Checkpoint]) ->
	transition_batch(ToGo, BI, Checkpoint, 0);
transition_batch(ToGo, BI, Checkpoint) ->
	transition_batch(ToGo, BI, Checkpoint, 0).

transition_batch(ToGo, _BI, Checkpoint, ?BATCH_SIZE) ->
	ar:info([{event, transitioned_batch}, {size, ?BATCH_SIZE}]),
	{ok, Checkpoint, ToGo};
transition_batch([], _BI, Checkpoint, Count) ->
	ar:info([{event, transitioned_batch}, {size, Count}]),
	{ok, Checkpoint, []};
transition_batch([{H, _} | Rest], BI, Checkpoint, Count) ->
	{ok, NewEntry} = transition_block(H, BI, [{BH, WS} || {BH, WS, _} <- Checkpoint]),
	transition_batch(Rest, BI, [NewEntry | Checkpoint], Count + 1).

transition_block(H, BI, NewBI) ->
	RawB =
		ar_node:get_block(
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
					txs = ar_storage:read_tx(RawB#block.txs),
					poa = ar_poa:generate_2_0(NewBI, 2)
				}),
			B = BV2#block {
				indep_hash = ar_weave:indep_hash_post_fork_2_0(BV2)
			},
			ar:info([
				{event, transitioning_block},
				{hash, ar_util:encode(B#block.indep_hash)}
			]),
			ar_storage:write_block(B),
			{ok, {B#block.indep_hash, B#block.weave_size, RawB#block.indep_hash}}
	end.

save_checkpoint(GH, Checkpoint) ->
	save_checkpoint2(checkpoint_location(GH), Checkpoint).

save_checkpoint2(File, Checkpoint) ->
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
