-module(ar_localnet_mining_server).

-behaviour(ar_mining_server_behaviour).
-behaviour(gen_server).

-export([start_link/0, start_mining/1, pause/0, is_paused/0, set_difficulty/1,
	set_merkle_rebase_threshold/1, set_height/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_mining.hrl").
-include("ar_vdf.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-record(state, {
	paused = true,
	difficulty = {infinity, infinity},
	merkle_rebase_threshold = infinity,
	height = 0
}).

-define(RETRY_MINE_DELAY_MS, 1000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_mining(Args) ->
	gen_server:cast(?MODULE, {start_mining, Args}).

pause() ->
	gen_server:cast(?MODULE, pause).

is_paused() ->
	gen_server:call(?MODULE, is_paused).

set_difficulty(DiffPair) ->
	gen_server:cast(?MODULE, {set_difficulty, DiffPair}).

set_merkle_rebase_threshold(Threshold) ->
	gen_server:cast(?MODULE, {set_merkle_rebase_threshold, Threshold}).

set_height(Height) ->
	gen_server:cast(?MODULE, {set_height, Height}).


%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{}}.

handle_call(is_paused, _From, State) ->
	{reply, State#state.paused, State};
handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({start_mining, _Args}, #state{ paused = false } = State) ->
	{noreply, State};
handle_cast({start_mining, {DiffPair, RebaseThreshold, Height}}, State) ->
	gen_server:cast(self(), mine),
	{noreply, State#state{
		paused = false,
		difficulty = DiffPair,
		merkle_rebase_threshold = RebaseThreshold,
		height = Height
	}};

handle_cast(pause, State) ->
	ar:console("Pausing localnet mining.~n"),
	{noreply, State#state{ paused = true }};

handle_cast({set_difficulty, DiffPair}, State) ->
	{noreply, State#state{ difficulty = DiffPair }};

handle_cast({set_merkle_rebase_threshold, Threshold}, State) ->
	{noreply, State#state{ merkle_rebase_threshold = Threshold }};

handle_cast({set_height, Height}, State) ->
	{noreply, State#state{ height = Height }};

handle_cast(mine, #state{ paused = true } = State) ->
	{noreply, State};
handle_cast(mine, State) ->
	case mine_block(State) of
		ok ->
			{noreply, State#state{ paused = true }};
		error ->
			erlang:send_after(?RETRY_MINE_DELAY_MS, self(), retry_mine),
			{noreply, State}
	end;

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(retry_mine, State) ->
	gen_server:cast(self(), mine),
	{noreply, State};
handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Internal functions.
%%%===================================================================

mine_block(State) ->
	{ok, Config} = arweave_config:get_env(),
	MiningAddr = Config#config.mining_addr,
	StorageModules = Config#config.storage_modules,
	mine_block2(pick_random_storage_module(StorageModules), State, MiningAddr, StorageModules).

mine_block2(error, _State, _MiningAddr, _StorageModules) ->
	?LOG_ERROR([{event, failed_to_create_localnet_block}, {step, sample_storage_module}, {reason, all_storage_modules_empty}]),
	error;
mine_block2({StoreID, Intervals}, State, MiningAddr, StorageModules) ->
	mine_block3(sample_chunk_with_proof(StoreID, Intervals, MiningAddr), State, MiningAddr, StorageModules).

mine_block3({error, Error}, _State, _MiningAddr, _StorageModules) ->
	?LOG_ERROR([{event, failed_to_create_localnet_block}, {step, sample_chunk_with_proof}, {reason, io_lib:format("~p", [Error])}]),
	error;
mine_block3({RecallByte1, _Chunk1, PoA1}, State, MiningAddr, StorageModules) ->
	NoncesPerChunk = ar_block:get_nonces_per_chunk(?REPLICA_2_9_PACKING_DIFFICULTY),
	Nonce = rand:uniform(NoncesPerChunk) - 1,
	SubChunk1 = get_sub_chunk(PoA1#poa.chunk, Nonce, ?REPLICA_2_9_PACKING_DIFFICULTY),
	Stage1Data = #{
		recall_byte1 => RecallByte1,
		poa1 => PoA1#poa{ chunk = SubChunk1 },
		nonce => Nonce
	},
	IsTwoChunk = rand:uniform(2) == 2,
	mine_block4(IsTwoChunk, Stage1Data, State, MiningAddr, StorageModules).

mine_block4(false, Stage1Data, State, MiningAddr, _StorageModules) ->
	mine_block7(Stage1Data, one_chunk, State, MiningAddr);
mine_block4(true, Stage1Data, State, MiningAddr, StorageModules) ->
	mine_block5(pick_random_storage_module(StorageModules), Stage1Data, State, MiningAddr, StorageModules).

mine_block5(error, Stage1Data, State, MiningAddr, _StorageModules) ->
	mine_block7(Stage1Data, one_chunk, State, MiningAddr);
mine_block5({StoreID2, Intervals2}, Stage1Data, State, MiningAddr, StorageModules) ->
	mine_block6(sample_chunk_with_proof(StoreID2, Intervals2, MiningAddr), Stage1Data, State, MiningAddr, StorageModules).

mine_block6({error, _Error}, Stage1Data, State, MiningAddr, _StorageModules) ->
	mine_block7(Stage1Data, one_chunk, State, MiningAddr);
mine_block6({RecallByte2, _Chunk2, PoA2}, Stage1Data, State, MiningAddr, _StorageModules) ->
	#{ nonce := Nonce } = Stage1Data,
	SubChunk2 = get_sub_chunk(PoA2#poa.chunk, Nonce, ?REPLICA_2_9_PACKING_DIFFICULTY),
	Stage2Data = #{
		recall_byte2 => RecallByte2,
		poa2 => PoA2#poa{ chunk = SubChunk2 }
	},
	mine_block7(Stage1Data, Stage2Data, State, MiningAddr).

mine_block7(Stage1Data, Stage2Data, State, MiningAddr) ->
	[{_, TipNonceLimiterInfo}] = ets:lookup(node_state, nonce_limiter_info),
	PrevStepNumber = TipNonceLimiterInfo#nonce_limiter_info.global_step_number,
	SessionKey = ar_nonce_limiter:session_key(TipNonceLimiterInfo),
	case ar_nonce_limiter:get_session(SessionKey) of
		not_found ->
			?LOG_ERROR([
				{event, localnet_nonce_limiter_session_not_found},
				{session_key, ar_nonce_limiter:encode_session_key(SessionKey)},
				{prev_step_number, PrevStepNumber}
			]),
			error;
		#vdf_session{} = Session ->
			mine_block7_with_session(
				Session,
				SessionKey,
				PrevStepNumber,
				TipNonceLimiterInfo,
				Stage1Data,
				Stage2Data,
				State,
				MiningAddr
			)
	end.

mine_block7_with_session(
	Session,
	SessionKey,
	PrevStepNumber,
	TipNonceLimiterInfo,
	Stage1Data,
	Stage2Data,
	State,
	MiningAddr
) ->
	{NextSeed, StartIntervalNumber, NextVDFDifficulty} = SessionKey,
	{StepNumber, Output, Seed, Checkpoints, Steps} =
		case Session#vdf_session.step_number == PrevStepNumber of
			true ->
				{
					PrevStepNumber,
					TipNonceLimiterInfo#nonce_limiter_info.output,
					TipNonceLimiterInfo#nonce_limiter_info.seed,
					TipNonceLimiterInfo#nonce_limiter_info.last_step_checkpoints,
					TipNonceLimiterInfo#nonce_limiter_info.steps
				};
			false ->
				StepNumber0 = Session#vdf_session.step_number,
				{
					StepNumber0,
					hd(Session#vdf_session.steps),
					Session#vdf_session.seed,
					maps:get(StepNumber0, Session#vdf_session.step_checkpoints_map, []),
					Session#vdf_session.steps
				}
		end,
	#{ recall_byte1 := RecallByte1, poa1 := PoA1, nonce := Nonce } = Stage1Data,
	H0 = ar_block:compute_h0(
		Output,
		ar_node:get_partition_number(RecallByte1),
		Seed,
		MiningAddr,
		?REPLICA_2_9_PACKING_DIFFICULTY
	),
	{H1, _} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallByte2, PoA2, SolutionHash} =
		case Stage2Data of
			one_chunk ->
				{undefined, #poa{}, H1};
			#{ recall_byte2 := RecallByte2_0, poa2 := PoA2_0 } ->
				{H2, _} = ar_block:compute_h2(H1, PoA2_0#poa.chunk, H0),
				{RecallByte2_0, PoA2_0, H2}
		end,
	Solution = #mining_solution{
		mining_address = MiningAddr,
		merkle_rebase_threshold = State#state.merkle_rebase_threshold,
		next_seed = NextSeed,
		next_vdf_difficulty = NextVDFDifficulty,
		nonce = Nonce,
		nonce_limiter_output = Output,
		partition_number = ar_node:get_partition_number(RecallByte1),
		partition_upper_bound = ar_node:get_weave_size(),
		poa1 = PoA1,
		poa2 = PoA2,
		recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		seed = Seed,
		solution_hash = SolutionHash,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		packing_difficulty = ?REPLICA_2_9_PACKING_DIFFICULTY,
		replica_format = 1,
		last_step_checkpoints = Checkpoints,
		steps = Steps
	},
	ar_node_worker:found_solution(miner, Solution, undefined, undefined),
	ok.

pick_random_storage_module(StorageModules) ->
	ModulesWithData =
		lists:filtermap(
			fun(Module) ->
				StoreID = ar_storage_module:id(Module),
				Intervals = ar_sync_record:get(ar_data_sync, StoreID),
				case ar_intervals:is_empty(Intervals) of
					true ->
						false;
					false ->
						{true, {StoreID, Intervals}}
				end
			end,
			StorageModules
		),
	case ModulesWithData of
		[] ->
			error;
		_ ->
			lists:nth(rand:uniform(length(ModulesWithData)), ModulesWithData)
	end.

sample_chunk_with_proof(_StoreID, Intervals, MiningAddr) ->
	TotalSize = ar_intervals:sum(Intervals),
	RandomOffset = rand:uniform(TotalSize) - 1,
	List = ar_intervals:to_list(Intervals),
	AbsoluteOffset = find_offset_in_intervals(List, RandomOffset),
	RecallByte = (AbsoluteOffset div ?DATA_CHUNK_SIZE) * ?DATA_CHUNK_SIZE,
	Packing = {replica_2_9, MiningAddr},
	Options = #{ pack => true, packing => Packing, origin => miner },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, Proof} ->
			#{ chunk := PackedChunk, tx_path := TXPath, data_path := DataPath } = Proof,
			case maps:get(unpacked_chunk, Proof, not_found) of
				not_found ->
					#{ tx_root := TXRoot, absolute_end_offset := AbsoluteEndOffset,
						chunk_size := ChunkSize } = Proof,
					case ar_packing_server:unpack(
						Packing, AbsoluteEndOffset, TXRoot, PackedChunk, ChunkSize
					) of
						{ok, UnpackedChunk} ->
							PaddedUnpackedChunk = ar_packing_server:pad_chunk(UnpackedChunk),
							{RecallByte, PackedChunk, #poa{
								chunk = PackedChunk,
								unpacked_chunk = PaddedUnpackedChunk,
								data_path = DataPath,
								tx_path = TXPath
							}};
						Error ->
							{error, Error}
					end;
				UnpackedChunk ->
					PaddedUnpackedChunk = ar_packing_server:pad_chunk(UnpackedChunk),
					{RecallByte, PackedChunk, #poa{
						chunk = PackedChunk,
						unpacked_chunk = PaddedUnpackedChunk,
						data_path = DataPath,
						tx_path = TXPath
					}}
			end;
		Error ->
			{error, Error}
	end.

find_offset_in_intervals([{End, Start} | Rest], Offset) ->
	Len = End - Start,
	case Offset < Len of
		true ->
			Start + Offset;
		false ->
			find_offset_in_intervals(Rest, Offset - Len)
	end.

get_sub_chunk(Chunk, _Nonce, 0) when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	Chunk;
get_sub_chunk(Chunk, Nonce, _PackingDifficulty) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	SubChunkStartOffset = SubChunkSize * Nonce,
	binary:part(Chunk, SubChunkStartOffset, SubChunkSize).
