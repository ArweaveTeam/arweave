%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_disk_cache).

-behaviour(gen_server).

-export([lookup_block_filename/1, lookup_tx_filename/1, write_block/1, write_block_shadow/1,
		lookup_wallet_list_chunk_filename/2, write_wallet_list_chunk/3, reset/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
		code_change/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

-define(DISK_CACHE_DIR, "disk_cache").
-define(DISK_CACHE_BLOCK_DIR, "blocks").
-define(DISK_CACHE_TX_DIR, "txs").
-define(DISK_CACHE_WALLET_LIST_DIR, "wallet_lists").

%% Internal state definition.
-record(state, {
	limit_max,
	limit_min,
	size = 0,
	path,
	block_path,
	tx_path,
	wallet_list_path
}).

%%%===================================================================
%%% API
%%%===================================================================

lookup_block_filename(Hash) when is_binary(Hash)->
	%% Use the process dictionary to keep the path.
	PathBlock = case get(ar_disk_cache_path) of
		undefined ->
			{ok, Config} = application:get_env(arweave, config),
			Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
			put(ar_disk_cache_path, Path),
			filename:join(Path, ?DISK_CACHE_BLOCK_DIR);
		Path ->
			filename:join(Path, ?DISK_CACHE_BLOCK_DIR)
	end,
	FileName = binary_to_list(ar_util:encode(Hash)) ++ ".json",
	File = filename:join(PathBlock, FileName),
	case filelib:is_file(File) of
		true ->
			{ok, File};
		_ ->
			unavailable
	end.

lookup_tx_filename(Hash) when is_binary(Hash) ->
	PathTX = case get(ar_disk_cache_path) of
		undefined ->
			{ok, Config} = application:get_env(arweave, config),
			Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
			put(ar_disk_cache_path, Path),
			filename:join(Path, ?DISK_CACHE_TX_DIR);
		Path ->
			filename:join(Path, ?DISK_CACHE_TX_DIR)
	end,
	FileName = binary_to_list(ar_util:encode(Hash)) ++ ".json",
	File = filename:join(PathTX, FileName),
	case filelib:is_file(File) of
		true ->
			{ok, File};
		_ ->
			unavailable
	end.

write_block_shadow(B) ->
	case catch gen_server:call(?MODULE, {write_block, B}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

write_block(B) ->
	BShadow = B#block{ txs = [TX#tx.id || TX <- B#block.txs] },
	case catch gen_server:call(?MODULE, {write_block, BShadow}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		ok ->
			write_txs(B#block.txs);
		Reply ->
			Reply
	end.

write_txs([]) ->
	ok;
write_txs([TX | TXs]) ->
	case catch gen_server:call(?MODULE, {write_tx, TX}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		ok ->
			write_txs(TXs);
		Reply ->
			Reply
	end.

lookup_wallet_list_chunk_filename(RootHash, Position) when is_binary(RootHash) ->
	WalletListPath = case get(ar_disk_cache_path) of
		undefined ->
			{ok, Config} = application:get_env(arweave, config),
			Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
			put(ar_disk_cache_path, Path),
			filename:join(Path, ?DISK_CACHE_WALLET_LIST_DIR);
		Path ->
			filename:join(Path, ?DISK_CACHE_WALLET_LIST_DIR)
	end,
	Name = binary_to_list(ar_util:encode(RootHash))
		++ "-" ++ integer_to_list(Position)
		++ "-" ++ integer_to_list(?WALLET_LIST_CHUNK_SIZE)
		++ ".bin",
	Filename = filename:join(WalletListPath, Name),
	case filelib:is_file(Filename) of
		true ->
			{ok, Filename};
		_ ->
			unavailable
	end.

write_wallet_list_chunk(RootHash, Range, Position) ->
	case catch gen_server:call(?MODULE, {write_wallet_list_chunk, RootHash, Range, Position}) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

reset() ->
	gen_server:call(?MODULE, reset).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
	BlockPath = filename:join(Path, ?DISK_CACHE_BLOCK_DIR),
	TXPath = filename:join(Path, ?DISK_CACHE_TX_DIR),
	WalletListPath = filename:join(Path, ?DISK_CACHE_WALLET_LIST_DIR),
	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
	ok = filelib:ensure_dir(WalletListPath ++ "/"),
	Size =
		filelib:fold_files(
			Path,
			"(.*\\.json$)|(.*\\.bin$)",
			true,
			fun(F,Acc) -> filelib:file_size(F) + Acc end,
			0
		),
	LimitMax = Config#config.disk_cache_size * 1048576, % MB to Bytes.
	LimitMin = trunc(LimitMax * (100 - ?DISK_CACHE_CLEAN_PERCENT_MAX) / 100),
	State = #state{
		limit_max = LimitMax,
		limit_min = LimitMin,
		size = Size,
		path = Path,
		block_path = BlockPath,
		tx_path = TXPath,
		wallet_list_path = WalletListPath
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({write_block, B}, _From, State) ->
	Name = binary_to_list(ar_util:encode(B#block.indep_hash)) ++ ".json",
	File = filename:join(State#state.block_path, Name),
	JSONStruct = ar_serialize:block_to_json_struct(B),
	Data = ar_serialize:jsonify(JSONStruct),
	Size = State#state.size + byte_size(Data),
	case ar_storage:write_file_atomic(File, Data) of
		ok ->
			?LOG_DEBUG(
				"Added block to cache. file (~p bytes) ~p. Cache size: ~p",
				[byte_size(Data), Name, Size]),
			gen_server:cast(?MODULE, may_be_clean_up),
			{reply, ok, State#state{ size = Size }};
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_store_block_in_disk_cache},
				{reason, io_lib:format("~p", [Reason])}]),
			{reply, Error, State}
	end;

handle_call({write_tx, TX}, _From, State) ->
	Name = binary_to_list(ar_util:encode(TX#tx.id)) ++ ".json",
	File = filename:join(State#state.tx_path, Name),
	TXHeader = case TX#tx.format of 1 -> TX; 2 -> TX#tx{ data = <<>> } end,
	JSONStruct = ar_serialize:tx_to_json_struct(TXHeader),
	Data = ar_serialize:jsonify(JSONStruct),
	Size = State#state.size + byte_size(Data),
	case ar_storage:write_file_atomic(File, Data) of
		ok ->
			?LOG_DEBUG("Added tx to cache. file (~p bytes) ~p. Cache size: ~p",
				[byte_size(Data), Name, Size]),
			gen_server:cast(?MODULE, may_be_clean_up),
			{reply, ok, State#state{ size = Size }};
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_store_transaction_in_disk_cache},
				{reason, io_lib:format("~p", [Reason])}]),
			{reply, Error, State}
	end;

handle_call({write_wallet_list_chunk, RootHash, Range, Position}, _From, State) ->
	Name = binary_to_list(ar_util:encode(RootHash))
		++ "-" ++ integer_to_list(Position)
		++ "-" ++ integer_to_list(?WALLET_LIST_CHUNK_SIZE),
	Binary = term_to_binary(Range),
	File = filename:join(State#state.wallet_list_path, Name ++ ".bin"),
	Size = State#state.size + byte_size(Binary),
	case ar_storage:write_file_atomic(File, Binary) of
		ok ->
			?LOG_DEBUG("Added wallet to cache. file (~p bytes) ~p. Cache size: ~p",
				[byte_size(Binary), Name, Size]),
			gen_server:cast(?MODULE, may_be_clean_up),
			{reply, ok, State#state{ size = Size }};
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_store_wallet_list_chunk_in_disk_cache},
				{reason, io_lib:format("~p", [Reason])}]),
			{reply, Error, State}
	end;

handle_call(reset, _From, State) ->
	Path = State#state.path,
	?LOG_DEBUG("reset disk cache: ~p", [Path]),
	os:cmd("rm -r " ++ Path ++ "/*"),
	BlockPath = filename:join(Path, ?DISK_CACHE_BLOCK_DIR),
	TXPath = filename:join(Path, ?DISK_CACHE_TX_DIR),
	WalletListPath = filename:join(Path, ?DISK_CACHE_WALLET_LIST_DIR),
	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
	ok = filelib:ensure_dir(WalletListPath ++ "/"),
	{reply, ok, State#state{ size = 0 }};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_cast(may_be_clean_up, State) when State#state.size > State#state.limit_max ->
	?LOG_DEBUG("Exceed the limit (~p): ~p", [State#state.limit_max, State#state.size]),
	Files =
		lists:sort(filelib:fold_files(
			State#state.path,
			"(.*\\.json$)|(.*\\.bin$)",
			true,
			fun(F, A) ->
				 [{filelib:last_modified(F), filelib:file_size(F), F} | A]
			end,
			[])
		),
	%% How much space should be cleaned up.
	ToRemove = State#state.size - State#state.limit_min,
	Removed = delete_file(Files, ToRemove, 0),
	Size = State#state.size - Removed,
	{noreply, State#state{ size = Size }};
handle_cast(may_be_clean_up, State) ->
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

delete_file([], _ToRemove, Removed) ->
	Removed;
delete_file(_Files, ToRemove, Removed) when ToRemove < 0 ->
	Removed;
delete_file([{_DateTime, Size, Filename} | Files], ToRemove, Removed) ->
	case file:delete(Filename) of
		ok ->
			?LOG_DEBUG("Clean disk cache. File (~p bytes): ~p", [Size, Filename]),
			delete_file(Files, ToRemove - Size, Removed + Size);
		{error, Reason} ->
			?LOG_ERROR("Can't delete file ~p: ~p", [Filename, Reason]),
			delete_file(Files, ToRemove, Removed)
	end.
