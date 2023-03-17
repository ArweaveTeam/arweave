%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_disk_cache).

-behaviour(gen_server).

-export([lookup_block_filename/1, lookup_tx_filename/1, write_block/1, write_block_shadow/1,
		reset/0, write_tx/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
		code_change/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

-define(DISK_CACHE_DIR, "disk_cache").
-define(DISK_CACHE_BLOCK_DIR, "blocks").
-define(DISK_CACHE_TX_DIR, "txs").

%% Internal state definition.
-record(state, {
	limit_max,
	limit_min,
	size = 0,
	path
}).

%%%===================================================================
%%% API
%%%===================================================================

lookup_block_filename(H) when is_binary(H)->
	%% Use the process dictionary to keep the path.
	PathBlock =
		case get(ar_disk_cache_path) of
			undefined ->
				{ok, Config} = application:get_env(arweave, config),
				Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
				put(ar_disk_cache_path, Path),
				filename:join(Path, ?DISK_CACHE_BLOCK_DIR);
			Path ->
				filename:join(Path, ?DISK_CACHE_BLOCK_DIR)
		end,
	FileName = binary_to_list(ar_util:encode(H)),
	FilePath = filename:join(PathBlock, FileName),
	FilePathJSON = iolist_to_binary([FilePath, ".json"]),
	case ar_storage:is_file(FilePathJSON) of
		true ->
			{ok, {FilePathJSON, json}};
		_ ->
			FilePathBin = iolist_to_binary([FilePath, ".bin"]),
			case ar_storage:is_file(FilePathBin) of
				true ->
					{ok, {FilePathBin, binary}};
				_ ->
					unavailable
			end
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
	case ar_storage:is_file(File) of
		true ->
			{ok, File};
		_ ->
			unavailable
	end.

write_block_shadow(B) ->
	Name = binary_to_list(ar_util:encode(B#block.indep_hash)) ++ ".bin",
	File = filename:join(get_block_path(), Name),
	Bin = ar_serialize:block_to_binary(B),
	Size = byte_size(Bin),
	gen_server:cast(?MODULE, {record_written_data, Size}),
	case ar_storage:write_file_atomic(File, Bin) of
		ok ->
			ok;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_store_block_in_disk_cache},
				{reason, io_lib:format("~p", [Reason])}]),
			Error
	end.

write_block(B) ->
	BShadow = B#block{ txs = [TX#tx.id || TX <- B#block.txs] },
	case write_block_shadow(BShadow) of
		ok ->
			write_txs(B#block.txs);
		Reply ->
			Reply
	end.

write_txs([]) ->
	ok;
write_txs([TX | TXs]) ->
	case write_tx(TX) of
		ok ->
			write_txs(TXs);
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
	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
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
		path = Path
	},
	erlang:garbage_collect(),
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
handle_call(reset, _From, State) ->
	Path = State#state.path,
	?LOG_DEBUG([{event, reset_disk_cache}, {path, Path}]),
	os:cmd("rm -r " ++ Path ++ "/*"),
	BlockPath = filename:join(Path, ?DISK_CACHE_BLOCK_DIR),
	TXPath = filename:join(Path, ?DISK_CACHE_TX_DIR),
	ok = filelib:ensure_dir(BlockPath ++ "/"),
	ok = filelib:ensure_dir(TXPath ++ "/"),
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

handle_cast({record_written_data, Size}, State) ->
	CacheSize = State#state.size + Size,
	?LOG_DEBUG([{event, updated_disk_cache}, {prev_size, State#state.size},
			{new_size, CacheSize}]),
	gen_server:cast(?MODULE, may_be_clean_up),
	{noreply, State#state{ size = CacheSize }};

handle_cast(may_be_clean_up, State) when State#state.size > State#state.limit_max ->
	?LOG_DEBUG([{event, disk_cache_exceeds_limit}, {limit, State#state.limit_max},
			{cache_size, State#state.size}]),
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
	erlang:garbage_collect(),
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

get_block_path() ->
	{ok, Config} = application:get_env(arweave, config),
	Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
	filename:join(Path, ?DISK_CACHE_BLOCK_DIR).

get_tx_path() ->
	{ok, Config} = application:get_env(arweave, config),
	Path = filename:join(Config#config.data_dir, ?DISK_CACHE_DIR),
	filename:join(Path, ?DISK_CACHE_TX_DIR).

write_tx(TX) ->
	Name = binary_to_list(ar_util:encode(TX#tx.id)) ++ ".json",
	File = filename:join(get_tx_path(), Name),
	TXHeader = case TX#tx.format of 1 -> TX; 2 -> TX#tx{ data = <<>> } end,
	JSONStruct = ar_serialize:tx_to_json_struct(TXHeader),
	Data = ar_serialize:jsonify(JSONStruct),
	Size = byte_size(Data),
	gen_server:cast(?MODULE, {record_written_data, Size}),
	case ar_storage:write_file_atomic(File, Data) of
		ok ->
			ok;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_store_transaction_in_disk_cache},
				{reason, io_lib:format("~p", [Reason])}]),
			Error
	end.

delete_file([], _ToRemove, Removed) ->
	Removed;
delete_file(_Files, ToRemove, Removed) when ToRemove < 0 ->
	Removed;
delete_file([{_DateTime, Size, Filename} | Files], ToRemove, Removed) ->
	case file:delete(Filename) of
		ok ->
			?LOG_DEBUG([{event, cleaned_disk_cache}, {removed_file, Filename},
					{cleaned_size, Size}]),
			delete_file(Files, ToRemove - Size, Removed + Size);
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_remove_disk_cache_file},
					{file, Filename}, {reason, io_lib:format("~p", [Reason])}]),
			delete_file(Files, ToRemove, Removed)
	end.
