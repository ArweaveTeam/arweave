-module(ar_sqlite3).
-behaviour(gen_server).

-export([start_link/1]).
-export([populate_db/1]).
-export([select_tx_by_id/1, select_txs_by/1]).
-export([select_block_by_tx_id/1, select_tags_by_tx_id/1]).
-export([eval_legacy_arql/1]).
-export([insert_full_block/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include("ar.hrl").

-define(DB_NAME, list_to_atom(atom_to_list(?MODULE) ++ "_db")).

%% The migration name should follow the format YYYYMMDDHHMMSS_human_readable_name
%% where the date is picked at the time of naming the migration.
-define(CREATE_MIGRATION_TABLE_SQL, "
CREATE TABLE migration (
	name TEXT PRIMARY KEY,
	date TEXT
);

CREATE INDEX idx_migration_name ON migration (name);
CREATE INDEX idx_migration_date ON migration (date);
").

-define(CREATE_TABLES_SQL, "
CREATE TABLE block (
	indep_hash TEXT PRIMARY KEY,
	previous_block TEXT,
	height INTEGER,
	timestamp INTEGER
);

CREATE TABLE tx (
	id TEXT PRIMARY KEY,
	block_indep_hash TEXT,
	last_tx TEXT,
	owner TEXT,
	from_address TEXT,
	target TEXT,
	quantity INTEGER,
	signature TEXT,
	reward INTEGER
);

CREATE TABLE tag (
	tx_id TEXT,
	name TEXT,
	value TEXT
);
").

-define(CREATE_INDEXES_SQL, "
CREATE INDEX idx_block_height ON block (height);
CREATE INDEX idx_block_timestamp ON block (timestamp);

CREATE INDEX idx_tx_block_indep_hash ON tx (block_indep_hash);
CREATE INDEX idx_tx_from_address ON tx (from_address);

CREATE INDEX idx_tag_tx_id ON tag (tx_id);
CREATE INDEX idx_tag_name_value ON tag (name, value);
").

-define(DROP_INDEXES_SQL, "
DROP INDEX idx_block_height;
DROP INDEX idx_block_timestamp;

DROP INDEX idx_tx_block_indep_hash;
DROP INDEX idx_tx_from_address;

DROP INDEX idx_tag_tx_id;
DROP INDEX idx_tag_name_value;
").

-define(INSERT_BLOCK_SQL, "INSERT OR REPLACE INTO block VALUES (?, ?, ?, ?)").
-define(INSERT_TX_SQL, "INSERT OR REPLACE INTO tx VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").
-define(INSERT_TAG_SQL, "INSERT OR REPLACE INTO tag VALUES (?, ?, ?)").
-define(SELECT_TX_BY_ID_SQL, "SELECT * FROM tx WHERE id = ?").

-define(SELECT_BLOCK_BY_TX_ID_SQL, "
	SELECT block.* FROM block
	JOIN tx on tx.block_indep_hash = block.indep_hash
	WHERE tx.id = ?
").

-define(SELECT_TAGS_BY_TX_ID_SQL, "SELECT * FROM tag WHERE tx_id = ?").

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link(Opts) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

populate_db(BHL) ->
	gen_server:cast(?MODULE, {populate_db, BHL}).

select_tx_by_id(ID) ->
	gen_server:call(?MODULE, {select_tx_by_id, ID}).

select_txs_by(Tags) ->
	gen_server:call(?MODULE, {select_txs_by, Tags}).

select_block_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_block_by_tx_id, TXID}).

select_tags_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_tags_by_tx_id, TXID}).

eval_legacy_arql(Query) ->
	gen_server:call(?MODULE, {eval_legacy_arql, Query}).

insert_full_block(#block {} = FullBlock) ->
	{BlockFields, TxFieldsList, TagFieldsList} = full_block_to_fields(FullBlock),
	gen_server:cast(?MODULE, {insert_block, BlockFields}),
	lists:foreach(fun (TxFields) ->
		gen_server:cast(?MODULE, {insert_tx, TxFields})
	end, TxFieldsList),
	lists:foreach(fun (TagFields) ->
		gen_server:cast(?MODULE, {insert_tag, TagFields})
	end, TagFieldsList),
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Opts) ->
	{data_dir, DataDir} = proplists:lookup(data_dir, Opts),
	ar:info([{ar_sqlite3, init}, {data_dir, DataDir}]),
	DbPath = filename:join([DataDir, ?SQLITE3_DIR, "arql.db"]),
	ok = filelib:ensure_dir(DbPath),
	{ok, _} = sqlite3:open(?DB_NAME, [{file, DbPath}]),
	ok = ensure_meta_table_created(),
	ok = ensure_schema_created(),
	{ok, InsertBlockStmt} = sqlite3:prepare(?DB_NAME, ?INSERT_BLOCK_SQL),
	{ok, InsertTxStmt} = sqlite3:prepare(?DB_NAME, ?INSERT_TX_SQL),
	{ok, InsertTagStmt} = sqlite3:prepare(?DB_NAME, ?INSERT_TAG_SQL),
	{ok, SelectTxByIdStmt} = sqlite3:prepare(?DB_NAME, ?SELECT_TX_BY_ID_SQL),
	{ok, SelectBlockByTxIdStmt} = sqlite3:prepare(?DB_NAME, ?SELECT_BLOCK_BY_TX_ID_SQL),
	{ok, SelectTagsByTxIdStmt} = sqlite3:prepare(?DB_NAME, ?SELECT_TAGS_BY_TX_ID_SQL),
	{ok, #{
		data_dir => DataDir,
		insert_block_stmt => InsertBlockStmt,
		insert_tx_stmt => InsertTxStmt,
		insert_tag_stmt => InsertTagStmt,
		select_tx_by_id_stmt => SelectTxByIdStmt,
		select_block_by_tx_id_stmt => SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt => SelectTagsByTxIdStmt
	}}.

handle_call({select_tx_by_id, ID}, _, State) ->
	#{ select_tx_by_id_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, [ID]),
	Reply = case sqlite3:next(?DB_NAME, Stmt) of
		Tuple when is_tuple(Tuple) -> {ok, tx_map(Tuple)};
		done -> not_found
	end,
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{reply, Reply, State};
handle_call({select_txs_by, Opts}, _, State) ->
	{WhereClause, Params} = select_txs_by_where_clause(Opts),
	SQL = lists:concat([
		"SELECT tx.* FROM tx ",
		"JOIN block on tx.block_indep_hash = block.indep_hash ",
		"WHERE ", WhereClause,
		" ORDER BY block.height DESC, tx.id DESC"
	]),
	{ok, Stmt} = sqlite3:prepare(?DB_NAME, SQL),
	ok = sqlite3:bind(?DB_NAME, Stmt, Params),
	Reply = collect_txs(Stmt),
	ok = sqlite3:finalize(?DB_NAME, Stmt),
	{reply, Reply, State};
handle_call({select_block_by_tx_id, TXID}, _, State) ->
	#{ select_block_by_tx_id_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, [TXID]),
	Reply = case sqlite3:next(?DB_NAME, Stmt) of
		Tuple when is_tuple(Tuple) -> {ok, block_map(Tuple)};
		done -> not_found
	end,
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{reply, Reply, State};
handle_call({select_tags_by_tx_id, TXID}, _, State) ->
	#{ select_tags_by_tx_id_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, [TXID]),
	Reply = collect_tags(Stmt),
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{reply, Reply, State};
handle_call({eval_legacy_arql, Query}, _, State) ->
	Reply = case catch eval_legacy_arql_where_clause(Query) of
		{WhereClause, Params} ->
			SQL = lists:concat([
				"SELECT tx.id FROM tx ",
				"JOIN block ON tx.block_indep_hash = block.indep_hash ",
				"WHERE ", WhereClause,
				" ORDER BY block.height DESC, tx.id DESC"
			]),
			case sqlite3:prepare(?DB_NAME, SQL) of
				{ok, Stmt} ->
					ok = sqlite3:bind(?DB_NAME, Stmt, Params),
					R = collect_txids(Stmt),
					ok = sqlite3:finalize(?DB_NAME, Stmt),
					R;
				{error, 1, "parser stack overflow"} ->
					sqlite_parser_stack_overflow;
				_ ->
					bad_query
			end;
		bad_query ->
			bad_query
	end,
	{reply, Reply, State}.

handle_cast({populate_db, BHL}, State) ->
	ok = ensure_db_populated(BHL, State),
	{noreply, State};
handle_cast({insert_block, BlockFields}, State) ->
	#{ insert_block_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, BlockFields),
	done = sqlite3:next(?DB_NAME, Stmt),
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{noreply, State};
handle_cast({insert_tx, TxFields}, State) ->
	#{ insert_tx_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, TxFields),
	done = sqlite3:next(?DB_NAME, Stmt),
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{noreply, State};
handle_cast({insert_tag, TagFields}, State) ->
	#{ insert_tag_stmt := Stmt } = State,
	ok = sqlite3:bind(?DB_NAME, Stmt, TagFields),
	done = sqlite3:next(?DB_NAME, Stmt),
	ok = sqlite3:reset(?DB_NAME, Stmt),
	{noreply, State}.

terminate(Reason, State) ->
	ar:info([{ar_sqlite3, terminate}, {reason, Reason}]),
	#{
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt,
		select_tx_by_id_stmt := SelectTxByIdStmt,
		select_block_by_tx_id_stmt := SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt := SelectTagsByTxIdStmt
	} = State,
	ok = sqlite3:finalize(?DB_NAME, SelectTagsByTxIdStmt),
	ok = sqlite3:finalize(?DB_NAME, SelectBlockByTxIdStmt),
	ok = sqlite3:finalize(?DB_NAME, SelectTxByIdStmt),
	ok = sqlite3:finalize(?DB_NAME, InsertTagStmt),
	ok = sqlite3:finalize(?DB_NAME, InsertTxStmt),
	ok = sqlite3:finalize(?DB_NAME, InsertBlockStmt),
	ok = sqlite3:close(?DB_NAME).

%%%===================================================================
%%% Internal functions.
%%%===================================================================

ensure_meta_table_created() ->
	case sqlite3:sql_exec(?DB_NAME, "
		SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'migration'
	") of
		[{columns,["1"]},{rows,[{1}]}] -> ok;
		[{columns,["1"]},{rows,[]}] -> create_meta_table()
	end.

create_meta_table() ->
	ar:info([{ar_sqlite3, creating_meta_table}]),
	ok = sqlite3:sql_exec(?DB_NAME, ?CREATE_MIGRATION_TABLE_SQL),
	ok.

ensure_schema_created() ->
	case sqlite3:sql_exec(?DB_NAME, "
		SELECT 1 FROM migration
		WHERE name = '20191009160000_schema_created'
	") of
		[{columns,["1"]},{rows,[{1}]}] -> ok;
		[{columns,["1"]},{rows,[]}] -> create_schema()
	end.

create_schema() ->
	ar:info([{ar_sqlite3, creating_schema}]),
	ok = sqlite3:sql_exec(?DB_NAME, "BEGIN TRANSACTION"),
	[ok, ok, ok] = sqlite3:sql_exec_script_timeout(?DB_NAME, ?CREATE_TABLES_SQL, infinity),
	[ok, ok, ok, ok, ok, ok] = sqlite3:sql_exec_script_timeout(?DB_NAME, ?CREATE_INDEXES_SQL, infinity),
	{rowid, _} = sqlite3:sql_exec(?DB_NAME, "INSERT INTO migration VALUES ('20191009160000_schema_created', ?)", [sql_now()]),
	ok = sqlite3:sql_exec(?DB_NAME, "COMMIT TRANSACTION"),
	ok.

ensure_db_populated(BHL, State) ->
	case sqlite3:sql_exec(?DB_NAME, "
		SELECT 1 FROM migration
		WHERE name = '20191015153000_db_populated'
	") of
		[{columns,["1"]},{rows,[{1}]}] ->
			ok;
		[{columns,["1"]},{rows,[]}] ->
			ar:info([{ar_sqlite3, populating_db}]),
			{Time, ok} = timer:tc(fun() -> ok = do_populate_db(BHL, State) end),
			ar:info([{ar_sqlite3, populated_db}, {time, Time}]),
			ok
	end.

do_populate_db(BHL, State) ->
	ok = sqlite3:sql_exec(?DB_NAME, "BEGIN TRANSACTION"),
	[ok, ok, ok, ok, ok, ok] = sqlite3:sql_exec_script(?DB_NAME, ?DROP_INDEXES_SQL),
	ok = lists:foreach(fun(BH) ->
		ok = insert_block_json(BH, State)
	end, BHL),
	[ok, ok, ok, ok, ok, ok] = sqlite3:sql_exec_script_timeout(?DB_NAME, ?CREATE_INDEXES_SQL, infinity),
	{rowid, _} = sqlite3:sql_exec(?DB_NAME, "INSERT INTO migration VALUES ('20191015153000_db_populated', ?)", [sql_now()]),
	ok = sqlite3:sql_exec(?DB_NAME, "COMMIT TRANSACTION"),
	ok.

insert_block_json(BH,  Env) ->
	#{ data_dir := DataDir, insert_block_stmt := Stmt } = Env,
	case read_block(BH, DataDir) of
		{ok, BlockMap} ->
			ok = sqlite3:bind(?DB_NAME, Stmt, [
				maps:get(<<"indep_hash">>, BlockMap),
				maps:get(<<"previous_block">>, BlockMap),
				maps:get(<<"height">>, BlockMap),
				maps:get(<<"timestamp">>, BlockMap)
			]),
			done = sqlite3:next(?DB_NAME, Stmt),
			ok = lists:foreach(fun(TXHash) ->
				ok = insert_tx_json(TXHash, maps:get(<<"indep_hash">>, BlockMap), Env)
			end, maps:get(<<"txs">>, BlockMap)),
			ok = sqlite3:reset(?DB_NAME, Stmt),
			ok;
		not_found ->
			ok
	end.

insert_tx_json(TXHash, BlockIndepHash, Env) ->
	#{ insert_tx_stmt := Stmt, data_dir := DataDir } = Env,
	case read_tx(TXHash, DataDir) of
		{ok, TxMap} ->
			ok = sqlite3:bind(?DB_NAME, Stmt, [
				maps:get(<<"id">>, TxMap),
				BlockIndepHash,
				maps:get(<<"last_tx">>, TxMap),
				maps:get(<<"owner">>, TxMap),
				to_wallet(maps:get(<<"owner">>, TxMap)),
				maps:get(<<"target">>, TxMap),
				maps:get(<<"quantity">>, TxMap),
				maps:get(<<"signature">>, TxMap),
				maps:get(<<"reward">>, TxMap)
			]),
			done = sqlite3:next(?DB_NAME, Stmt),
			ok = lists:foreach(fun(TagMap) ->
				ok = insert_tag_json(TagMap, maps:get(<<"id">>, TxMap), Env)
			end, maps:get(<<"tags">>, TxMap)),
			sqlite3:reset(?DB_NAME, Stmt),
			ok;
		not_found ->
			ok
	end.

insert_tag_json(TagMap, TXID, #{ insert_tag_stmt := Stmt }) ->
	ok = sqlite3:bind(?DB_NAME, Stmt, [
		TXID,
		ar_util:decode(maps:get(<<"name">>, TagMap)),
		ar_util:decode(maps:get(<<"value">>, TagMap))
	]),
	done = sqlite3:next(?DB_NAME, Stmt),
	ok = sqlite3:reset(?DB_NAME, Stmt),
	ok.

read_block(BH, _DataDir) ->
	case ar_storage:lookup_block_filename(BH) of
		unavailable ->
			not_found;
		Filename ->
			{ok, Bin} = file:read_file(Filename),
			case ar_serialize:json_decode(Bin, [return_maps]) of
				{ok, BlockMap} ->
					{ok, BlockMap};
				{error, _} ->
					not_found
			end
	end.

read_tx(TX, DataDir) ->
	case file:read_file(filename:join([DataDir, ?TX_DIR, tx_filename(TX)])) of
		{ok, Bin} ->
			case ar_serialize:json_decode(Bin, [return_maps]) of
				{ok, TXMap} ->
					{ok, TXMap};
				{error, _} ->
					not_found
			end;
		{error, enoent} ->
			not_found
	end.

tx_filename(Hash) ->
	<<Hash/binary, ".json">>.

to_wallet(OwnerHash) ->
	ar_util:encode(ar_wallet:to_address(ar_util:decode(OwnerHash))).

sql_now() ->
	calendar:system_time_to_rfc3339(erlang:system_time(second), [{offset, "Z"}]).

select_txs_by_where_clause(Opts) ->
	FromQuery = proplists:get_value(from, Opts, any),
	ToQuery = proplists:get_value(to, Opts, any),
	Tags = proplists:get_value(tags, Opts, []),
	{FromWhereClause, FromParams} = select_txs_by_where_clause_from(FromQuery),
	{ToWhereClause, ToParams} = select_txs_by_where_clause_to(ToQuery),
	{TagsWhereClause, TagsParams} = select_txs_by_where_clause_tags(Tags),
	{WhereClause, WhereParams} = {
		lists:concat([
			FromWhereClause,
			" AND ",
			ToWhereClause,
			" AND ",
			TagsWhereClause
		]),
		FromParams ++ ToParams ++ TagsParams
	},
	{WhereClause, WhereParams}.

select_txs_by_where_clause_from(any) ->
	{"1", []};
select_txs_by_where_clause_from(FromQuery) ->
	{
		lists:concat([
			"tx.from_address IN (",
			lists:concat(lists:join(", ", lists:map(fun(_) -> "?" end, FromQuery))),
			")"
		]),
		FromQuery
	}.

select_txs_by_where_clause_to(any) ->
	{"1", []};
select_txs_by_where_clause_to(ToQuery) ->
	{
		lists:concat([
			"tx.target IN (",
			lists:concat(lists:join(", ", lists:map(fun(_) -> "?" end, ToQuery))),
			")"
		]),
		ToQuery
	}.

select_txs_by_where_clause_tags(Tags) ->
	lists:foldl(fun
		({Name, any}, {WhereClause, WhereParams}) ->
			{
				WhereClause ++ " AND tx.id IN (SELECT tx_id FROM tag WHERE name = ?)",
				WhereParams ++ [Name]
			};
		({Name, Value}, {WhereClause, WhereParams}) ->
			{
				WhereClause ++ " AND tx.id IN (SELECT tx_id FROM tag WHERE name = ? AND value = ?)",
				WhereParams ++ [Name, Value]
			}
	end, {"1", []}, Tags).

collect_txs(Stmt) ->
	collect_txs(Stmt, []).

collect_txs(Stmt, Acc) ->
	case sqlite3:next(?DB_NAME, Stmt) of
		Tuple when is_tuple(Tuple) ->
			collect_txs(Stmt, [tx_map(Tuple) | Acc]);
		done ->
			lists:reverse(Acc)
	end.

tx_map({
	Id,
	BlockIndepHash,
	LastTx,
	Owner,
	FromAddress,
	Target,
	Quantity,
	Signature,
	Reward
}) -> #{
	id => Id,
	block_indep_hash => BlockIndepHash,
	last_tx => LastTx,
	owner => Owner,
	from_address => FromAddress,
	target => Target,
	quantity => Quantity,
	signature => Signature,
	reward => Reward
}.

block_map({
	IndepHash,
	PreviousBlock,
	Height,
	Timestamp
}) -> #{
	indep_hash => IndepHash,
	previous_block => PreviousBlock,
	height => Height,
	timestamp => Timestamp
}.

collect_tags(Stmt) ->
	collect_tags(Stmt, []).

collect_tags(Stmt, Acc) ->
	case sqlite3:next(?DB_NAME, Stmt) of
		Tuple when is_tuple(Tuple) ->
			collect_tags(Stmt, [tags_map(Tuple) | Acc]);
		done ->
			lists:reverse(Acc)
	end.

tags_map({TxId, Name, Value}) ->
	#{
		tx_id => TxId,
		name => Name,
		value => Value
	}.

eval_legacy_arql_where_clause({equals, <<"from">>, Value}) ->
	{
		"tx.from_address = ?",
		[Value]
	};
eval_legacy_arql_where_clause({equals, <<"to">>, Value}) ->
	{
		"tx.target = ?",
		[Value]
	};
eval_legacy_arql_where_clause({equals, Key, Value})
	when is_binary(Key), is_binary(Value) ->
	{
		"tx.id IN (SELECT tx_id FROM tag WHERE name = ? and value = ?)",
		[Key, Value]
	};
eval_legacy_arql_where_clause({'and',E1,E2}) ->
	{E1WhereClause, E1Params} = eval_legacy_arql_where_clause(E1),
	{E2WhereClause, E2Params} = eval_legacy_arql_where_clause(E2),
	{
		lists:concat([
			"(",
			E1WhereClause,
			" AND ",
			E2WhereClause,
			")"
		]),
		E1Params ++ E2Params
	};
eval_legacy_arql_where_clause({'or',E1,E2}) ->
	{E1WhereClause, E1Params} = eval_legacy_arql_where_clause(E1),
	{E2WhereClause, E2Params} = eval_legacy_arql_where_clause(E2),
	{
		lists:concat([
			"(",
			E1WhereClause,
			" OR ",
			E2WhereClause,
			")"
		]),
		E1Params ++ E2Params
	};
eval_legacy_arql_where_clause(_) ->
	throw(bad_query).

collect_txids(Stmt) ->
	collect_txids(Stmt, []).

collect_txids(Stmt, Acc) ->
	case sqlite3:next(?DB_NAME, Stmt) of
		{EncodedTXID} ->
			collect_txids(Stmt, [EncodedTXID | Acc]);
		done ->
			lists:reverse(Acc)
	end.

full_block_to_fields(FullBlock) ->
	BlockIndepHash = ar_util:encode(FullBlock#block.indep_hash),
	BlockFields = [
		BlockIndepHash,
		ar_util:encode(FullBlock#block.previous_block),
		FullBlock#block.height,
		FullBlock#block.timestamp
	],
	TxFieldsList = lists:map(fun(TX) -> [
		ar_util:encode(TX#tx.id),
		BlockIndepHash,
		ar_util:encode(TX#tx.last_tx),
		ar_util:encode(TX#tx.owner),
		ar_util:encode(ar_wallet:to_address(TX#tx.owner)),
		ar_util:encode(TX#tx.target),
		TX#tx.quantity,
		ar_util:encode(TX#tx.signature),
		TX#tx.reward
	] end, FullBlock#block.txs),
	TagFieldsList = lists:flatmap(fun(TX) ->
		EncodedTXID = ar_util:encode(TX#tx.id),
		lists:map(fun({Name, Value}) -> [
			EncodedTXID,
			Name,
			Value
		] end, TX#tx.tags)
	end, FullBlock#block.txs),
	{BlockFields, TxFieldsList, TagFieldsList}.
