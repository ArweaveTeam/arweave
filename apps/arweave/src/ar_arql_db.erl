-module(ar_arql_db).
-behaviour(gen_server).

-export([start_link/1]).
-export([populate_db/1]).
-export([select_tx_by_id/1, select_txs_by/1]).
-export([select_block_by_tx_id/1, select_tags_by_tx_id/1]).
-export([eval_legacy_arql/1]).
-export([insert_full_block/1]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include("ar.hrl").

%% Duration after which to consider the query time unusually long.
%% Set to 100ms.
-define(LONG_QUERY_TIME, 100000).
%% Duration after which to consider the full block metadata insert
%% time unusually long.
-define(LONG_INSERT_FULL_BLOCK_TIME, 1000000).

%% Timeout passed to gen_server:call when running SELECTs.
%% Set to 5s.
-define(SELECT_TIMEOUT, 5000).

%% Time to wait for the NIF thread to send back the query results.
%% Set to 4.5s.
-define(DRIVER_TIMEOUT, 4500).

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

populate_db(BI) ->
	gen_server:cast(?MODULE, {populate_db, BI}).

select_tx_by_id(ID) ->
	gen_server:call(?MODULE, {select_tx_by_id, ID}, ?SELECT_TIMEOUT).

select_txs_by(Opts) ->
	gen_server:call(?MODULE, {select_txs_by, Opts}, ?SELECT_TIMEOUT).

select_block_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_block_by_tx_id, TXID}, ?SELECT_TIMEOUT).

select_tags_by_tx_id(TXID) ->
	gen_server:call(?MODULE, {select_tags_by_tx_id, TXID}, ?SELECT_TIMEOUT).

eval_legacy_arql(Query) ->
	gen_server:call(?MODULE, {eval_legacy_arql, Query}, ?SELECT_TIMEOUT).

insert_full_block(#block {} = FullBlock) ->
	{BlockFields, TxFieldsList, TagFieldsList} = full_block_to_fields(FullBlock),
	gen_server:cast(?MODULE, {insert_full_block, BlockFields, TxFieldsList, TagFieldsList}),
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Opts) ->
	{data_dir, DataDir} = proplists:lookup(data_dir, Opts),
	ar:info([{ar_arql_db, init}, {data_dir, DataDir}]),
	DbPath = filename:join([DataDir, ?SQLITE3_DIR, "arql.db"]),
	ok = filelib:ensure_dir(DbPath),
	ok = ar_sqlite3:start_link(),
	{ok, Conn} = ar_sqlite3:open(DbPath, ?DRIVER_TIMEOUT),
	ok = ensure_meta_table_created(Conn),
	ok = ensure_schema_created(Conn),
	{ok, InsertBlockStmt} = ar_sqlite3:prepare(Conn, ?INSERT_BLOCK_SQL, ?DRIVER_TIMEOUT),
	{ok, InsertTxStmt} = ar_sqlite3:prepare(Conn, ?INSERT_TX_SQL, ?DRIVER_TIMEOUT),
	{ok, InsertTagStmt} = ar_sqlite3:prepare(Conn, ?INSERT_TAG_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTxByIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TX_BY_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectBlockByTxIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_BLOCK_BY_TX_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, SelectTagsByTxIdStmt} = ar_sqlite3:prepare(Conn, ?SELECT_TAGS_BY_TX_ID_SQL, ?DRIVER_TIMEOUT),
	{ok, #{
		data_dir => DataDir,
		conn => Conn,
		insert_block_stmt => InsertBlockStmt,
		insert_tx_stmt => InsertTxStmt,
		insert_tag_stmt => InsertTagStmt,
		select_tx_by_id_stmt => SelectTxByIdStmt,
		select_block_by_tx_id_stmt => SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt => SelectTagsByTxIdStmt
	}}.

handle_call({select_tx_by_id, ID}, _, State) ->
	#{ select_tx_by_id_stmt := Stmt } = State,
	ok = ar_sqlite3:bind(Stmt, [ID], ?DRIVER_TIMEOUT),
	{Time, Reply} = timer:tc(fun() ->
		case ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT) of
			{row, Row} -> {ok, tx_map(Row)};
			done -> not_found
		end
	end),
	ok = ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, select_tx_by_id},
				{microseconds, T},
				{sql, select_tx_by_id},
				{txid, ID}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State};
handle_call({select_txs_by, Opts}, _, #{ conn := Conn } = State) ->
	{WhereClause, Params} = select_txs_by_where_clause(Opts),
	SQL = lists:concat([
		"SELECT tx.* FROM tx ",
		"JOIN block on tx.block_indep_hash = block.indep_hash ",
		"WHERE ", WhereClause,
		" ORDER BY block.height DESC, tx.id DESC"
	]),
	{Time, Reply} = timer:tc(fun() ->
		case sql_fetchall(Conn, SQL, Params, ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, select_txs_by},
				{microseconds, T},
				{sql, SQL},
				{params, Params}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State};
handle_call({select_block_by_tx_id, TXID}, _, State) ->
	#{ select_block_by_tx_id_stmt := Stmt } = State,
	ok = ar_sqlite3:bind(Stmt, [TXID], ?DRIVER_TIMEOUT),
	{Time, Reply} = timer:tc(fun() ->
		case ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT) of
			{row, Row} -> {ok, block_map(Row)};
			done -> not_found
		end
	end),
	ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, select_block_by_tx_id},
				{microseconds, T},
				{sql, select_block_by_tx_id},
				{txid, TXID}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State};
handle_call({select_tags_by_tx_id, TXID}, _, State) ->
	#{ select_tags_by_tx_id_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [TXID], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tags_map/1, Rows)
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, select_tags_by_tx_id},
				{microseconds, T},
				{sql, select_tags_by_tx_id},
				{txid, TXID}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State};
handle_call({eval_legacy_arql, Query}, _, #{ conn := Conn } = State) ->
	{Time, {Reply, _SQL, _Params}} = timer:tc(fun() ->
		case catch eval_legacy_arql_where_clause(Query) of
			{WhereClause, Params} ->
				SQL = lists:concat([
					"SELECT tx.id FROM tx ",
					"JOIN block ON tx.block_indep_hash = block.indep_hash ",
					"WHERE ", WhereClause,
					" ORDER BY block.height DESC, tx.id DESC"
				]),
				case sql_fetchall(Conn, SQL, Params, ?DRIVER_TIMEOUT) of
					Rows when is_list(Rows) ->
						{lists:map(fun([TXID]) -> TXID end, Rows), SQL, Params}
				end;
			bad_query ->
				{bad_query, 'n/a', 'n/a'}
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, eval_legacy_arql},
				{microseconds, T},
				{query, Query}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State}.

handle_cast({populate_db, BI}, State) ->
	ok = ensure_db_populated(BI, State),
	{noreply, State};
handle_cast({insert_full_block, BlockFields, TxFieldsList, TagFieldsList}, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
		ok = ar_sqlite3:bind(InsertBlockStmt, BlockFields, ?DRIVER_TIMEOUT),
		done = ar_sqlite3:step(InsertBlockStmt, ?DRIVER_TIMEOUT),
		ok = ar_sqlite3:reset(InsertBlockStmt, ?DRIVER_TIMEOUT),
		lists:foreach(
			fun(TxFields) ->
				ok = ar_sqlite3:bind(InsertTxStmt, TxFields, ?DRIVER_TIMEOUT),
				done = ar_sqlite3:step(InsertTxStmt, ?DRIVER_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTxStmt, ?DRIVER_TIMEOUT)
			end,
			TxFieldsList
		),
		lists:foreach(
			fun(TagFields) ->
				ok = ar_sqlite3:bind(InsertTagStmt, TagFields, ?DRIVER_TIMEOUT),
				done = ar_sqlite3:step(InsertTagStmt, ?DRIVER_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTagStmt, ?DRIVER_TIMEOUT)
			end,
			TagFieldsList
		),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?DRIVER_TIMEOUT),
		ok
	end),
	ok = case Time of
		T when T > ?LONG_INSERT_FULL_BLOCK_TIME ->
			ar:warn([
				{ar_arql_db, long_query},
				{query_type, insert_full_block},
				{microseconds, T},
				{block_indep_hash, lists:nth(1, BlockFields)}
			]),
			ok;
		_ ->
			ok
	end,
	{noreply, State}.

terminate(Reason, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt,
		select_tx_by_id_stmt := SelectTxByIdStmt,
		select_block_by_tx_id_stmt := SelectBlockByTxIdStmt,
		select_tags_by_tx_id_stmt := SelectTagsByTxIdStmt
	} = State,
	ar:info([{ar_arql_db, terminate}, {reason, Reason}]),
	ar_sqlite3:finalize(InsertBlockStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(InsertTxStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(InsertTagStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTxByIdStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectBlockByTxIdStmt, ?DRIVER_TIMEOUT),
	ar_sqlite3:finalize(SelectTagsByTxIdStmt, ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:close(Conn, ?DRIVER_TIMEOUT).

%%%===================================================================
%%% Internal functions.
%%%===================================================================

ensure_meta_table_created(Conn) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'migration'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} -> ok;
		done -> create_meta_table(Conn)
	end.

create_meta_table(Conn) ->
	ar:info([{ar_arql_db, creating_meta_table}]),
	ok = ar_sqlite3:exec(Conn, ?CREATE_MIGRATION_TABLE_SQL, ?DRIVER_TIMEOUT),
	ok.

ensure_schema_created(Conn) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM migration
		WHERE name = '20191009160000_schema_created'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} -> ok;
		done -> create_schema(Conn)
	end.

create_schema(Conn) ->
	ar:info([{ar_arql_db, creating_schema}]),
	ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_TABLES_SQL, ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_INDEXES_SQL, ?DRIVER_TIMEOUT),
	done = sql_fetchone(Conn, "INSERT INTO migration VALUES ('20191009160000_schema_created', ?)", [sql_now()], ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?DRIVER_TIMEOUT),
	ok.

ensure_db_populated(BI, #{ conn := Conn} = State) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM migration
		WHERE name = '20191015153000_db_populated'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} ->
			ok;
		done ->
			ar:info([{ar_arql_db, populating_db}]),
			{Time, ok} = timer:tc(fun() -> ok = do_populate_db(BI, State) end),
			ar:info([{ar_arql_db, populated_db}, {time, Time}]),
			ok
	end.

do_populate_db(BI, #{ conn := Conn} = State) ->
	ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?DROP_INDEXES_SQL, ?DRIVER_TIMEOUT),
	ok = lists:foreach(fun({BH, _}) ->
		ok = insert_block_json(BH, State)
	end, BI),
	ok = ar_sqlite3:exec(Conn, ?CREATE_INDEXES_SQL, infinity),
	done = sql_fetchone(Conn, "INSERT INTO migration VALUES ('20191015153000_db_populated', ?)", [sql_now()], ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", infinity),
	ok.

sql_fetchone(Conn, SQL, Timeout) -> sql_fetchone(Conn, SQL, [], Timeout).
sql_fetchone(Conn, SQL, Params, Timeout) ->
	{ok, Stmt} = ar_sqlite3:prepare(Conn, SQL, Timeout),
	ok = ar_sqlite3:bind(Stmt, Params, Timeout),
	Result = ar_sqlite3:step(Stmt, Timeout),
	ok = ar_sqlite3:finalize(Stmt, Timeout),
	Result.

sql_fetchall(Conn, SQL, Params, Timeout) ->
	{ok, Stmt} = ar_sqlite3:prepare(Conn, SQL, Timeout),
	Results = stmt_fetchall(Stmt, Params, Timeout),
	ar_sqlite3:finalize(Stmt, Timeout),
	Results.

stmt_fetchall(Stmt, Params, Timeout) ->
	ok = ar_sqlite3:bind(Stmt, Params, Timeout),
	From = self(),
	Ref = make_ref(),
	spawn_link(fun() ->
		From ! {Ref, collect_sql_results(Stmt)}
	end),
	Results =
		receive
			{Ref, R} -> R
		after Timeout ->
			error(timeout)
		end,
	ar_sqlite3:reset(Stmt, Timeout),
	Results.

collect_sql_results(Stmt) -> collect_sql_results(Stmt, []).
collect_sql_results(Stmt, Acc) ->
	case ar_sqlite3:step(Stmt, infinity) of
		{row, Row} -> collect_sql_results(Stmt, [Row | Acc]);
		done -> lists:reverse(Acc)
	end.

insert_block_json(BH,  Env) ->
	#{ data_dir := DataDir, insert_block_stmt := Stmt } = Env,
	case read_block(BH, DataDir) of
		{ok, BlockMap} ->
			ok = ar_sqlite3:bind(Stmt, [
				maps:get(<<"indep_hash">>, BlockMap),
				maps:get(<<"previous_block">>, BlockMap),
				maps:get(<<"height">>, BlockMap),
				maps:get(<<"timestamp">>, BlockMap)
			], ?DRIVER_TIMEOUT),
			done = ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT),
			ok = ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
			ok = lists:foreach(fun(TXHash) ->
				ok = insert_tx_json(TXHash, maps:get(<<"indep_hash">>, BlockMap), Env)
			end, maps:get(<<"txs">>, BlockMap)),
			ok;
		not_found ->
			ok
	end.

insert_tx_json(TXHash, BlockIndepHash, Env) ->
	#{ insert_tx_stmt := Stmt, data_dir := DataDir } = Env,
	case read_tx(TXHash, DataDir) of
		{ok, TxMap} ->
			ok = ar_sqlite3:bind(Stmt, [
				maps:get(<<"id">>, TxMap),
				BlockIndepHash,
				maps:get(<<"last_tx">>, TxMap),
				maps:get(<<"owner">>, TxMap),
				to_wallet(maps:get(<<"owner">>, TxMap)),
				maps:get(<<"target">>, TxMap),
				maps:get(<<"quantity">>, TxMap),
				maps:get(<<"signature">>, TxMap),
				maps:get(<<"reward">>, TxMap)
			], ?DRIVER_TIMEOUT),
			done = ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT),
			ok = ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
			ok = lists:foreach(fun(TagMap) ->
				ok = insert_tag_json(TagMap, maps:get(<<"id">>, TxMap), Env)
			end, maps:get(<<"tags">>, TxMap)),
			ok;
		not_found ->
			ok
	end.

insert_tag_json(TagMap, TXID, #{ insert_tag_stmt := Stmt }) ->
	ok = ar_sqlite3:bind(Stmt, [
		TXID,
		ar_util:decode(maps:get(<<"name">>, TagMap)),
		ar_util:decode(maps:get(<<"value">>, TagMap))
	], ?DRIVER_TIMEOUT),
	done = ar_sqlite3:step(Stmt, ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:reset(Stmt, ?DRIVER_TIMEOUT),
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

tx_map([
	Id,
	BlockIndepHash,
	LastTx,
	Owner,
	FromAddress,
	Target,
	Quantity,
	Signature,
	Reward
]) -> #{
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

block_map([
	IndepHash,
	PreviousBlock,
	Height,
	Timestamp
]) -> #{
	indep_hash => IndepHash,
	previous_block => PreviousBlock,
	height => Height,
	timestamp => Timestamp
}.

tags_map([TxId, Name, Value]) ->
	#{
		tx_id => TxId,
		name => Name,
		value => Value
	}.

eval_legacy_arql_where_clause({equals, <<"from">>, Value})
	when is_binary(Value) ->
	{
		"tx.from_address = ?",
		[Value]
	};
eval_legacy_arql_where_clause({equals, <<"to">>, Value})
	when is_binary(Value) ->
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
