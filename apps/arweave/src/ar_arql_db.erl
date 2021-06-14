-module(ar_arql_db).
-behaviour(gen_server).

-export([
	start_link/0,
	populate_db/1,
	select_tx_by_id/1, select_txs_by/1,
	select_block_by_tx_id/1, select_tags_by_tx_id/1,
	eval_legacy_arql/1,
	insert_full_block/1, insert_full_block/2, insert_block/1, insert_tx/2, insert_tx/3
]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% Timeout passed to gen_server:call when running SELECTs.
%% Set to 5s.
-define(SELECT_TIMEOUT, 5000).

%% Time to wait for the NIF thread to send back the query results.
%% Set to 4.5s.
-define(DRIVER_TIMEOUT, 4500).

%% Time to wait until an operation (bind, step, etc) required for inserting an entity is complete.
-define(INSERT_STEP_TIMEOUT, 60000).

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

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

populate_db(BHL) ->
	gen_server:cast(?MODULE, {populate_db, BHL}).

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

insert_full_block(FullBlock) ->
	insert_full_block(FullBlock, store_tags).

insert_full_block(#block {} = FullBlock, StoreTags) ->
	{BlockFields, TxFieldsList} = full_block_to_fields(FullBlock),
	TagFieldsList = case StoreTags of
		store_tags ->
			block_to_tag_fields_list(FullBlock);
		_ ->
			[]
	end,
	Call = {insert_full_block, BlockFields, TxFieldsList, TagFieldsList},
	case catch gen_server:call(?MODULE, Call, 30000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, sqlite_timeout};
		Reply ->
			Reply
	end.

insert_block(B) ->
	BlockFields = block_to_fields(B),
	gen_server:cast(?MODULE, {insert_block, BlockFields}),
	ok.

insert_tx(BH, TX) ->
	insert_tx(BH, TX, store_tags).

insert_tx(BH, TX, StoreTags) ->
	TXFields = tx_to_fields(BH, TX),
	TagFieldsList = case StoreTags of
		store_tags ->
			tx_to_tag_fields_list(TX);
		_ ->
			[]
	end,
	gen_server:cast(?MODULE, {insert_tx, TXFields, TagFieldsList}),
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	?LOG_INFO([{ar_arql_db, init}, {data_dir, DataDir}]),
	%% Very occasionally the port fails to be reopened immediately after
	%% a crash so we give it a little time here.
	timer:sleep(1000),
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

handle_call({insert_full_block, BlockFields, TxFieldsList, TagFieldsList}, _From, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertBlockStmt, BlockFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		lists:foreach(
			fun(TxFields) ->
				ok = ar_sqlite3:bind(InsertTxStmt, TxFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTxStmt, ?INSERT_STEP_TIMEOUT)
			end,
			TxFieldsList
		),
		lists:foreach(
			fun(TagFields) ->
				ok = ar_sqlite3:bind(InsertTagStmt, TagFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTagStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTagStmt, ?INSERT_STEP_TIMEOUT)
			end,
			TagFieldsList
		),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_full_block, Time),
	{reply, ok, State};

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
	record_query_time(select_tx_by_id, Time),
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
	record_query_time(select_txs_by, Time),
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
	record_query_time(select_block_by_tx_id, Time),
	{reply, Reply, State};

handle_call({select_tags_by_tx_id, TXID}, _, State) ->
	#{ select_tags_by_tx_id_stmt := Stmt } = State,
	{Time, Reply} = timer:tc(fun() ->
		case stmt_fetchall(Stmt, [TXID], ?DRIVER_TIMEOUT) of
			Rows when is_list(Rows) ->
				lists:map(fun tags_map/1, Rows)
		end
	end),
	record_query_time(select_tags_by_tx_id, Time),
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
	record_query_time(eval_legacy_arql, Time),
	{reply, Reply, State}.

handle_cast({populate_db, BHL}, State) ->
	ok = ensure_db_populated(BHL, State),
	{noreply, State};

handle_cast({insert_block, BlockFields}, State) ->
	#{
		conn := Conn,
		insert_block_stmt := InsertBlockStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertBlockStmt, BlockFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertBlockStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_block, Time),
	{noreply, State};

handle_cast({insert_tx, TXFields, TagFieldsList}, State) ->
	#{
		conn := Conn,
		insert_tx_stmt := InsertTxStmt,
		insert_tag_stmt := InsertTagStmt
	} = State,
	{Time, ok} = timer:tc(fun() ->
		ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:bind(InsertTxStmt, TXFields, ?INSERT_STEP_TIMEOUT),
		done = ar_sqlite3:step(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
		ok = ar_sqlite3:reset(InsertTxStmt, ?INSERT_STEP_TIMEOUT),
		lists:foreach(
			fun(TagFields) ->
				ok = ar_sqlite3:bind(InsertTagStmt, TagFields, ?INSERT_STEP_TIMEOUT),
				done = ar_sqlite3:step(InsertTagStmt, ?INSERT_STEP_TIMEOUT),
				ok = ar_sqlite3:reset(InsertTagStmt, ?INSERT_STEP_TIMEOUT)
			end,
			TagFieldsList
		),
		ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?INSERT_STEP_TIMEOUT),
		ok
	end),
	record_query_time(insert_tx, Time),
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
	?LOG_INFO([{ar_arql_db, terminate}, {reason, Reason}]),
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
	?LOG_INFO([{ar_arql_db, creating_meta_table}]),
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
	?LOG_INFO([{ar_arql_db, creating_schema}]),
	ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_TABLES_SQL, ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?CREATE_INDEXES_SQL, ?DRIVER_TIMEOUT),
	done = sql_fetchone(Conn, "INSERT INTO migration VALUES ('20191009160000_schema_created', ?)", [sql_now()], ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, "COMMIT TRANSACTION", ?DRIVER_TIMEOUT),
	ok.

ensure_db_populated(BHL, #{ conn := Conn} = State) ->
	case sql_fetchone(Conn, "
		SELECT 1 FROM migration
		WHERE name = '20191015153000_db_populated'
	", ?DRIVER_TIMEOUT) of
		{row, [1]} ->
			ok;
		done ->
			?LOG_INFO([{ar_arql_db, populating_db}]),
			{Time, ok} = timer:tc(fun() -> ok = do_populate_db(BHL, State) end),
			?LOG_INFO([{ar_arql_db, populated_db}, {time, Time}]),
			ok
	end.

do_populate_db(BHL, #{ conn := Conn} = State) ->
	ok = ar_sqlite3:exec(Conn, "BEGIN TRANSACTION", ?DRIVER_TIMEOUT),
	ok = ar_sqlite3:exec(Conn, ?DROP_INDEXES_SQL, ?DRIVER_TIMEOUT),
	ok = lists:foreach(fun(BH) ->
		ok = insert_block_json(BH, State)
	end, BHL),
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
	BlockFields = block_to_fields(FullBlock),
	BlockIndepHash = lists:nth(1, BlockFields),
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
	{BlockFields, TxFieldsList}.

block_to_tag_fields_list(B) ->
	lists:flatmap(fun(TX) ->
		EncodedTXID = ar_util:encode(TX#tx.id),
		lists:map(fun({Name, Value}) -> [
			EncodedTXID,
			Name,
			Value
		] end, TX#tx.tags)
	end, B#block.txs).

block_to_fields(B) ->
	[
		ar_util:encode(B#block.indep_hash),
		ar_util:encode(B#block.previous_block),
		B#block.height,
		B#block.timestamp
	].

tx_to_fields(BH, TX) ->
	[
		ar_util:encode(TX#tx.id),
		ar_util:encode(BH),
		ar_util:encode(TX#tx.last_tx),
		ar_util:encode(TX#tx.owner),
		ar_util:encode(ar_wallet:to_address(TX#tx.owner)),
		ar_util:encode(TX#tx.target),
		TX#tx.quantity,
		ar_util:encode(TX#tx.signature),
		TX#tx.reward
	].

tx_to_tag_fields_list(TX) ->
	EncodedTXID = ar_util:encode(TX#tx.id),
	lists:map(
		fun({Name, Value}) ->
			[EncodedTXID, Name, Value]
		end,
		TX#tx.tags
	).

record_query_time(Label, TimeUs) ->
	prometheus_histogram:observe(sqlite_query_time, [Label], TimeUs div 1000).
