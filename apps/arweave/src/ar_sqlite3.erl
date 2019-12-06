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

%% Duration after which to consider the query time unusually long.
%% Set to 100ms.
-define(LONG_QUERY_TIME, 100000).
%% Duration after which to consider the full block metadata insert
%% time unusually long.
-define(LONG_INSERT_FULL_BLOCK_TIME, 1000000).

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

select_txs_by(Opts) ->
	gen_server:call(?MODULE, {select_txs_by, Opts}).

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
	{ok, Conn} = esqlite3:open(DbPath),
	ok = ensure_meta_table_created(Conn),
	ok = ensure_schema_created(Conn),
	{ok, InsertBlockStmt} = esqlite3:prepare(?INSERT_BLOCK_SQL, Conn),
	{ok, InsertTxStmt} = esqlite3:prepare(?INSERT_TX_SQL, Conn),
	{ok, InsertTagStmt} = esqlite3:prepare(?INSERT_TAG_SQL, Conn),
	{ok, SelectTxByIdStmt} = esqlite3:prepare(?SELECT_TX_BY_ID_SQL, Conn),
	{ok, SelectBlockByTxIdStmt} = esqlite3:prepare(?SELECT_BLOCK_BY_TX_ID_SQL, Conn),
	{ok, SelectTagsByTxIdStmt} = esqlite3:prepare(?SELECT_TAGS_BY_TX_ID_SQL, Conn),
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
	ok = esqlite3:bind(Stmt, [ID]),
	{Time, Reply} = timer:tc(fun() ->
		case esqlite3:fetchone(Stmt) of
			Tuple when is_tuple(Tuple) -> {ok, tx_map(Tuple)};
			ok -> not_found
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
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
		case esqlite3:q(SQL, Params, Conn) of
			Rows when is_list(Rows) ->
				lists:map(fun tx_map/1, Rows)
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
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
	ok = esqlite3:bind(Stmt, [TXID]),
	{Time, Reply} = timer:tc(fun() ->
		case esqlite3:fetchone(Stmt) of
			Tuple when is_tuple(Tuple) -> {ok, block_map(Tuple)};
			ok -> not_found
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
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
	ok = esqlite3:bind(Stmt, [TXID]),
	{Time, Reply} = timer:tc(fun() ->
		case esqlite3:fetchall(Stmt) of
			Rows when is_list(Rows) ->
				lists:map(fun tags_map/1, Rows)
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
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
	{Time, {Reply, SQL, Params}} = timer:tc(fun() ->
		case catch eval_legacy_arql_where_clause(Query) of
			{WhereClause, Params} ->
				SQL = lists:concat([
					"SELECT tx.id FROM tx ",
					"JOIN block ON tx.block_indep_hash = block.indep_hash ",
					"WHERE ", WhereClause,
					" ORDER BY block.height DESC, tx.id DESC"
				]),
				case catch esqlite3:q(SQL, Params, Conn) of
					Rows when is_list(Rows) ->
						{lists:map(fun({TXID}) -> TXID end, Rows), SQL, Params};
					{error, {sqlite_error, "parser stack overflow"}} ->
						{sqlite_parser_stack_overflow, SQL, Params};
					Error ->
						ar:warn([{ar_sqlite3, eval_legacy_arql}, {error, Error}]),
						{bad_query, SQL, Params}
				end;
			bad_query ->
				{bad_query, 'n/a', 'n/a'}
		end
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
				{query_type, eval_legacy_arql},
				{microseconds, T},
				{query, Query}
			]),
			ok;
		_ ->
			ok
	end,
	{reply, Reply, State}.

handle_cast({populate_db, BHL}, State) ->
	ok = ensure_db_populated(BHL, State),
	{noreply, State};
handle_cast({insert_block, BlockFields}, State) ->
	#{ insert_block_stmt := Stmt } = State,
	ok = esqlite3:bind(Stmt, BlockFields),
	{Time, ok} = timer:tc(fun() ->
		esqlite3:fetchone(Stmt)
	end),
	ok = case Time of
		T when T > ?LONG_INSERT_FULL_BLOCK_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
				{query_type, insert_block},
				{microseconds, T},
				{indep_hash, lists:nth(1, BlockFields)}
			]),
			ok;
		_ ->
			ok
	end,
	{noreply, State};
handle_cast({insert_tx, TxFields}, State) ->
	#{ insert_tx_stmt := Stmt } = State,
	ok = esqlite3:bind(Stmt, TxFields),
	{Time, ok} = timer:tc(fun() ->
		esqlite3:fetchone(Stmt)
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
				{query_type, insert_tx},
				{microseconds, T}
			]),
			ok;
		_ ->
			ok
	end,
	{noreply, State};
handle_cast({insert_tag, TagFields}, State) ->
	#{ insert_tag_stmt := Stmt } = State,
	ok = esqlite3:bind(Stmt, TagFields),
	{Time, ok} = timer:tc(fun() ->
		esqlite3:fetchone(Stmt)
	end),
	ok = case Time of
		T when T > ?LONG_QUERY_TIME ->
			ar:warn([
				{ar_sqlite3, long_query},
				{query_type, insert_tag},
				{microseconds, T}
			]),
			ok;
		_ ->
			ok
	end,
	{noreply, State}.

terminate(Reason, #{conn := Conn}) ->
	ar:info([{ar_sqlite3, terminate}, {reason, Reason}]),
	ok = esqlite3:close(Conn).

%%%===================================================================
%%% Internal functions.
%%%===================================================================

ensure_meta_table_created(Conn) ->
	case esqlite3:q("
		SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'migration'
	", Conn) of
		[{1}] -> ok;
		[] -> create_meta_table(Conn)
	end.

create_meta_table(Conn) ->
	ar:info([{ar_sqlite3, creating_meta_table}]),
	ok = esqlite3:exec(?CREATE_MIGRATION_TABLE_SQL, Conn),
	ok.

ensure_schema_created(Conn) ->
	case esqlite3:q("
		SELECT 1 FROM migration
		WHERE name = '20191009160000_schema_created'
	", Conn) of
		[{1}] -> ok;
		[] -> create_schema(Conn)
	end.

create_schema(Conn) ->
	ar:info([{ar_sqlite3, creating_schema}]),
	ok = esqlite3:exec("BEGIN TRANSACTION", Conn),
	ok = esqlite3:exec(?CREATE_TABLES_SQL, Conn),
	ok = esqlite3:exec(?CREATE_INDEXES_SQL, Conn),
	'$done' = esqlite3:exec("INSERT INTO migration VALUES ('20191009160000_schema_created', ?)", [sql_now()], Conn),
	ok = esqlite3:exec("COMMIT TRANSACTION", Conn),
	ok.

ensure_db_populated(BHL, #{ conn := Conn} = State) ->
	case esqlite3:q("
		SELECT 1 FROM migration
		WHERE name = '20191015153000_db_populated'
	", Conn) of
		[{1}] ->
			ok;
		[] ->
			ar:info([{ar_sqlite3, populating_db}]),
			{Time, ok} = timer:tc(fun() -> ok = do_populate_db(BHL, State) end),
			ar:info([{ar_sqlite3, populated_db}, {time, Time}]),
			ok
	end.

do_populate_db(BHL, #{ conn := Conn} = State) ->
	ok = esqlite3:exec("BEGIN TRANSACTION", Conn),
	ok = esqlite3:exec(?DROP_INDEXES_SQL, Conn),
	ok = lists:foreach(fun(BH) ->
		ok = insert_block_json(BH, State)
	end, BHL),
	ok = esqlite3:exec(?CREATE_INDEXES_SQL, Conn, infinity),
	'$done' = esqlite3:exec("INSERT INTO migration VALUES ('20191015153000_db_populated', ?)", [sql_now()], Conn),
	ok = esqlite3:exec("COMMIT TRANSACTION", Conn, infinity),
	ok.

insert_block_json(BH,  Env) ->
	#{ data_dir := DataDir, insert_block_stmt := Stmt } = Env,
	case read_block(BH, DataDir) of
		{ok, BlockMap} ->
			ok = esqlite3:bind(Stmt, [
				maps:get(<<"indep_hash">>, BlockMap),
				maps:get(<<"previous_block">>, BlockMap),
				maps:get(<<"height">>, BlockMap),
				maps:get(<<"timestamp">>, BlockMap)
			]),
			ok = esqlite3:fetchone(Stmt),
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
			ok = esqlite3:bind(Stmt, [
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
			ok = esqlite3:fetchone(Stmt),
			ok = lists:foreach(fun(TagMap) ->
				ok = insert_tag_json(TagMap, maps:get(<<"id">>, TxMap), Env)
			end, maps:get(<<"tags">>, TxMap)),
			ok;
		not_found ->
			ok
	end.

insert_tag_json(TagMap, TXID, #{ insert_tag_stmt := Stmt }) ->
	ok = esqlite3:bind(Stmt, [
		TXID,
		ar_util:decode(maps:get(<<"name">>, TagMap)),
		ar_util:decode(maps:get(<<"value">>, TagMap))
	]),
	ok = esqlite3:fetchone(Stmt),
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

tags_map({TxId, Name, Value}) ->
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
