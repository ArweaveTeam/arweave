-module(app_search).
-export([start/0, start/1]).
-export([start_link/1]).
-export([update_tag_table/1]).
-export([get_entries/2, get_entries/3, get_tags_by_id/3]).
-include("../ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Searchable transaction tag index.
%%%
%%% Access to the DB is wrapped in a server, which limits the number of
%%% concurrent requests to 1.
%%%
%%% In addition to the transaction tags, some auxilirary tags are added (which
%%% overwrites potential real tags with the same name).
%%%
%%% Warning! Data is never removed from the index, so it might return entries
%%% for transactions that only existed in a fork.
%%%
%%% Warning! Duplications of tags exists because every time a block is integrated
%%% into the weave, all it's transactions are written to the index no matter if
%%% the transaction was already written once before, e.g. in a different fork.
%%%
%%% Warning! The index is not complete. E.g. block_height and block_indep_hash
%%% was added at a later stage. The index includes only the transactions from
%%% the downloaded blocks.

-record(arql_tag, {name, value, tx}).

%%@doc Start a search node, linking to a supervisor process
start_link(Args) ->
	Pid = erlang:apply(app_search, start, Args),
	{ok, Pid}.

%% @doc Start a search node.
start() -> start([]).

start(Peers) ->
	initDB(),
	spawn(fun server/0).

add_entry(Name, Value, ID) -> add_entry(http_search_node, Name, Value, ID).

add_entry(ProcessName, Name, Value, ID) when not is_pid(ProcessName) ->
	add_entry(whereis(ProcessName), Name, Value, ID);
add_entry(undefined, _, _, _) -> do_nothing;
add_entry(Pid, Name, Value, ID) ->
	Pid ! {add_tx, Name, Value, ID}.

get_tags_by_id(Pid, TXID, Timeout) ->
	Pid ! {get_tags, TXID, self()},
	receive {tags, Tags} ->
		{ok, Tags}
	after Timeout ->
		{error, timeout}
	end.

%% @doc Returns a list of all transaction IDs which has the tag Name set to
%% Value. Duplicates might be returned due to the duplication in the index.
get_entries(Name, Value) -> get_entries(whereis(http_search_node), Name, Value).

get_entries(Pid, Name, Value) ->
	Pid ! {get_tx, Name, Value, self()},
	receive {txs, TXIDs} ->
		TXIDs
	after 3000 ->
		[]
	end.

%% @doc Updates the index of stored tranasaction data with all of the
%% transactions in the given block
update_tag_table(B) when ?IS_BLOCK(B) ->
	lists:foreach(
		fun(TX) ->
			AddEntry = fun({Name, Value}) ->
				add_entry(Name, Value, TX#tx.id)
			end,
			lists:foreach(AddEntry, entries(B, TX))
		end,
		ar_storage:read_tx(B#block.txs)
	);
update_tag_table(_) ->
	not_updated.

entries(B, TX) ->
	AuxTags = [
		{<<"from">>, ar_util:encode(ar_wallet:to_address(TX#tx.owner))},
		{<<"to">>, ar_util:encode(ar_wallet:to_address(TX#tx.target))},
		{<<"quantity">>, TX#tx.quantity},
		{<<"reward">>, TX#tx.reward},
		{<<"block_height">>, B#block.height},
		{<<"block_indep_hash">>, ar_util:encode(B#block.indep_hash)}
	],
	AuxTags ++ multi_delete(TX#tx.tags, proplists:get_keys(AuxTags)).

multi_delete(Proplist, []) ->
	Proplist;
multi_delete(Proplist, [Key | Keys]) ->
	multi_delete(proplists:delete(Key, Proplist), Keys).

server() ->
	try
		receive
			{get_tx, Name, Value, Pid} ->
				% ar:d({retrieving_tx, search_by_exact_tag(Name, Value)}),
				Pid ! {txs, search_by_exact_tag(Name, Value)},
				server();
			{get_tags, TXID, Pid} ->
				Tags = lists:map(
					fun(Tag) ->
						{_, Name, Value, _} = Tag,
						{Name, Value}
					end,
					search_by_id(TXID)
				),
				Pid ! {tags, Tags},
				server();
			stop -> ok;
			{add_tx, Name, Value, ID} ->
				% ar:d({adding_tags, Name, Value, ID}),
				storeDB(Name, Value, ID),
				server();
			_OtherMsg -> server()
		end
	catch
		throw:Term ->
			ar:report([{'SearchEXCEPTION', Term}]),
			server();
		exit:Term ->
			ar:report([{'SearchEXIT', Term}]),
			server();
		error:Term ->
			ar:report([{'SearchERROR', {Term, erlang:get_stacktrace()}}]),
			server()
	end.

%% @doc Initialise the mnesia database
initDB() ->
	application:set_env(mnesia, dir, "data/mnesia"),
	mnesia:create_schema([node()]),
	mnesia:start(),
	try
		mnesia:table_info(type, arql_tag)
	catch
		exit: _ ->
			mnesia:create_table(
				arql_tag,
				[
					{attributes, record_info(fields, arql_tag)},
					{type, bag},
					{disc_only_copies, [node()]}
				]
			)
	end.

%% @doc Store a transaction ID tag triplet in the index.
storeDB(Name, Value, TXid) ->
	mnesia:dirty_write(#arql_tag { name = Name, value = Value, tx = TXid}).

%% @doc Search for a list of transactions that match the given tag
search_by_exact_tag(Name, Value) ->
	mnesia:dirty_select(
		arql_tag,
		[
			{
				#arql_tag { name = Name, value = Value, tx = '$1'},
				[],
				['$1']
			}
		]
	).

%% @doc Search for a list of tags of the transaction with the given ID
search_by_id(TXID) ->
	mnesia:dirty_select(
		arql_tag,
		[
			{
				#arql_tag { tx = TXID, _ = '_' },
				[],
				['$_']
			}
		]
	).

basic_usage_test() ->
	ar_storage:clear(),
	SearchServer = start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Generate the transaction.
	RawTX = ar_tx:new(),
	TX = RawTX#tx {tags = [
		{<<"TestName">>, <<"TestVal">>},
		{<<"block_height">>, <<"user-specified-block-height">>}
	]},
	%% Add tx to network
	ar_node:add_tx(hd(Peers), TX),
	%% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	%% Get TX by tag
	TXIDs = get_entries(SearchServer, <<"TestName">>, <<"TestVal">>),
	?assert(lists:member(TX#tx.id, TXIDs)),
	%% Get tags by TX
	{ok, Tags} = get_tags_by_id(SearchServer, TX#tx.id, 3000),
	?assert(lists:member({<<"TestName">>, <<"TestVal">>}, Tags)),
	%% Check aux tags
	?assert({<<"block_height">>, 1} == lists:keyfind(<<"block_height">>, 1, Tags)).
