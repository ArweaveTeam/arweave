%% @doc This module provide process work with content policy of transactions.
-module(ar_tx_blacklist).

-behaviour(gen_server).

%%% ==================================================================
%%% gen_server API
%%% ==================================================================

-export([
	start_link/1
]).

%%% ==================================================================
%%% gen_server callbacks
%%% ==================================================================

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	terminate/2
]).

%%% ==================================================================
%%% API
%%% ==================================================================

-export([
	process_policy_content/2,
	scan_tx/1,
	scan_and_clean_disk/0,
	reload/0,
	verify_content_sigs/2,
	get_tx_blacklist_ids/0
]).

%%% ==================================================================
%%% Includes
%%% ==================================================================

-include("ar.hrl").

%%% ==================================================================
%%% Records
%%% ==================================================================

%% Blacklits state of transactions
-record(state, {
	tx_blacklist_ids = sets:new()
}).

%%% ==================================================================
%%% gen_server API functions
%%% ==================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%% ==================================================================
%%% gen_server callbacks
%%% ==================================================================

init(_) ->
	timer:apply_after(?CONTENT_POLICY_SCAN_INTERVAL, gen_server, cast, [?MODULE, process_policy_content]),
	{ok, #state{}}.

handle_call(get_tx_blacklist_ids, _, #state{ tx_blacklist_ids = TxBlacklistIds } = State) ->
	{reply, TxBlacklistIds, State};

handle_call(_, _, State) ->
	{noreply, State}.

handle_cast(process_policy_content, State) ->
	timer:apply_after(?CONTENT_POLICY_SCAN_INTERVAL, gen_server, cast, [?MODULE, process_policy_content]),
	ar_data_sync:reset_black_listed_record(),
	{noreply, State#state{ tx_blacklist_ids = process_policy_content(ar_meta_db:get(content_policy_provider_urls, []), sets:new()) }};

handle_cast(_, State) ->
	{noreply, State}.

terminate(Reason, _) ->
	ar:err(Reason).

%%% ==================================================================
%%% API
%%% ==================================================================

process_policy_content([], Acc) ->
	process_policy_content_txs(load_transaction_blacklist(), Acc);
process_policy_content([URL|T], Acc) ->
	case http_uri:parse(URL) of
		{ok, {_Scheme, UserInfo, Host, Port, Path, Query}} ->
			Peer = {Host, Port},
			FullPath = Path ++ Query,
			Headers = build_req_headers(UserInfo),
			Opts = #{method => get, peer => Peer, path => FullPath, headers => Headers, is_peer_request => false},
			case ar_http:req(Opts) of
				{ok, {{_ ,_}, _, Data, _, _}} ->
					TXBlacklist = binary:split(Data, <<"\n">>, [global]),
					process_policy_content(T, process_policy_content_txs(TXBlacklist, Acc));
				InvalidResponse ->
					ar:err([{event, process_policy_content_request}, {url, URL}, {response, InvalidResponse}]),
					process_policy_content(T, Acc)
			end;
		{error, Reason} ->
			ar:err([{event, process_policy_content_url}, {url, URL}, {reason, Reason}]),
			process_policy_content(T, Acc)
	end.

scan_tx(TX) ->
	Blacklist = get_tx_blacklist_ids(),
	case sets:is_element(TX#tx.id, Blacklist) of
		true ->
			ar:info([{?MODULE, reject_tx}, {tx, ar_util:encode(TX#tx.id)}, tx_is_blacklisted]),
			reject;
		false ->
			verify_content_sigs(TX, ar_tx_blacklist_utils:load_sign(ar_meta_db:get(content_policy_files)))
	end.

scan_and_clean_disk() ->
	TXDir = filename:join(ar_meta_db:get(data_dir), ?TX_DIR),
	case file:list_dir(TXDir) of
		{error, Reason} ->
			ar:err([
				{?MODULE, scan_and_clean_disk},
				{listing_dir, TXDir},
				{error, Reason}
			]),
			ok;
		{ok, Files} ->
			lists:foreach(
				fun(File) ->
					Filepath = filename:join(TXDir, File),
					case scan_file(Filepath) of
						{reject, _} ->
							ar:info([
								{?MODULE, scan_and_clean_disk},
								{removing_file, File}
							]),
							file:delete(Filepath),
							ok;
						{error, Reason} ->
							ar:warn([
								{?MODULE, scan_and_clean_disk},
								{loading_file, File},
								{error, Reason}
							]),
							ok;
						{accept, _} ->
							ok
					end
				end,
				Files
			)
	end,
	ar:console([{?MODULE, disk_scan_complete}]).

reload() ->
	ar_meta_db:put(transaction_blacklist_files, []),
	ar_meta_db:put(content_policy_files, []).

get_tx_blacklist_ids() ->
	gen_server:call(?MODULE, get_tx_blacklist_ids).

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

build_req_headers([]) ->
	[];
build_req_headers(UserInfo) ->
	[{<<"Authorization">>, <<"Basic ", (base64:encode(UserInfo))/binary>>}].

process_policy_content_txs([], Acc) ->
	Acc;
process_policy_content_txs([<<>>|T], Acc) ->
	process_policy_content_txs(T, Acc);
process_policy_content_txs([ID|T], Acc) ->
	TrimID = re:replace(ID, "[\s\t\r\n]", "", [{return, binary}, global]),
	case catch ar_util:decode(TrimID) of
		DecID when is_binary(DecID) ->
			ar_data_sync:delete_tx_data(DecID),
			ar_storage:delete_tx(DecID),
			process_policy_content_txs(T, sets:add_element(DecID, Acc));
		_ ->
			process_policy_content_txs(T, Acc)
	end.

load_transaction_blacklist() ->
	lists:flatten(lists:map(fun load_tx_ids_from_file/1, ar_meta_db:get(transaction_blacklist_files))).

load_tx_ids_from_file(File) ->
	try
		{ok, Binary} = file:read_file(File),
		lists:filtermap(
			fun(TXID) ->
				case TXID of
					<<>> ->
						false;
					TXID ->
						{true, TXID}
				end
			end,
			binary:split(Binary, <<"\n">>, [global])
		)
	catch Type:Pattern ->
		Warning = [load_tx_ids_from_file, {load_file, File}, {exception, {Type, Pattern}}],
		ar:warn(Warning),
		ar:console(Warning),
		[]
	end.

verify_content_sigs(_, {[], no_pattern}) ->
	accept;
verify_content_sigs(TX, ContentSigs) ->
	Tags = lists:foldl(
		fun({K, V}, Acc) ->
			[K, V | Acc]
		end,
		[],
		TX#tx.tags
	),
	ScanList = [TX#tx.data, TX#tx.target | Tags],
	case ar_tx_blacklist_utils:is_infected(ScanList, ContentSigs) of
		{true, MatchedSigs} ->
			ar:info([
				{?MODULE, reject_tx},
				{tx, ar_util:encode(TX#tx.id)},
				{matches, MatchedSigs}
			]),
			reject;
		false ->
			accept
	end.

scan_file(File) ->
	case file:read_file(File) of
		{error, Reason} ->
			{error, Reason};
		{ok, Binary} ->
			try
				TX = ar_serialize:json_struct_to_tx(Binary),
				{scan_tx(TX), TX#tx.id}
			catch Type:Pattern ->
				{error, {exception, Type, Pattern}}
			end
	end.
