%%% @doc The module manages a transaction blacklist. The blacklisted identifiers
%%% are read from the configured files or downloaded from the configured HTTP endpoints.
%%% The server coordinates the removal of the transaction headers and data and answers
%%% queries about the currently blacklisted transactions and the corresponding global
%%% byte offsets.
%%% @end
-module(ar_tx_blacklist).

-behaviour(gen_server).

-export([
	start_link/1,
	is_tx_blacklisted/1,
	is_byte_blacklisted/1,
	get_next_not_blacklisted_byte/1,
	notify_about_removed_tx/1,
	notify_about_removed_tx_data/1,
	store_state/0
]).

-export([init/1, handle_cast/2, handle_call/3, terminate/2]).

-include("ar.hrl").
-include("common.hrl").

%% @doc The frequency of refreshing the blacklist.
-ifdef(DEBUG).
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 2000).
-else.
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 60 * 60 * 1000).
-endif.

%% @doc How long to wait for the response to the previously requested
%% header or data removal (takedown) before requesting it for a new tx.
%% @end
-define(REQUEST_TAKEDOWN_DELAY_MS, 2000).

%% @doc The frequency of checking whether the time for the response to
%% the previously requested takedown is due.
%% @end
-define(CHECK_PENDING_ITEMS_INTERVAL_MS, 1000).

%% @doc The frequency of persisting the server state.
-ifdef(DEBUG).
-define(STORE_STATE_FREQUENCY_MS, 20000).
-else.
-define(STORE_STATE_FREQUENCY_MS, 10 * 60 * 1000).
-endif.

%% @doc The server state.
-record(ar_tx_blacklist_state, {
	%% @doc The timestamp of the last requested transaction header takedown.
	%% It is used to throttle the takedown requests.
	%% @end.
	header_takedown_request_timestamp = os:system_time(millisecond),
	%% @doc The timestamp of the last requested transaction data takedown.
	%% It is used to throttle the takedown requests.
	%% @end.
	data_takedown_request_timestamp = os:system_time(millisecond)
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc Check whether the given transaction is blacklisted.
is_tx_blacklisted(TXID) ->
	ets:member(ar_tx_blacklist, TXID)
		orelse ets:member(ar_tx_blacklist_pending_headers, TXID)
			orelse ets:member(ar_tx_blacklist_pending_data, TXID).

%% @doc Check whether the byte with the given global offset is blacklisted.
is_byte_blacklisted(Offset) ->
	case ets:next(ar_tx_blacklist_offsets, Offset - 1) of
		'$end_of_table' ->
			false;
		NextOffset ->
			case ets:lookup(ar_tx_blacklist_offsets, NextOffset) of
				[{NextOffset, Start}] ->
					Offset > Start;
				[] ->
					%% The key should have been just removed, unlucky timing.
					is_byte_blacklisted(Offset)
			end
	end.

%% @doc Lookup the smallest not blacklisted byte greater than or equal to the given byte.
get_next_not_blacklisted_byte(Offset) ->
	case ets:next(ar_tx_blacklist_offsets, Offset - 1) of
		'$end_of_table' ->
			Offset;
		NextOffset ->
			case ets:lookup(ar_tx_blacklist_offsets, NextOffset) of
				[{NextOffset, Start}] ->
					case Start >= Offset of
						true ->
							Offset;
						false ->
							NextOffset + 1
					end;
				[] ->
					%% The key should have been just removed, unlucky timing.
					get_next_not_blacklisted_byte(Offset)
			end
	end.

%% @doc Notify the server about the removed transaction header.
notify_about_removed_tx(TXID) ->
	gen_server:cast(?MODULE, {removed_tx, TXID}).

%% @doc Notify the server about the removed transaction data.
notify_about_removed_tx_data(TXID) ->
	gen_server:cast(?MODULE, {removed_tx_data, TXID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = initialize_state(),
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, refresh_blacklist),
	gen_server:cast(?MODULE, maybe_request_takedown),
	{ok, _} = timer:apply_interval(?STORE_STATE_FREQUENCY_MS, ?MODULE, store_state, []),
    {ok, #ar_tx_blacklist_state{}}.

handle_cast(refresh_blacklist, State) ->
	WhitelistFiles = ar_meta_db:get(transaction_whitelist_files),
	Set = load_from_files(WhitelistFiles),
	Files = ar_meta_db:get(transaction_blacklist_files),
	Set2 = load_from_files(Files),
	URLs = ar_meta_db:get(transaction_blacklist_urls),
	Set3 = sets:subtract(sets:union(Set2, load_from_urls(URLs)), Set),
	Restored =
		ets:foldl(
			fun({TXID}, Acc) ->
				case sets:is_element(TXID, Set) of
					true ->
						[TXID | Acc];
					false ->
						Acc
				end
			end,
			[],
			ar_tx_blacklist
		),
	ok =
		sets:fold(
			fun(TXID, ok) ->
				case ets:member(ar_tx_blacklist, TXID) of
					false ->
						ets:insert(ar_tx_blacklist_pending_headers, [{TXID}]),
						ets:insert(ar_tx_blacklist_pending_data, [{TXID}]),
						ok;
					true ->
						ok
				end
			end,
			ok,
			Set3
		),
	gen_server:cast(?MODULE, {update_restored_offsets, Restored}),
	timer:apply_after(
		?REFRESH_BLACKLISTS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), refresh_blacklist]
	),
	{noreply, State};

handle_cast({update_restored_offsets, []}, State) ->
	{noreply, State};
handle_cast({update_restored_offsets, [TXID | List]}, State) ->
	case ar_data_sync:get_tx_offset(TXID) of
		{ok, {End, Size}} ->
			restore_offsets(End, End - Size);
		{error, not_joined} ->
			ok;
		{error, Reason} ->
			ar:err([
				{event, ar_tx_blacklist_failed_to_fetch_tx_offset},
				{tx, ar_util:encode(TXID)},
				{reason, Reason}
			])
	end,
	gen_server:cast(?MODULE, {update_restored_offsets, List}),
	{noreply, State};

handle_cast(maybe_request_takedown, State) ->
	#ar_tx_blacklist_state{
		header_takedown_request_timestamp = HTS,
		data_takedown_request_timestamp = DTS
	} = State,
	Now = os:system_time(millisecond),
	State2 =
		case HTS + ?REQUEST_TAKEDOWN_DELAY_MS < Now of
			true ->
				request_header_takedown(State);
			false ->
				State
		end,
	State3 = 
		case DTS + ?REQUEST_TAKEDOWN_DELAY_MS < Now of
			true ->
				request_data_takedown(State2);
			false ->
				State2
		end,
	timer:apply_after(
		?CHECK_PENDING_ITEMS_INTERVAL_MS,
		gen_server,
		cast,
		[self(), maybe_request_takedown]
	),
	{noreply, State3};

handle_cast({removed_tx, TXID}, State) ->
	case ets:member(ar_tx_blacklist_pending_headers, TXID) of
		false ->
			{noreply, State};
		true ->
			case ets:member(ar_tx_blacklist_pending_data, TXID) of
				false ->
					ets:insert(ar_tx_blacklist, [{TXID}]),
					ets:delete(ar_tx_blacklist_pending_headers, TXID);
				true ->
					ets:delete(ar_tx_blacklist_pending_headers, TXID)
			end,
			{noreply, request_header_takedown(State)}
	end;

handle_cast({removed_tx_data, TXID}, State) ->
	case ets:member(ar_tx_blacklist_pending_data, TXID) of
		false ->
			{noreply, State};
		true ->
			case ets:member(ar_tx_blacklist_pending_headers, TXID) of
				false ->
					ets:insert(ar_tx_blacklist, [{TXID}]),
					ets:delete(ar_tx_blacklist_pending_data, TXID);
				true ->
					ets:delete(ar_tx_blacklist_pending_data, TXID)
			end,
			{noreply, request_data_takedown(State)}
	end.

handle_call(_Message, _From, _State) ->
	not_implemented.

terminate(Reason, _State) ->
	ar:info([{event, ar_tx_blacklist_terminate}, {reason, Reason}]),
	store_state(),
	close_dets().

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state() ->
	DataDir = ar_meta_db:get(data_dir),
	Dir = filename:join(DataDir, "ar_tx_blacklist"),
	ok = filelib:ensure_dir(Dir ++ "/"),
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				{ok, _} = dets:open_file(Name, [{file, filename:join(Dir, Name)}]),
				true = ets:from_dets(Name, Name)
		end,
		Names
	).

load_from_files(Files) ->
	sets:from_list(lists:flatten(lists:map(fun load_from_file/1, Files))).

load_from_file(File) ->
	try
		{ok, Binary} = file:read_file(File),
		parse_binary(Binary)
	catch Type:Pattern ->
		Warning = [
			{event, failed_to_load_and_parse_file},
			{file, File},
			{exception, {Type, Pattern}}
		],
		?LOG_WARNING(Warning),
		[]
	end.

parse_binary(Binary) ->
	lists:filtermap(
		fun(TXID) ->
			case TXID of
				<<>> ->
					false;
				TXIDEncoded ->
					case ar_util:safe_decode(TXIDEncoded) of
						{error, invalid} ->
							false;
						{ok, Decoded} ->
							{true, Decoded}
					end
			end
		end,
		binary:split(Binary, <<"\n">>, [global])
	).

load_from_urls(URLs) ->
	sets:from_list(lists:flatten(lists:map(fun load_from_url/1, URLs))).

load_from_url(URL) ->
	try
		{ok, {_Scheme, _UserInfo, Host, Port, Path, Query}} =
			http_uri:parse(case is_list(URL) of true -> list_to_binary(URL); _ -> URL end),
		Reply =
			ar_http:req(#{
				method => get,
				peer => {binary_to_list(Host), Port},
				path => binary_to_list(<<Path/binary, Query/binary>>),
				is_peer_request => false,
				timeout => 20000,
				connect_timeout => 1000
			}),
		case Reply of
			{ok, {{<<"200">>, _}, _, Body, _, _}} ->
				parse_binary(Body);
			_ ->
				ar:console([
					{event, failed_to_download_tx_blacklist},
					{url, URL},
					{reply, Reply}
				]),
				[]
		end
	catch Type:Pattern ->
		ar:console([
			{event, failed_to_load_and_parse_tx_blacklist},
			{url, URL},
			{exception, {Type, Pattern}}
		]),
		[]
	end.

request_header_takedown(State) ->
	case ets:first(ar_tx_blacklist_pending_headers) of
		'$end_of_table' ->
			State;
		TXID ->
			ar_header_sync:request_tx_removal(TXID),
			State#ar_tx_blacklist_state{
				header_takedown_request_timestamp = os:system_time(millisecond)
			}
	end.

request_data_takedown(State) ->
	case ets:first(ar_tx_blacklist_pending_data) of
		'$end_of_table' ->
			State;
		TXID ->
			case ar_data_sync:get_tx_offset(TXID) of
				{ok, {End, Size}} ->
					add_offsets(End, End - Size),
					ar_data_sync:request_tx_data_removal(TXID),
					State2 =
						State#ar_tx_blacklist_state{
							data_takedown_request_timestamp = os:system_time(millisecond)
						},
					State2;
				{error, _Reason} ->
					State
			end
	end.

store_state() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				ets:to_dets(Name, Name)
		end,
		Names
	).

restore_offsets(End, Start) ->
	case ets:next(ar_tx_blacklist_offsets, Start) of
		'$end_of_table' ->
			ok;
		End2 ->
			case ets:lookup(ar_tx_blacklist_offsets, End2) of
				[{_End2, Start2}] when Start2 >= End ->
					ok;
				[{End2, Start2}] ->
					Insert =
						case Start2 < Start of
							true ->
								[{Start, Start2}];
							false ->
								[]
						end,
					Insert2 =
						case End2 > End of
							true ->
								[{End2, End} | Insert];
							false ->
								Insert
						end,
					ets:insert(ar_tx_blacklist_offsets, Insert2),
					case End2 =< End of
						true ->
							ets:delete(ar_tx_blacklist_offsets, End2),
							case End2 < End of
								true ->
									restore_offsets(End, End2);
								false ->
									ok
							end;
						false ->
							ok
					end
			end
	end.

add_offsets(End, Start) ->
	case ets:next(ar_tx_blacklist_offsets, Start) of
		'$end_of_table' ->
			ets:insert(ar_tx_blacklist_offsets, [{End, Start}]);
		End2 ->
			case ets:lookup(ar_tx_blacklist_offsets, End2) of
				[{_End2, Start2}] when Start2 > End ->
					ets:insert(ar_tx_blacklist_offsets, [{End, Start}]);
				[{End2, Start2}] ->
					ets:insert(ar_tx_blacklist_offsets, [{max(End, End2), min(Start, Start2)}]),
					case End2 < End of
						true ->
							ets:delete(ar_tx_blacklist_offsets, End2);
						false ->
							ok
					end
			end
	end.

close_dets() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets
	],
	lists:foreach(
		fun
			(Name) ->
				case dets:close(Name) of
					ok ->
						ok;
					{error, Reason} ->
						ar:err([
							{event, failed_to_close_dets_table},
							{name, Name},
							{reason, Reason}
						])
				end
		end,
		Names
	).
