%%% @doc The module manages a transaction blacklist. The blacklisted identifiers
%%% are read from the configured files or downloaded from the configured HTTP endpoints.
%%% The server coordinates the removal of the transaction headers and data and answers
%%% queries about the currently blacklisted transactions and the corresponding global
%%% byte offsets.
%%% @end
-module(ar_tx_blacklist).

-behaviour(gen_server).

-export([start_link/0, start_taking_down/0, is_tx_blacklisted/1, is_byte_blacklisted/1,
		get_next_not_blacklisted_byte/1, notify_about_removed_tx/1,
		norify_about_orphaned_tx/1, notify_about_added_tx/3, store_state/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% The frequency of refreshing the blacklist.
-ifdef(DEBUG).
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 2000).
-else.
-define(REFRESH_BLACKLISTS_FREQUENCY_MS, 10 * 60 * 1000).
-endif.

%% How long to wait before retrying to compose a blacklist from local and external
%% sources after a failed attempt.
-define(REFRESH_BLACKLISTS_RETRY_DELAY_MS, 10000).

%% How long to wait for the response to the previously requested
%% header or data removal (takedown) before requesting it for a new tx.
-ifdef(DEBUG).
-define(REQUEST_TAKEDOWN_DELAY_MS, 1000).
-else.
-define(REQUEST_TAKEDOWN_DELAY_MS, 30000).
-endif.

%% The frequency of checking whether the time for the response to
%% the previously requested takedown is due.
-define(CHECK_PENDING_ITEMS_INTERVAL_MS, 1000).

%% The frequency of persisting the server state.
-ifdef(DEBUG).
-define(STORE_STATE_FREQUENCY_MS, 20000).
-else.
-define(STORE_STATE_FREQUENCY_MS, 10 * 60 * 1000).
-endif.

%% @doc The server state.
-record(ar_tx_blacklist_state, {
	%% The timestamp of the last requested transaction header takedown.
	%% It is used to throttle the takedown requests.
	header_takedown_request_timestamp = os:system_time(millisecond),
	%% The timestamp of the last requested transaction data takedown.
	%% It is used to throttle the takedown requests.
	data_takedown_request_timestamp = os:system_time(millisecond),
	%% A cursor pointing to a TXID in the list of pending unblacklisted transactions.
	%% Some of them might be orphaned or simply non-existent.
	pending_restore_cursor = first,
	unblacklist_timeout = os:system_time(second)
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Start removing blacklisted headers and data, if any.
start_taking_down() ->
	gen_server:cast(?MODULE, start_taking_down).

%% @doc Check whether the given transaction is blacklisted.
is_tx_blacklisted(TXID) ->
	ets:member(ar_tx_blacklist, TXID).

%% @doc Check whether the byte with the given global offset is blacklisted.
is_byte_blacklisted(Offset) ->
	ar_ets_intervals:is_inside(ar_tx_blacklist_offsets, Offset).

%% @doc
%% Return the smallest not blacklisted byte bigger than or equal to
%% the byte at the given global offset.
%% @end
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

%% @doc Notify the server about the orphaned tx caused by the fork.
norify_about_orphaned_tx(TXID) ->
	gen_server:cast(?MODULE, {orphaned_tx, TXID}).

%% @doc Notify the server about the added transaction.
notify_about_added_tx(TXID, End, Start) ->
	gen_server:cast(?MODULE, {added_tx, TXID, End, Start}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = initialize_state(),
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(tx),
	gen_server:cast(?MODULE, refresh_blacklist),
	{ok, _} = timer:apply_interval(?STORE_STATE_FREQUENCY_MS, ?MODULE, store_state, []),
	{ok, #ar_tx_blacklist_state{}}.

handle_call(Request, _From, State) ->
	?LOG_ERROR([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(start_taking_down, State) ->
	gen_server:cast(?MODULE, maybe_restore),
	gen_server:cast(?MODULE, maybe_request_takedown),
	{noreply, State};

handle_cast(refresh_blacklist, State) ->
	case refresh_blacklist() of
		error ->
			timer:apply_after(
				?REFRESH_BLACKLISTS_RETRY_DELAY_MS,
				gen_server,
				cast,
				[self(), refresh_blacklist]
			);
		ok ->
			timer:apply_after(
				?REFRESH_BLACKLISTS_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), refresh_blacklist]
			)
	end,
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

handle_cast(maybe_restore, #ar_tx_blacklist_state{ pending_restore_cursor = Cursor,
		unblacklist_timeout = UnblacklistTimeout } = State) ->
	Now = os:system_time(second),
	ar_util:cast_after(200, ?MODULE, maybe_restore),
	case UnblacklistTimeout + 30000 < Now of
		true ->
			Read =
				case Cursor of
					first ->
						ets:first(ar_tx_blacklist_pending_restore_headers);
					_ ->
						ets:next(ar_tx_blacklist_pending_restore_headers, Cursor)
				end,
			case Read of
				'$end_of_table' ->
					{noreply, State#ar_tx_blacklist_state{ pending_restore_cursor = first,
							unblacklist_timeout = Now }};
				TXID ->
					?LOG_DEBUG([{event, preparing_transaction_unblacklisting},
							{tx, ar_util:encode(TXID)}]),
					ar_events:send(tx, {preparing_unblacklisting, TXID}),
					{noreply, State#ar_tx_blacklist_state{ pending_restore_cursor = TXID,
							unblacklist_timeout = Now }}
			end;
		false ->
			{noreply, State}
	end;

handle_cast({removed_tx, TXID}, State) ->
	case ets:member(ar_tx_blacklist_pending_headers, TXID) of
		false ->
			{noreply, State};
		true ->
			ets:delete(ar_tx_blacklist_pending_headers, TXID),
			{noreply, request_header_takedown(State)}
	end;

handle_cast({orphaned_tx, TXID}, State) ->
	case ets:lookup(ar_tx_blacklist, TXID) of
		[{TXID, End, Start}] ->
			restore_offsets(End, Start),
			ets:insert(ar_tx_blacklist, [{TXID}]);
		_ ->
			ok
	end,
	{noreply, State};

handle_cast({added_tx, TXID, End, Start}, State) ->
	case ets:lookup(ar_tx_blacklist, TXID) of
		[{TXID}] ->
			ets:insert(ar_tx_blacklist, [{TXID, End, Start}]),
			ets:insert(ar_tx_blacklist_pending_data, [{TXID}]),
			{noreply, request_data_takedown(State)};
		[{TXID, CurrentEnd, CurrentStart}] ->
			restore_offsets(CurrentEnd, CurrentStart),
			ets:insert(ar_tx_blacklist, [{TXID, End, Start}]),
			ets:insert(ar_tx_blacklist_pending_data, [{TXID}]),
			{noreply, request_data_takedown(State)};
		_ ->
			{noreply, State}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({removed_range, Ref}, State) ->
	case erlang:get(Ref) of
		undefined ->
			{noreply, State};
		{range, {Start, End}} ->
			erlang:erase(Ref),
			case ets:lookup(ar_tx_blacklist, {End, Start}) of
				[{{End, Start}}] ->
					ets:delete(ar_tx_blacklist_pending_data, {End, Start}),
					{noreply, request_data_takedown(State)};
				_ ->
					{noreply, State}
			end;
		{tx, {TXID, Start, End}} ->
			erlang:erase(Ref),
			case ets:lookup(ar_tx_blacklist, TXID) of
				[{TXID, End, Start}] ->
					ets:delete(ar_tx_blacklist_pending_data, TXID),
					{noreply, request_data_takedown(State)};
				_ ->
					{noreply, State}
			end
	end;

handle_info({event, tx, {ready_for_unblacklisting, TXID}}, State) ->
	?LOG_DEBUG([{event, unblacklisting_transaction}, {tx, ar_util:encode(TXID)}]),
	ets:delete(ar_tx_blacklist_pending_restore_headers, TXID),
	{noreply, State#ar_tx_blacklist_state{ unblacklist_timeout = os:system_time(second) }};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, Reason}]),
	store_state(),
	close_dets().

%%%===================================================================
%%% Private functions.
%%%===================================================================

initialize_state() ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	Dir = filename:join(DataDir, "ar_tx_blacklist"),
	ok = filelib:ensure_dir(Dir ++ "/"),
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restore_headers
	],
	lists:foreach(
		fun
			(Name) ->
				{ok, _} = dets:open_file(Name, [{file, filename:join(Dir, Name)}]),
				true = ets:from_dets(Name, Name)
		end,
		Names
	).

refresh_blacklist() ->
	{ok, Config} = application:get_env(arweave, config),
	WhitelistFiles = Config#config.transaction_whitelist_files,
	case load_from_files(WhitelistFiles) of
		error ->
			error;
		{ok, Whitelist} ->
			WhitelistURLs = Config#config.transaction_whitelist_urls,
			case load_from_urls(WhitelistURLs) of
				error ->
					error;
				{ok, Whitelist2} ->
					refresh_blacklist(sets:union(Whitelist, Whitelist2))
			end
	end.

refresh_blacklist(Whitelist) ->
	{ok, Config} = application:get_env(arweave, config),
	BlacklistFiles = Config#config.transaction_blacklist_files,
	case load_from_files(BlacklistFiles) of
		error ->
			error;
		{ok, Blacklist} ->
			BlacklistURLs = Config#config.transaction_blacklist_urls,
			case load_from_urls(BlacklistURLs) of
				error ->
					error;
				{ok, Blacklist2} ->
					refresh_blacklist(Whitelist, sets:union(Blacklist, Blacklist2))
			end
	end.

refresh_blacklist(Whitelist, Blacklist) ->
	Removed =
		sets:fold(
			fun	(TXID, Acc) when is_binary(TXID) ->
					case not sets:is_element(TXID, Whitelist)
							andalso not ets:member(ar_tx_blacklist, TXID) of
						true ->
							[TXID | Acc];
						false ->
							Acc
					end;
				({End, Start}, Acc) ->
					case ets:member(ar_tx_blacklist, {End, Start}) of
						true ->
							Acc;
						false ->
							[{End, Start} | Acc]
					end
			end,
			[],
			Blacklist
		),
	Restored =
		ets:foldl(
			fun	({End, Start}, Acc) ->
					case sets:is_element({End, Start}, Blacklist) of
						true ->
							Acc;
						false ->
							[{End, Start} | Acc]
					end;
				(Entry, Acc) ->
					TXID = element(1, Entry),
					case sets:is_element(TXID, Whitelist)
							orelse not sets:is_element(TXID, Blacklist) of
						true ->
							[TXID | Acc];
						false ->
							Acc
					end
			end,
			[],
			ar_tx_blacklist
		),
	lists:foreach(
		fun	(TXID) when is_binary(TXID) ->
				ets:insert(ar_tx_blacklist, [{TXID}]),
				ets:insert(ar_tx_blacklist_pending_headers, [{TXID}]),
				ets:insert(ar_tx_blacklist_pending_data, [{TXID}]),
				ets:delete(ar_tx_blacklist_pending_restore_headers, TXID);
			({End, Start}) ->
				ets:insert(ar_tx_blacklist, [{{End, Start}}]),
				ets:insert(ar_tx_blacklist_pending_data, [{{End, Start}}])
		end,
		Removed
	),
	lists:foreach(
		fun	(TXID) when is_binary(TXID) ->
				ets:insert(ar_tx_blacklist_pending_restore_headers, [{TXID}]),
				case ets:lookup(ar_tx_blacklist, TXID) of
					[{TXID}] ->
						ok;
					[{TXID, End, Start}] ->
						restore_offsets(End, Start)
				end,
				ets:delete(ar_tx_blacklist, TXID),
				ets:delete(ar_tx_blacklist_pending_data, TXID),
				ets:delete(ar_tx_blacklist_pending_headers, TXID);
			({End, Start}) ->
				restore_offsets(End, Start),
				ets:delete(ar_tx_blacklist, {End, Start}),
				ets:delete(ar_tx_blacklist_pending_data, {End, Start})
		end,
		Restored
	),
	ok.

load_from_files(Files) ->
	Lists = lists:map(fun load_from_file/1, Files),
	case lists:all(fun(error) -> false; (_) -> true end, Lists) of
		true ->
			{ok, sets:from_list(lists:flatten(Lists))};
		false ->
			error
	end.

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
		error
	end.

parse_binary(Binary) ->
	lists:filtermap(
		fun(Line) ->
			case Line of
				<<>> ->
					false;
				TXIDOrRange ->
					case ar_util:safe_decode(TXIDOrRange) of
						{error, invalid} ->
							case binary:split(TXIDOrRange, <<",">>, [global]) of
								[StartBin, EndBin] ->
									case {catch binary_to_integer(StartBin),
											catch binary_to_integer(EndBin)} of
										{Start, End} when is_integer(Start),
												is_integer(End), End > Start, Start >= 0 ->
											{true, {End, Start}};
										_ ->
											?LOG_WARNING([{event, failed_to_parse_line},
													{line, Line}]),
											false
									end;
								_ ->
									?LOG_WARNING([{event, failed_to_parse_line},
											{line, Line}]),
									false
							end;
						{ok, TXID} ->
							{true, TXID}
					end
			end
		end,
		binary:split(Binary, <<"\n">>, [global])
	).

load_from_urls(URLs) ->
	Lists = lists:map(fun load_from_url/1, URLs),
	case lists:all(fun(error) -> false; (_) -> true end, Lists) of
		true ->
			{ok, sets:from_list(lists:flatten(Lists))};
		false ->
			error
	end.

load_from_url(URL) ->
	try
		#{ host := Host, path := Path, scheme := Scheme } = M = uri_string:parse(URL),
		Query = case maps:get(query, M, not_found) of not_found -> <<>>; Q -> [<<"?">>, Q] end,
		Port = maps:get(port, M, case Scheme of "http" -> 80; "https" -> 443 end),
		Reply =
			ar_http:req(#{
				method => get,
				peer => {Host, Port},
				path => binary_to_list(iolist_to_binary([Path, Query])),
				is_peer_request => false,
				timeout => 20000,
				connect_timeout => 1000
			}),
		case Reply of
			{ok, {{<<"200">>, _}, _, Body, _, _}} ->
				parse_binary(Body);
			_ ->
				?LOG_INFO([
					{event, failed_to_download_tx_blacklist},
					{url, URL},
					{reply, Reply}
				]),
				error
		end
	catch Type:Pattern ->
		?LOG_INFO([
			{event, failed_to_load_and_parse_tx_blacklist},
			{url, URL},
			{exception, {Type, Pattern}}
		]),
		error
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
		TXID when is_binary(TXID)  ->
			case ets:lookup(ar_tx_blacklist, TXID) of
				[{TXID}] ->
					case ar_data_sync:get_tx_offset(TXID) of
						{ok, {End, Size}} ->
							Start = End - Size,
							ets:insert(ar_tx_blacklist, [{TXID, End, Start}]),
							blacklist_offsets(TXID, End, Start, State);
						{error, Reason} ->
							?LOG_WARNING([{event, failed_to_find_blocklisted_tx_in_the_index},
									{tx, ar_util:encode(TXID)},
									{reason, io_lib:format("~p", [Reason])}]),
							ets:delete(ar_tx_blacklist_pending_data, TXID),
							ets:delete(ar_tx_blacklist, TXID),
							State
					end;
				[{TXID, End, Start}] ->
					blacklist_offsets(TXID, End, Start, State)
			end;
		{End, Start} ->
			blacklist_offsets(End, Start, State)
	end.

store_state() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restore_headers
	],
	lists:foreach(
		fun
			(Name) ->
				ets:to_dets(Name, Name)
		end,
		Names
	).

restore_offsets(End, Start) ->
	ar_ets_intervals:delete(ar_tx_blacklist_offsets, End, Start).

blacklist_offsets(End, Start, State) ->
	ar_ets_intervals:add(ar_tx_blacklist_offsets, End, Start),
	Ref = make_ref(),
	erlang:put(Ref, {range, {Start, End}}),
	ar_data_sync:request_data_removal(Start, End, Ref, self()),
	State#ar_tx_blacklist_state{
		data_takedown_request_timestamp = os:system_time(millisecond)
	}.

blacklist_offsets(TXID, End, Start, State) ->
	ar_ets_intervals:add(ar_tx_blacklist_offsets, End, Start),
	Ref = make_ref(),
	erlang:put(Ref, {tx, {TXID, Start, End}}),
	ar_data_sync:request_tx_data_removal(TXID, Ref, self()),
	State#ar_tx_blacklist_state{
		data_takedown_request_timestamp = os:system_time(millisecond)
	}.

close_dets() ->
	Names = [
		ar_tx_blacklist,
		ar_tx_blacklist_pending_headers,
		ar_tx_blacklist_pending_data,
		ar_tx_blacklist_offsets,
		ar_tx_blacklist_pending_restore_headers
	],
	lists:foreach(
		fun
			(Name) ->
				case dets:close(Name) of
					ok ->
						ok;
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_close_dets_table},
							{name, Name},
							{reason, Reason}
						])
				end
		end,
		Names
	).
