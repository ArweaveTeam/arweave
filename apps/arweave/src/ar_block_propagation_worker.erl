-module(ar_block_propagation_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({send_block, SendFun, RetryCount, From}, State) ->
	case SendFun() of
		{ok, {{<<"412">>, _}, _, _, _, _}} when RetryCount > 0 ->
			ar_util:cast_after(2000, self(),
					{send_block, SendFun, RetryCount - 1, From}),
			{noreply, State};
		_ ->
			From ! {worker_sent_block, self()},
			{noreply, State}
	end;

handle_cast({send_block2, Peer, SendAnnouncementFun, SendFun, RetryCount, From}, State) ->
	case SendAnnouncementFun() of
		{ok, {{<<"412">>, _}, _, _, _, _}} when RetryCount > 0 ->
			ar_util:cast_after(2000, self(),
					{send_block2, Peer, SendAnnouncementFun, SendFun,
							RetryCount - 1, From});
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case catch ar_serialize:binary_to_block_announcement_response(Body) of
				{'EXIT', Reason} ->
					ar_events:send(peer, {bad_response,
							{Peer, block_announcement, Reason}}),
					From ! {worker_sent_block, self()};
				{error, Reason} ->
					ar_events:send(peer, {bad_response,
							{Peer, block_announcement, Reason}}),
					From ! {worker_sent_block, self()};
				{ok, #block_announcement_response{ missing_tx_indices = L,
						missing_chunk = MissingChunk, missing_chunk2 = MissingChunk2 }} ->
					case SendFun(MissingChunk, MissingChunk2, L) of
						{ok, {{<<"418">>, _}, _, Bin, _, _}} when RetryCount > 0 ->
							case parse_txids(Bin) of
								error ->
									ok;
								{ok, TXIDs} ->
									SendFun(MissingChunk, MissingChunk2, TXIDs)
							end;
						{ok, {{<<"419">>, _}, _, _, _, _}} when RetryCount > 0 ->
							SendFun(true, true, L);
						_ ->
							ok
					end,
					From ! {worker_sent_block, self()}
			end;
		_ ->	%% 208 (the peer has already received this block) or
				%% an unexpected response.
			From ! {worker_sent_block, self()}
	end,
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({gun_down, _PID, http, closed, _, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

parse_txids(<< TXID:32/binary, Rest/binary >>) ->
	case parse_txids(Rest) of
		error ->
			error;
		{ok, TXIDs} ->
			{ok, [TXID | TXIDs]}
	end;
parse_txids(<<>>) ->
	{ok, []}.
