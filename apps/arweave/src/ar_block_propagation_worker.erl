-module(ar_block_propagation_worker).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
    name % The registered name ar_bridge knows the worker by.
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([Name]) ->
    {ok, #state{ name = Name }}.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({send_block, SendFun, RetryCount, From}, State) ->
    case SendFun() of
        {ok, {{<<"412">>, _}, _, _, _, _}} when RetryCount > 0 ->
            arweave_util:cast_after(2000, self(),
                               {send_block, SendFun, RetryCount - 1, From}),
            {noreply, State};
        _ ->
            From ! {worker_sent_block, State#state.name},
            {noreply, State}
    end;

handle_cast({send_block2, Peer, SendAnnouncementFun, SendFun, RetryCount, From}, State) ->
    case SendAnnouncementFun() of
        {ok, {{<<"412">>, _}, _, _, _, _}} when RetryCount > 0 ->
            arweave_util:cast_after(2000, self(),
                               {send_block2, Peer, SendAnnouncementFun, SendFun,
                                RetryCount - 1, From});
        {ok, {{<<"200">>, _}, _, Body, _, _}} ->
            case catch ar_serialize:binary_to_block_announcement_response(Body) of
                {'EXIT', Reason} ->
                    ?LOG_INFO([{event, send_announcement_response}, {peer, arweave_util:format_peer(Peer)},
                               {exit, Reason}]),
                    ar_peers:issue_warning(Peer, block_announcement, Reason),
                    From ! {worker_sent_block, State#state.name};
                {error, Reason} ->
                    ?LOG_INFO([{event, send_announcement_response}, {peer, arweave_util:format_peer(Peer)},
                               {error, Reason}]),
                    ar_peers:issue_warning(Peer, block_announcement, Reason),
                    From ! {worker_sent_block, State#state.name};
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
                    From ! {worker_sent_block, State#state.name}
            end;
        _ ->    %% 208 (the peer has already received this block) or
            %% an unexpected response.
            From ! {worker_sent_block, State#state.name}
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
    {noreply, State}.

handle_info({gun_down, _PID, http, closed, _, _}, State) ->
    {noreply, State};

handle_info({gun_error, _ConnPid, _StreamRef, {badstate, _Reason}}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, Reason}]),
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
    {ok, []};
parse_txids(_Bin) ->
    error.

%%%===================================================================
%%% Tests.
%%%===================================================================

parse_txids_test() ->
    ?assertEqual({ok, []}, parse_txids(<<>>)),
    ?assertEqual({ok, [<< 1:256 >>, << 2:256 >>]},
            parse_txids(<< 1:256, 2:256 >>)),
    ?assertEqual(error, parse_txids(<< 1:8 >>)),
    ?assertEqual(error, parse_txids(<< 1:256, 2:8 >>)).

%% A 418 reply whose body is not a list of 32-byte transaction identifiers
%% must not keep the worker from reporting back.
malformed_418_reply_test() ->
    Name = ar_block_propagation_worker_test,
    {ok, PID} = start_link(Name),
    Reply = fun(Status, Body) -> {ok, {{Status, <<>>}, [], Body, 0, 0}} end,
    SendAnnouncementFun =
        fun() ->
            Reply(<<"200">>, ar_serialize:block_announcement_response_to_binary(
                    #block_announcement_response{}))
        end,
    SendFun = fun(_, _, _) -> Reply(<<"418">>, << 1:8 >>) end,
    Peer = {127, 0, 0, 1, 1984},
    gen_server:cast(Name,
            {send_block2, Peer, SendAnnouncementFun, SendFun, 1, self()}),
    Result =
        receive
            {worker_sent_block, Worker} ->
                Worker
        after 5000 ->
            timeout
        end,
    gen_server:stop(PID),
    ?assertEqual(Name, Result).
