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

handle_cast({send_block, SendFun, From}, State) ->
	case SendFun() of
		{ok, {{<<"412">>, _}, _, _, _, _}} ->
			ar_util:cast_after(5000, self(), {send_block_retry, SendFun, From}),
			{noreply, State};
		_ ->
			From ! {worker_sent_block, self()},
			{noreply, State}
	end;

handle_cast({send_block_retry, SendFun, From}, State) ->
	SendFun(),
	From ! {worker_sent_block, self()},
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
