%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_events).

-behaviour(gen_server).

-export([
	event_to_process/1,
	subscribe/1,
	cancel/1,
	send/2
]).

-export([
	start_link/1,
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% Internal state definition.
-record(state, {
	name,
	subscribers = #{}
}).

%%%===================================================================
%%% API
%%%===================================================================

event_to_process(Event) when is_atom(Event) -> list_to_atom("ar_event_" ++ atom_to_list(Event)).

subscribe(Event) when is_atom(Event) ->
	Process = event_to_process(Event),
	gen_server:call(Process, subscribe);
subscribe([]) ->
	[];
subscribe([Event | Events]) ->
	[subscribe(Event) | subscribe(Events)].

cancel(Event) ->
	Process = event_to_process(Event),
	gen_server:call(Process, cancel).

send(Event, Value) ->
	Process = event_to_process(Event),
	case whereis(Process) of
		undefined ->
			error;
		_ ->
			gen_server:cast(Process, {send, self(), Value})
	end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name) ->
    RegName = ar_events:event_to_process(Name),
	gen_server:start_link({local, RegName}, ?MODULE, Name, []).

%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Name) ->
	{ok, #state{ name = Name }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(subscribe , {From, _Tag}, State) ->
	case maps:get(From, State#state.subscribers, unknown) of
		unknown ->
			Ref = erlang:monitor(process, From),
			Subscribers = maps:put(From, Ref, State#state.subscribers),
			{reply, ok, State#state{subscribers = Subscribers}};
		_ ->
			{reply, already_subscribed, State}
	end;

handle_call(cancel, {From, _Tag}, State) ->
	case maps:get(From, State#state.subscribers, unknown) of
		unknown ->
			{reply, unknown, State};
		Ref ->
			Subscribers = maps:remove(From, State#state.subscribers),
			erlang:demonitor(Ref),
			{reply, ok, State#state{ subscribers = Subscribers }}
	end;

handle_call(Request, _From, State) ->
	?LOG_ERROR([{event, unhandled_call}, {message, Request}]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({send, From, Value}, State) ->
	%% Send to the subscribers except self.
	[Pid ! {event, State#state.name, Value}
		|| Pid <- maps:keys(State#state.subscribers), Pid /= From],
	{noreply, State};
handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', _,  process, From, _}, State) ->
	{_, _, State1} = handle_call(cancel, {From, x}, State),
	{noreply, State1};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
