%%% @doc Unix signal handler for the arweave node.
%%%
%%% Inspired by https://github.com/rabbitmq/rabbitmq-server/pull/2227/files
%%% See https://www.erlang.org/docs/man/os#set_signal-2 and
%%% https://www.erlang.org/docs/man/kernel_app.
-module(arweave_config_signal_handler).
-compile(warnings_as_errors).
-behavior(gen_event).
-export([
	start_link/0
]).
-export([
	init/1,
	terminate/2,
	handle_call/2,
	handle_event/2,
	handle_info/2
]).
-include_lib("kernel/include/logger.hrl").

-type signal() :: sigquit | sigterm | sigusr1 | sigusr2.

-type state() :: #{ signal() => pos_integer() }.

%% @doc Start the unix signal handler.
start_link() ->
	_ = gen_event:delete_handler(erl_signal_server, ?MODULE, []),
	ok = gen_event:swap_sup_handler(
		erl_signal_server ,
		{erl_signal_handler, []},
		{?MODULE, []}
	),
	set_signals(),
	gen_event:start_link({local, ?MODULE}).

%% @doc List of supported Unix signals.
-spec signals() -> [signal()].
signals() ->
	[
		sigquit,
		sigterm,
		sigusr1,
		sigusr2
	].

%% Dispatch a signal to its handler.
-spec signal(Signal, State) -> Return when
	Signal :: signal(),
	State :: state(),
	Return :: {ok, state()}.
signal(E = sigquit, State) ->
	%% Halt with a core dump.
	erlang:halt("sigquit received", [{flush, true}]),
	update_state(E, State);
signal(E = sigterm, State) ->
	init:stop(0),
	update_state(E, State);
signal(E = sigusr1, State) ->
	%% Print diagnostic state to the logs for live debugging.
	sigusr1(),
	update_state(E, State);
signal(E = sigusr2, State) ->
	%% Reserved for runtime recovery actions (e.g. re-register with
	%% epmd).
	sigusr2(),
	update_state(E, State);
signal(E, State) ->
	%% Fall back to `erl_signal_handler` for unhandled signals.
	?LOG_INFO("received signal ~p", [E]),
	erl_signal_handler:handle_event(E, State),
	update_state(E, State).

sigusr1() ->
	spawn(fun () -> arweave_diagnostic:all() end).

sigusr2() ->
	ok.

-spec init(any()) -> {ok, state()}.
init(_) ->
	erlang:process_flag(trap_exit, true),
	{ok, #{}}.

-spec handle_event(Signal, State) -> Return when
	Signal :: signal(),
	State :: state(),
	Return :: {ok, state()}.
handle_event(Signal, State) when is_atom(Signal) ->
	?LOG_INFO("received signal ~p", [Signal]),
	try
		signal(Signal, State)
	catch
		_:_ ->
			{ok, State}
	end;
handle_event(Event, State) ->
	?LOG_DEBUG("received unexpected event: ~p", [Event]),
	{ok, State}.

-spec handle_info(Event, State) -> Return when
	Event :: any(),
	State :: state(),
	Return :: {ok, state()}.
handle_info(Event, State) ->
	?LOG_DEBUG("received unexpected event: ~p", [Event]),
	{ok, State}.

-spec handle_call(Event, State) -> Return when
	Event :: any(),
	State :: state(),
	Return :: {ok, ok, state()}.
handle_call(Event, State) ->
	?LOG_DEBUG("received unexpected event: ~p", [Event]),
	{ok, ok, State}.

-spec terminate(Reason, State) -> Return when
	Reason :: any(),
	State :: state(),
	Return :: ok.
terminate(_Reason, _State) ->
	?LOG_INFO("unix signal handler stopped"),
	ok.

set_signals() ->
	[
		begin
			?LOG_DEBUG("catch signal ~p", [S]),
			os:set_signal(S, handle)
		end
		|| S <- signals()
	].

%% Count signals received per type (for debugging).
-spec update_state(Signal, State) -> Return when
	Signal :: signal(),
	State :: state(),
	Return :: {ok, state()}.
update_state(Signal, State) ->
	Value = maps:get(Signal, State, 0),
	{ok, State#{ Signal => Value + 1 }}.
