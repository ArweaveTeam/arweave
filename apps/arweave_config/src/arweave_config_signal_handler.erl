%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration Unix Signal Handler.
%%%
%%% inspired by: https://github.com/rabbitmq/rabbitmq-server/pull/2227/files
%%% see also: https://www.erlang.org/docs/20/man/os#set_signal-2
%%% see also: https://www.erlang.org/docs/20/man/kernel_app#id63489
%%%
%%% @end
%%%===================================================================
-module(arweave_config_signal_handler).
-compile(warnings_as_errors).
-behavior(gen_event).
-export([
	start_link/0,
	signals/0,
	signal/2,
	sigusr1/0,
	sigusr2/0
]).
-export([
	init/1,
	terminate/2,
	handle_call/2,
	handle_event/2,
	handle_info/2
]).
-include_lib("kernel/include/logger.hrl").

-type signal() :: sighup | sigquit | sigterm | sigusr1 | sigusr2.

-type state() :: #{ signal() => pos_integer() }.

%%--------------------------------------------------------------------
%% @doc Starts unix signal handler.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	_ = gen_event:delete_handler(erl_signal_server, ?MODULE, []),
	ok = gen_event:swap_sup_handler(
		erl_signal_server ,
		{erl_signal_handler, []},
		{?MODULE, []}
	),
	set_signals(),
	gen_event:start_link({local, ?MODULE}).

%%--------------------------------------------------------------------
%% @doc Returns supported Unix signals.
%% @end
%%--------------------------------------------------------------------
-spec signals() -> [signal()].

signals() ->
	[
		sighup,
		sigquit,
		sigterm,
		sigusr1,
		sigusr2
	].

%%--------------------------------------------------------------------
%% @doc Executes signal side effect.
%% @end
%%--------------------------------------------------------------------
-spec signal(Signal, State) -> Return when
	Signal :: signal(),
	State :: state(),
	Return :: {ok, state()}.

signal(E = sighup, State) ->
	% 1. reload environment variable
	% 2. reload arguments
	% 3. reload the configuration
	arweave_config_environment:reset(),
	arweave_config_environment:load(),
	arweave_config_legacy:reset(),
	update_state(E, State);
signal(E = sigquit, State) ->
	% stop arweave and generate a core dump
	erlang:halt("sigquit received", [{flush, true}]),
	update_state(E, State);
signal(E = sigterm, State) ->
	% stop arweave, call init:stop/0.
	init:stop(0),
	update_state(E, State);
signal(E = sigusr1, State) ->
	% Custom signal mostly used to print arweave state
	% diagnostic, and debugging information in the logs.
	% In case of odd behavior, this signal can be helpful
	% to extract internal information.
	sigusr1(),
	update_state(E, State);
signal(E = sigusr2, State) ->
	% custom signal mostly used to recover arweave
	% 1. check if arweave is still connected to epmd
	% 2. reconnect to epmd if disconnected
	sigusr2(),
	update_state(E, State);
signal(E, State) ->
	% if the signal is not supported, we use the default
	% behavior from erl_signal_handler.
	?LOG_INFO("received signal ~p", [E]),
	erl_signal_handler:handle_event(E, State),
	update_state(E, State).

%%--------------------------------------------------------------------
%% @doc execute sigusr1. prints diagnostic.
%% @end
%%--------------------------------------------------------------------
sigusr1() ->
	spawn(fun () -> arweave_diagnostic:all() end).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
sigusr2() ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec init(any()) -> {ok, state()}.

init(_) ->
	erlang:process_flag(trap_exit, true),
	{ok, #{}}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_info(Event, State) -> Return when
	Event :: any(),
	State :: state(),
	Return :: {ok, state()}.

handle_info(Event, State) ->
	?LOG_DEBUG("received unexpected event: ~p", [Event]),
	{ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_call(Event, State) -> Return when
	Event :: any(),
	State :: state(),
	Return :: {ok, ok, state()}.

handle_call(Event, State) ->
	?LOG_DEBUG("received unexpected event: ~p", [Event]),
	{ok, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec terminate(Reason, State) -> Return when
	Reason :: any(),
	State :: state(),
	Return :: ok.

terminate(_Reason, _State) ->
	?LOG_INFO("unix signal handler stopped"),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
set_signals() ->
	[
		begin
			?LOG_DEBUG("catch signal ~p", [S]),
			os:set_signal(S, handle)
		end
		|| S <- signals()
	].

%%--------------------------------------------------------------------
%% @hidden
%% @doc counts the amount of signals received/used. Mostly used for
%% debugging purpose.
%% @end
%%--------------------------------------------------------------------
-spec update_state(Signal, State) -> Return when
	Signal :: signal(),
	State :: state(),
	Return :: {ok, state()}.

update_state(Signal, State) ->
	Value = maps:get(Signal, State, 0),
	{ok, State#{ Signal => Value + 1 }}.
