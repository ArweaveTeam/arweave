%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc An internal FSM implementation for Arweave Config.
%%%
%%% This kind of abstraction helps to create tests on individual part
%%% of functions, based on the returned values. With it, one controls
%%% both the input and the output. Side effects functions can also be
%%% isolated easily.
%%%
%%% The state passed to all callback function is not enforced, and can
%%% be of any Erlang type.
%%%
%%% == Examples ==
%%%
%%% `arweave_config_fsm' is a really simple finite state machine,
%%% inspired by `gen_statem' and `gen_fsm'. Each function returns a
%%% tuple with the next function to call. The state is passed to the
%%% next function, until one function is returning `{ok, term()}' to
%%% end the pipeline.
%%%
%%% ```
%%% -module(fsm_test).
%%% -export([start/1]).
%%% -export([first/1, second/1, third/1]).
%%%
%%% % starter
%%% start(Opts) ->
%%%   State = #{},
%%%   arweave_config_fsm:do_loop(?MODULE, first, State).
%%%
%%% % callback transition
%%% first(State) ->
%%%   {next, second, State#{ first => ok }}.
%%%
%%% % module callback transition and error return
%%% second(State = #{ first := ok }) ->
%%%   {next, {?MODULE, third}, State#{ second => ok }}.
%%% second(#{ first := error }) ->
%%%   {error, "first function failed"}.
%%%
%%% % loop (dangerous) support and final return value
%%% third(#{ reset := true }) ->
%%%   {next, first, #{ state => loop });
%%% third(State) ->
%%%   {ok, final_result}.
%%% '''
%%%
%%% == Metadata for Tracing and Debugging Purpose ==
%%%
%%% Debugging and tracing an execution pipeline can sometime be
%%% complex, even with Erlang tooling. This fsm implement a way to
%%% collect those information on demand, by setting the flag `meta' to
%%% `true' during initialization phase.
%%%
%%% ```
%%% -module(t).
%%% -export([start/0, my_function/1]).
%%%
%%% start() ->
%%%   {ok, value, Metadata} = arweave_config_fsm:init(
%%%     ?MODULE,
%%%     my_function,
%%%     #{ meta => true },
%%%     my_state
%%%   ).
%%%
%%% my_function(State) ->
%%%   {ok, value}.
%%% '''
%%%
%%% `Metadata' variable will contain the execution history with the
%%% timestamp when it was executed and the module/function callback. A
%%% counter is also available to have the number of step executed.
%%%
%%% Note: this will have a small impact on the performance, but it
%%% should be negligible. In case of a long and complex pipeline, the
%%% size of history can grow dangerously and use lot of memory.
%%%
%%% == TODO ==
%%%
%%% Better errors management (e.g. specific error message when a
%%% callback module or a callback function are not atom).
%%%
%%% Custom options (e.g. return metadata or send them to another
%%% process during execution).
%%%
%%% Enforce behavior.
%%%
%%% Add delay/timeout support.
%%%
%%% Permits to filter what we want to store in meta-data, and/or allow
%%% lambda function to collect custom data.
%%%
%%% Add a debug state where the returned values are stored.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_fsm).
-compile(warnings_as_errors).
-export([init/3, init/4]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% type definition, usefull for behavior feature and DRY.
%%--------------------------------------------------------------------
% a module callback as atom.
-type fsm_module() :: atom().

% a function callbcak as atom.
-type fsm_callback() :: atom().

% available fsm options/parameters.
-type fsm_opts() :: #{
	meta => boolean()
}.

% fsm state defined by developer during initialization or during
% execution.
-type fsm_state() :: term().

% fsm metadata used to store and collect stats and information about
% fsm execution.
-type fsm_meta_history() :: #{
	timestamp => pos_integer(),
	module => fsm_module(),
	callback => fsm_callback(),
	process_info => map(),
	pid => pid()
}.
-type fsm_metadata() :: #{
	meta => boolean(),
	history => [fsm_meta_history()],
	counter => pos_integer()
}.

% values returned by the callback defined by the developer.
-type fsm_callback_return() ::
	meta |
	{ok, term()} |
	{next, fsm_callback(), fsm_state()} |
	{next, fsm_module(), fsm_callback(), fsm_state()} |
	{error, term()} |
	{error, term(), fsm_state()}.

% values always returned by the fsm after the final callback.
-type fsm_return() ::
	{ok, term()} |
	{ok, term(), fsm_state() | fsm_metadata()} |
	{ok, term(), fsm_state(), fsm_metadata()} |
	{error, term()} |
	{error, term(), fsm_state()}.

%%--------------------------------------------------------------------
%% if one wants to use it as behavior, callback_name is an example
%% function, and no errors/warnings will be reported if this one is
%% not explicitely defined.
%%--------------------------------------------------------------------
-callback(
	callback_name(fsm_state()) -> fsm_callback_return()
).
-optional_callbacks(callback_name/1).

%%--------------------------------------------------------------------
%% @doc `arweave_config_fsm' initialize/starter function.
%% @see init/4
%% @end
%%--------------------------------------------------------------------
-spec init(Module, Callback, State) -> Return when
	Module :: fsm_module(),
	Callback :: fsm_callback(),
	State :: fsm_state(),
	Return :: fsm_return().

init(Module, Callback, State) ->
	init(Module, Callback, #{}, State).

%%--------------------------------------------------------------------
%% @doc `arweave_config_fsm' initialize/starter function. A meta
%% parameter can be configured during initialization, storing
%% information about the pipelined functions and offering a way to
%% trace the execution of the fsm.
%% @end
%%--------------------------------------------------------------------
-spec init(Module, Callback, Opts, State) -> Return when
	Module :: fsm_module(),
	Callback :: fsm_callback(),
	Opts :: fsm_opts(),
	State :: fsm_state(),
	Return :: fsm_return().

init(Module, Callback, Opts, State) ->
	init_meta(Module, Callback, Opts, State).

%%--------------------------------------------------------------------
%% @hidden
%% @doc initialize metadata.
%% @end
%%--------------------------------------------------------------------
init_meta(Module, Callback, Opts = #{ meta := true }, State) ->
	Meta = #{
		opts => Opts,
		meta => maps:get(meta, Opts, false),
		history => [meta_history(Module, Callback)],
		counter => 0
	},
	do_loop(Module, Callback, State, Meta);
init_meta(Module, Callback, _Opts, State) ->
	do_loop(Module, Callback, State, #{}).

%%--------------------------------------------------------------------
%% @hidden
%% @doc main loop where all callbacks are executed.
%% @end
%%--------------------------------------------------------------------
-spec do_loop(Module, Callback, State, Meta) -> Return when
	Module :: fsm_module(),
	Callback :: fsm_callback(),
	State :: fsm_state(),
	Meta :: fsm_metadata(),
	Return :: fsm_return().

do_loop(Module, Callback, State, Meta) ->
	try
		Return = erlang:apply(Module, Callback, [State]),
		do_eval(
			Module,
			Callback,
			Return,
			State,
			update_meta(Module, Callback, Meta)
		)
	catch
		_Error:Reason ->
			do_eval(
				Module,
				Callback,
				{error, Reason},
				State,
				update_meta(Module, Callback, Meta)
			 )
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc evaluate the return of a callback and do the routing part.
%% @end
%%--------------------------------------------------------------------
-spec do_eval(Module, Callback, CallbackReturn, State, Meta) -> Return when
	Module :: fsm_module(),
	Callback :: fsm_callback(),
	CallbackReturn :: fsm_callback_return(),
	State :: fsm_state(),
	Meta :: fsm_metadata(),
	Return :: fsm_return().

% return latested value with meta information and stop the fsm.
do_eval(_Module, _Callback, {ok, Return}, _State, Meta = #{ meta := true }) ->
	{ok, Return, Meta};

% return latest value with meta information and state then stop the
% fsm.
do_eval(_Module, _Callback, {ok, Return, NewState}, _State, Meta = #{ meta := true}) ->
	{ok, Return, NewState, Meta};

% final evaluation, return the value from the callback.
do_eval(_Module, _Callback, {ok, Return}, _State, _Meta) ->
	{ok, Return};

% final evaluation, return the value from the callback and its last
% state.
do_eval(_Module, _Callback, {ok, Return, NewState}, _State, _Meta) ->
	{ok, Return, NewState};

% return meta information and stop the fsm.
do_eval(_Module, _Callback, meta, _State, Meta) ->
	{meta, Meta};

% fsm transition with a new callback and a new state on the same module
% callback.
do_eval(Module, _Callback, {next, NextCallback, NewState}, _State, Meta)
	when is_atom(NextCallback) ->
		do_loop(
			Module,
			NextCallback,
			NewState,
			Meta
		);

% fsm transition with a new state. this function will switch to
% another module and another callback with a new state.
do_eval(_Module, _Callback, {next, NextModule, NextCallback, NewState}, _State, Meta)
	when is_atom(NextModule), is_atom(NextCallback) ->
		do_loop(
			NextModule,
			NextCallback,
			NewState,
			Meta
		);

% fsm error management with debugging feature for traceability.
do_eval(Module, Callback, {error, Reason}, State, Meta) ->
	{error, #{
			debug => #{
				module => ?MODULE,
				function => ?FUNCTION_NAME,
				function_arity => ?FUNCTION_ARITY,
				line => ?LINE
			},
			reason => Reason,
			module => Module,
			callback => Callback,
			state => State,
			meta => Meta
		 }
	};
do_eval(Module, Callback, {error, Reason, NewState}, State, Meta) ->
	{error, #{
			debug => #{
				module => ?MODULE,
				function => ?FUNCTION_NAME,
				function_arity => ?FUNCTION_ARITY,
				line => ?LINE
			},
			reason => Reason,
			module => Module,
			callback => Callback,
			state => State,
			new_state => NewState,
			meta => Meta
		 }, NewState
	};

% fsm error callback returned value.
do_eval(Module, Callback, Return, State, Meta) ->
	{error, #{
			debug => #{
				module => ?MODULE,
				function => ?FUNCTION_NAME,
				function_arity => ?FUNCTION_ARITY,
				line => ?LINE
			},
			reason => unsupported_return,
			return => Return,
			module => Module,
			callback => Callback,
			state => State,
			meta => Meta
		}
	}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc update metadata information if enabled.
%% @end
%%--------------------------------------------------------------------
-spec update_meta(Module, Callback, Meta) -> Return when
	Module :: fsm_module(),
	Callback :: fsm_callback(),
	Meta :: fsm_metadata(),
	Return :: Meta.

update_meta(Module, Callback, Meta = #{ meta := true }) ->
	History = maps:get(history, Meta),
	Counter = maps:get(counter, Meta),
	NewHistory= [meta_history(Module, Callback)|History],
	Meta#{
		history => NewHistory,
		counter => Counter+1
	};
update_meta(_, _, Meta) ->
	Meta.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc generate a meta history item.
%% @end
%%--------------------------------------------------------------------
meta_history(Module, Callback) ->
	#{
		timestamp => erlang:system_time(),
		module => Module,
		callback => Callback,
		process_info => meta_process_info(),
		pid => self()
	 }.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc wrapper around erlang:process_info/2
%% @end
%%--------------------------------------------------------------------
meta_process_info() ->
	maps:from_list(
		erlang:process_info(
			self(),
			[
				heap_size,
				message_queue_len,
				reductions,
				stack_size,
				status,
				total_heap_size,
				trap_exit
			]
		 )
	).
