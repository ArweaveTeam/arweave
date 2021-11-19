-module(ar_tx_validator).

-behaviour(gen_server).

-export([start_link/2, validate/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").

-record(state, {
	workers
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

validate(TX, Peer) ->
	catch gen_server:call(?MODULE, {validate, TX, Peer}, 30000).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(Workers) ->
	{ok, #state{ workers = queue:from_list(Workers) }}.

handle_call({validate, TX, Peer}, From, State) ->
	#state{ workers = Q } = State,
	{{value, W}, Q2} = queue:out(Q),
	gen_server:cast(W, {validate, TX, Peer, From}),
	{noreply, State#state{ workers = queue:in(W, Q2) }};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.
