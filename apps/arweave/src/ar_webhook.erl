%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
-module(ar_webhook).

-behaviour(gen_server).

-export([
	start_link/1
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar_config.hrl").

-define(NUMBER_OF_TRIES, 10).
-define(WAIT_BETWEEN_TRIES, 30 * 1000).

-define(BASE_HEADERS, [
	{<<"content-type">>, <<"application/json">>}
]).

%% Internal state definition.
-record(state, {
	url,
	headers
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
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
init(Hook) ->
	?LOG_DEBUG("Started web hook for ~p", [Hook]),
	lists:map(
		fun (transaction) ->
				ok = ar_events:subscribe(tx);
			(block) ->
				ok = ar_events:subscribe(block);
			(_) ->
				?LOG_ERROR("Wrong event name in webhook ~p", [Hook])
		end,
		Hook#config_webhook.events
	),
	State = #state{
		url = Hook#config_webhook.url,
		headers = Hook#config_webhook.headers
	},
	{ok, State}.

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
handle_call(Request, _From, State) ->
	?LOG_ERROR("unhandled call: ~p", [Request]),
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
handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
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
handle_info({event, block, {new, Block, _Source}}, State) ->
	?LOG_DEBUG("Web hook triggered on: block ~s", [ar_util:encode(Block#block.indep_hash)]),
	URL = State#state.url,
	Headers = State#state.headers,
	call_webhook(URL, Headers, Block, block),
	{noreply, State};

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info({event, tx, {new, TX, _Source}}, State) ->
	?LOG_DEBUG("Web hook triggered on: tx ~s", [ar_util:encode(TX#tx.id)]),
	URL = State#state.url,
	Headers = State#state.headers,
	call_webhook(URL, Headers, TX, transaction),
	{noreply, State};

handle_info({event, tx, _}, State) ->
	{noreply, State};

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

call_webhook(URL, Headers, Entity, Event) ->
	do_call_webhook(URL, Headers, Entity, Event, 0).

do_call_webhook(URL, Headers, Entity, Event, N) when N < ?NUMBER_OF_TRIES ->
	#{ host := Host, path := Path } = Map = uri_string:parse(URL),
	Query = maps:get(query, Map, <<>>),
	Peer = case maps:get(port, Map, undefined) of
		undefined ->
			case maps:get(scheme, Map, undefined) of
				"https" ->
					{binary_to_list(Host), 443};
				_ ->
					{binary_to_list(Host), 1984}
			end;
		Port ->
			{binary_to_list(Host), Port}
	end,
	case catch
		ar_http:req(#{
			method => post,
			peer => Peer,
			path => binary_to_list(<<Path/binary, Query/binary>>),
			headers => ?BASE_HEADERS ++ Headers,
			body => to_json(Entity),
			timeout => 10000,
			is_peer_request => false
		})
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} = Result ->
			?LOG_INFO([
				{ar_webhook_worker, webhook_call_success},
				{event, Event},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, Result}
			]),
			ok;
		Error ->
			?LOG_ERROR([
				{ar_webhook_worker, webhook_call_failure},
				{event, Event},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, Error},
				{retry_in, ?WAIT_BETWEEN_TRIES}
			]),
			timer:sleep(?WAIT_BETWEEN_TRIES),
			do_call_webhook(URL, Headers, Entity, Event, N + 1)
	end;
do_call_webhook(URL, Headers, Entity, Event, _N) ->
	?LOG_WARNING([gave_up_webhook_call,
		{event, Event},
		{id, entity_id(Entity)},
		{url, URL},
		{headers, Headers},
		{number_of_tries, ?NUMBER_OF_TRIES},
		{wait_between_tries, ?WAIT_BETWEEN_TRIES}
	]),
	ok.

entity_id(#block { indep_hash = ID }) -> ar_util:encode(ID);
entity_id(#tx { id = ID }) -> ar_util:encode(ID).

to_json(#block {} = Block) ->
	{JSONKVPairs} = ar_serialize:block_to_json_struct(Block),
	JSONStruct = {lists:keydelete(wallet_list, 1, JSONKVPairs)},
	ar_serialize:jsonify({[{block, JSONStruct}]});
to_json(#tx {} = TX) ->
	{JSONKVPairs1} = ar_serialize:tx_to_json_struct(TX),
	JSONKVPairs2 = lists:keydelete(data, 1, JSONKVPairs1),
	JSONKVPairs3 = [{data_size, TX#tx.data_size} | JSONKVPairs2],
	JSONStruct = {JSONKVPairs3},
	ar_serialize:jsonify({[{transaction, JSONStruct}]}).
