-module(ar_webhook_worker).
-behaviour(gen_server).

-export([start_link/1, cast_webhook/2]).
-export([init/1, handle_call/3, handle_cast/2]).

-include("ar_config.hrl").

-define(NUMBER_OF_TRIES, 10).
-define(WAIT_BETWEEN_TRIES, 30 * 1000).

-define(BASE_HEADERS, [
	{<<"content-type">>, <<"application/json">>}
]).

%% Public API

start_link(Config) ->
	gen_server:start_link(?MODULE, [Config], []).

cast_webhook(Worker, Event) ->
	gen_server:cast(Worker, {call_webhook, Event}).

%% Generic server callbacks

init([Config]) ->
	{ok, Config}.

handle_call(_, _, Config) ->
	{noreply, Config}.

handle_cast({call_webhook, Event}, Config) ->
	ok = call_webhook(Event, Config),
	{noreply, Config}.

%% Private functions

call_webhook({EventType, Entity}, #config_webhook { events = Events } = Config) ->
	case lists:member(EventType, Events) of
		true -> do_call_webhook({EventType, Entity}, Config, 0);
		false -> ok
	end.

do_call_webhook({EventType, Entity}, Config, N) when N < ?NUMBER_OF_TRIES ->
	#config_webhook{
		url = URL,
		headers = Headers
	} = Config,
	{ok, {Scheme, _UserInfo, Host, Port, Path, Query}} = http_uri:parse(URL),
	{ok, Client} = fusco:start(atom_to_list(Scheme) ++ "://" ++ binary_to_list(Host) ++ ":" ++ integer_to_list(Port), []),
	Result =
		fusco:request(
			Client,
			<<Path/binary, Query/binary>>,
			<<"POST">>,
			?BASE_HEADERS ++ Headers,
			to_json(Entity),
			10000
		),
	case Result of
		{ok, {{<<"200">>, _}, _, _, _, _}} ->
			ok = fusco:disconnect(Client),
			ar:info([
				{ar_webhook_worker, webhook_call_success},
				{event, EventType},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, Result}
			]),
			ok;
		UnsuccessfulResult ->
			ar:warn([
				{ar_webhook_worker, webhook_call_failure},
				{event, EventType},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, UnsuccessfulResult},
				{retry_in, ?WAIT_BETWEEN_TRIES}
			]),
			timer:sleep(?WAIT_BETWEEN_TRIES),
			do_call_webhook({EventType, Entity}, Config, N+1)
	end;
do_call_webhook({EventType, Entity}, Config, _) ->
	#config_webhook{
		url = URL,
		headers = Headers
	} = Config,
	ar:warn([gave_up_webhook_call,
		{event, EventType},
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
