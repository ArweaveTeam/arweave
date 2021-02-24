%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc Rating. We should compute rating for every single peer in
%%% order to give highest priority for the good nodes and decrease
%%% an influence of the bad ones (including ban for the bad behavior).
%%% Here are 3 kind of variables for the Rating formula - positive, negative, by value.
%%% Positive variables:
%%%   * Join bonus (aka base bonus) - provides for the new node we had
%%%     no record before in the rating table
%%%   * Response - getting response for the request with depending of the time response
%%%   * Push - peer shares an information
%%% Negative variables:
%%%   * Bad/Wrong response on our request
%%%     - malformed
%%%     - 404 for the data
%%%     - timeouts
%%%   * Bad/Wrong request
%%%     - to get any information
%%%     - to post (tx, block)
%%% By value:
%%%   * Time response - descrease rating for the slowest peers and increase
%%%                     for the others
%%%   * Lifespan -  age influencing. its getting bigger by the time from 0 to 1
%%%
%%%                                 1
%%%                influence = ------------ + 1
%%%                             -EXP(T/S)
%%%
%%%				T - number of days since we got this peer
%%%				S - how slow this influence should growth
%%%
%%%	This module also provides triggering mechanic in order to handle conditioned
%%%	behaviour (if some action was repeated N times during the period P).
%%%
%%% @end
-module(ar_rating).

-behaviour(gen_server).

-export([
	start_link/0,
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export([
	get/1,
	get_banned/0,
	get_top/1,
	rate_with_parameters/2,
	get_top_joined/1,
	influence/1,

	set_triggers/1,
	set_rates/1
]).


-include_lib("arweave/include/ar_rating.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(COMPUTE_RATING_PERIOD, 60000).

%% Internal state definition.
-record(state, {
	% Recompute ratings for the peers who got updates
	% and write them into the RocksDB
	peers_got_changes = #{},

	% Rating map defines a rate for the action.
	rates = #{
		% bonuses for incoming requests
		{request, tx} => 10,
		{request, block} => 20,
		{request, chunk} => 30,
		% Rate for the push/response = Bonus - T (in ms). longer time could make this value negative
		{push, tx} => 1000,
		{push, block} => 2000,
		{response, tx} => 1000,
		{response, block} => 2000,
		{response, chunk} => 3000,
		{response, any} => 1000,
		% penalties
		{request, malformed} => -1000,
		{response, malformed} => -10000,
		{response, request_timeout} => -1000,
		{response, connect_timeout} => -1, % can not be 0, otherwise trigger wont be called
		{response, not_found} => -500,
		{push, malformed} => -10000,
		{attack, any} => -10000
	},

	% Call Trigger(Value) if event happend N times during period P(in sec).
	% {Action, ActionType} => {N, P, Trigger, Value}.
	% Triggering call happens if it has a rate with the same key name {Action, ActionType}.
	% Otherwise it will be ignored.
	triggers = #{
		% If we got 30 blocks during last hour from the same peer
		% lets provide an extra bonus for the stable peering.
		{push, block} => {30, 3600, bonus, 500},
		% ban for an hour for the malformed request (10 times during an hour)
		{request, malformed} => {10, 3600, ban, 60},
		% Exceeding the limit of 60 requests per 1 minute
		% decreases rate by 10 points.
		{request, tx} => {60, 60, penalty, 10},
		% If we got timeout few times we should handle it as a peer
		% disconnection with removing it from the rating. We also
		% have to inform the other processes that its went offline.
		% Once the last peer went offline this node should handle
		% the disconnection process from the arweave network.
		{response, connect_timeout} => {5, 300, offline, 0},
		% Instant ban for the attack (for the next 24 hours).
		{attack, any} => {1, 0, ban, 1440}
	},

	% RocksDB reference
	db
}).

%%%===================================================================
%%% API
%%%===================================================================
get(Peer) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			BinPeer = term_to_binary(Peer),
			DB = gen_server:call(?MODULE, get_db),
			case ar_kv:get(DB, BinPeer) of
				not_found ->
					undefined;
				{ok, RatingBin} ->
					Rating = binary_to_term(RatingBin),
					{Rating#rating.r, Rating#rating.ban, Rating#rating.host, Rating#rating.port, offline}
			end;
		[{_, Rating}] ->
					{Rating#rating.r, Rating#rating.ban, Rating#rating.host, Rating#rating.port, online}
	end.

get_banned() ->
	DB = gen_server:call(?MODULE, get_db),
	% get all but not banned
	T = os:system_time(second),
	Filter = fun(_,RatingBin) ->
				% exclude banned peers
				Rating = binary_to_term(RatingBin),
				Rating#rating.ban > T
			 end,
	MapAllBin = ar_kv:select(DB, Filter),
	maps:fold(fun(K,V,A) ->
						Rating = binary_to_term(V),
						[{binary_to_term(K), Rating#rating.r,
						 Rating#rating.host, Rating#rating.port} | A]
					end, [], MapAllBin).

get_top(N) ->
	DB = gen_server:call(?MODULE, get_db),
	% get all but not banned
	T = os:system_time(second),
	Filter = fun(_,RatingBin) ->
				% exclude banned peers
				Rating = binary_to_term(RatingBin),
				Rating#rating.ban < T
			 end,

	MapAllBin = ar_kv:select(DB, Filter),
	All = maps:fold(fun(K,V,A) ->
						Rating = binary_to_term(V),
						[{binary_to_term(K),
						  Rating#rating.r,
						  Rating#rating.host,
						  Rating#rating.port} | A]
					end, [], MapAllBin),
	Sorted = lists:sort(fun({_, AR, _, _}, {_, BR, _, _}) ->
				AR > BR
			   end, All),
	lists:sublist(Sorted, N).

get_top_joined(N) ->
	T = os:system_time(second),
	case ets:select(?MODULE, [{ {{peer,'$1'},#rating{r='$2',ban='$3',host='$4',port='$5',_='_'}} ,
								[{'<','$3',T}, {'=:=',{is_integer,'$3'},true}],
								[{{ '$1','$2','$4','$5'}}] }]) of
		[] ->
			[];
		All ->
			Sorted = lists:sort(fun({_, AR, _, _}, {_, BR, _, _}) ->
										AR > BR
								end, All),
			lists:sublist(Sorted, N)
	end.

set_triggers(Triggers) ->
	ok = gen_server:call(?MODULE, {set_triggers, Triggers}).
set_rates(Rating) ->
	ok = gen_server:call(?MODULE, {set_rates, Rating}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
init([]) ->
	State = #state{},
	{ok, Config} = application:get_env(arweave, config),
	Triggers = maps:merge(State#state.triggers, Config#config.triggers),
	Rates = maps:merge(State#state.rates, Config#config.rates),
	{ok, Ref} = ar_kv:open("ratings"),
	State1 = State#state{
				db = Ref,
				rates = Rates,
				triggers = Triggers
			},
	ar_events:subscribe([peer]),
	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	{ok, State1}.

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
handle_call(get_db, _From, State) ->
	{reply, State#state.db, State};
handle_call({set_triggers, Triggers}, _From, State) ->
	Triggers1 = maps:merge(State#state.triggers, Triggers),
	{reply, ok, State#state{triggers = Triggers1}};
handle_call({set_rates, Rates}, _From, State) ->
	Rates1 = maps:merge(State#state.rates, Rates),
	{reply, ok, State#state{rates = Rates1}};
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
handle_cast(compute_ratings, State) ->
	PeersGotChanges = State#state.peers_got_changes,
	DB = State#state.db,
	maps:map(
		fun(Peer, _Value) ->
			update_rating(Peer, DB)
		end,
		PeersGotChanges
	),
	erlang:send_after(?COMPUTE_RATING_PERIOD, ?MODULE, {'$gen_cast', compute_ratings}),
	{noreply, State#state{peers_got_changes = #{}} };
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

handle_info({event, peer, {Act, Kind, Request}}, State)
	when is_record(Request, event_peer) ->
	Peer = Request#event_peer.peer,
	Time = Request#event_peer.time,
	ActRate = maps:get({Act, Kind}, State#state.rates, 0),
	Rate = rate_with_parameters(ActRate, [{time, Time}]),
	Trigger = maps:get({Act, Kind}, State#state.triggers, undefined),
	T = os:system_time(second),
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			{noreply, State};
		_ when Rate == 0 ->
			% do nothing.
			{noreply, State};
		[{_, Rating}] ->
			Positive = Rate > 0,
			{R, History} = maps:get({Act, Positive}, Rating#rating.rate_group, {0, []}),
			case trigger(Trigger, Peer, History, T) of
				{ExtraRate, [B|_] = History1} when B > T ->
					% peer was banned. trigger puts an UntilTime as a head item
					% in the returned History list for cases if a ban was triggered
					% for the given peer.
					R1 = R + Rate + ExtraRate,
					RG = maps:put({Act, Positive}, {R1, History1}, Rating#rating.rate_group),
					% rating of a banned peer should be immidiatelly updated and stored into DB
					Rating1 = Rating#rating{
						rate_group = RG,
						last_update = T,
						ban = true
					},
					ets:insert(?MODULE, {{peer, Peer}, Rating1}),
					update_rating(Peer, State#state.db),
					{noreply, State};
				{_ExtraRate, offline} ->
					% do nothing
					{noreply, State};
				{ExtraRate, History1} ->
					R1 = R + Rate + ExtraRate,
					RG = maps:put({Act, Positive}, {R1, History1}, Rating#rating.rate_group),
					Rating1 = Rating#rating{
						rate_group = RG,
						last_update = T,
						ban = false
					},
					ets:insert(?MODULE, {{peer, Peer}, Rating1}),
					PeersGotChanges = maps:put(Peer, true, State#state.peers_got_changes),
					{noreply, State#state{peers_got_changes = PeersGotChanges}}
			end
	end;

% just got a new peer
handle_info({event, peer, {joined, Peer, Host, Port}}, State) ->
	% check whether we had a peering with this Peer
	BinPeer = term_to_binary(Peer),
	Rating= case ar_kv:get(State#state.db, BinPeer) of
		not_found ->
			R = #rating{host = Host, port = Port},
			ok = ar_kv:put(State#state.db, BinPeer, term_to_binary(R)),
			R;
		{ok, R} ->
			R1 = R#rating{host = Host, port = Port},
			binary_to_term(R1)
	end,
	ets:insert(?MODULE, {{peer, Peer}, Rating}),
	{noreply, State};

% peer just left
handle_info({event, peer, {left, Peer}}, State) ->
	update_rating(Peer, State#state.db),
	ets:delete(?MODULE, {peer, Peer}),
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, Info}]),
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
terminate(_Reason, State) ->
	ar_kv:close(State#state.db),
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
update_rating(Peer, DB) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			ok;
		[{_, Rating}] ->
			% The influence is getting close to 1 during the time. Division by 3 makes
			% this value much close to 1 in around 10 days. Increasing divider makes
			% this transition longer.
			Influence = influence(Rating),
			% Sum up all the rates.
			R = lists:sum(maps:fold(fun(_,{N,_},A) -> [N|A] end, [], Rating#rating.rate_group)),
			% Apply Influence and store the result.
			Rating1 = Rating#rating{r = trunc(R * Influence)},
			BinPeer = term_to_binary(Peer),
			BinRating = term_to_binary(Rating1),
			ar_kv:put(DB, BinPeer, BinRating),
			ets:insert(?MODULE, {{peer, Peer}, Rating1})
	end.


trigger(undefined, _Peer, History, _T) ->
	% there is no reason to keep the history if trigger wasn't
	% defined for the action.
	{0, History};

trigger({_N, _P, _, _V}, Peer, [H|_] = History, T) when H > T ->
	% The last timestamp was added to the History is in the future (more
	% than time T) it means we got event from banned peer.
	% Send 'ban' again until the time H
	ar_events:send(access, {ban, Peer, H}),
	{0, History};
trigger({_N, P, _, _V}, _Peer, [H|_], T) when T - H > P ->
	% Last event happened longer than P seconds ago, so we dont
	% need to keep old values. Keep the current one only.
	{0, [T]};
trigger({N, _P, _, _V}, _Peer, History, T) when length(History)+1 < N ->
	% not enough events for the triggering. just keep it.
	{0, [T|History]};
trigger({N, P, Trigger, V}, Peer, History, T) ->
	History1 = [T|History],
	Period = T - lists:nth(N, History1),
	case Period > P of
		true when length(History1) > N ->
			{V, lists:sublist(History1, N)};
		true ->
			{V, History1};
		_ when Trigger == ban ->
			% for 'ban' the value of V is in minutes
			BanPeriod = V*60,
			BanTime = T + BanPeriod,
			ar_events:send(access, {ban, Peer, BanTime}),
			{0, [BanTime | History1]};
		_ when Trigger == bonus ->
			{V, History1};
		_ when Trigger == penalty ->
			{-V, History1};
		_ when Trigger == offline ->
			% remove it from the ETS so the rest of the
			% events in the mailbox will be ignored
			ets:delete(?MODULE, {peer, Peer}),
			ar_events:send(peer, {left, Peer}),
			{0, offline}
	end.


rate_with_parameters(X, []) ->
	X;
rate_with_parameters(X, [{time, T}|Parameters]) ->
	rate_with_parameters(X - T, Parameters);
rate_with_parameters(X, [_Unknown | Parameters]) ->
	rate_with_parameters(X, Parameters).


influence(Rating) when is_record(Rating, rating) ->
	% Compute age in days
	Age = (os:system_time(second) - Rating#rating.since)/(60*60*24),
	(1/-math:exp(Age/3))+1.
%%
%% Unit-tests
%%

-include_lib("eunit/include/eunit.hrl").

trigger_undefined_test() ->
	?assertMatch(
		{0, [1]},
		trigger(undefined, peer1, [1], 1)
	).
trigger_banned_test() ->
	[
	 ?assertMatch(
		{0, [5]},
		trigger({1, 1, test, 8}, 0, [5], 4)
	 ),
	 ?assertMatch(
		{0, [5,4,3,2,1]},
		trigger({1, 1, test, 8}, 0, [5,4,3,2,1], 4)
	 )
	].
trigger_empty_test() ->
	?assertMatch(
		{0, [3]},
		trigger({2, 1, test, 8}, peer1, [], 3)
	).
trigger_long_ago_test() ->
	% when the last event happened longer time ago than given period.
	% period = 4
	% current T = 12
	?assertMatch(
		{0, [12]},
		trigger({2, 4, test, 0}, peer1, [5,4,3,2,1], 12)
	).
trigger_cut_the_tail_events_test() ->
	% cut the tail of the event list if it didn't exceed the limit
	% of events for the given period
	?assertMatch(
		{0, [26,25,20]},
		trigger({3, 4, test, 0}, peer1, [25,20,15,10,5,1], 26)
	).

