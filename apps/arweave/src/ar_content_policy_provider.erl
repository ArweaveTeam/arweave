-module(ar_content_policy_provider).

-behaviour(gen_server).

%%% ==================================================================
%%% gen_server API
%%% ==================================================================

-export([
	start_link/1
]).

%%% ==================================================================
%%% gen_server callbacks
%%% ==================================================================

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	terminate/2
]).

%%% ==================================================================
%%% Includes
%%% ==================================================================

-include("ar.hrl").

%%% ==================================================================
%%% gen_server API functions
%%% ==================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%% ==================================================================
%%% gen_server callbacks
%%% ==================================================================

init(_) ->
	timer:apply_after(?CONTENT_POLICY_SCAN_INTERVAL, gen_server, cast, [?MODULE, process_policy_content]),
	{ok, []}.

handle_call(_, _, State) ->
	{noreply, State}.

handle_cast(process_policy_content, State) ->
	timer:apply_after(?CONTENT_POLICY_SCAN_INTERVAL, gen_server, cast, [?MODULE, process_policy_content]),
	process_policy_content(ar_meta_db:get(content_policy_provider)),
	{noreply, State};

handle_cast(_, State) ->
	{noreply, State}.

terminate(Reason, _) ->
	ar:err(Reason).

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Process policy content
%% @end
%% -------------------------------------------------------------------
-spec process_policy_content(WalletAddress :: binary()) -> ok.

process_policy_content(<<>>) ->
	ok;

process_policy_content(WA) ->
	WalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
	case [LastTX || {Addr, _, LastTX} <- WalletList, WA =:= Addr] of
		[LastTX] ->
			case ar_storage:read_tx(LastTX) of
				unavailable ->
					ok;
				TX ->
					case lists:member(?CONTENT_POLICY_TYPE, TX#tx.tags) of
						true ->
							process_policy_content_data(TX#tx.data);
						false ->
							ok
					end
			end;
		_ ->
			ok
	end.

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Process policy of data
%% @end
%% -------------------------------------------------------------------
-spec process_policy_content_data(Data :: binary()) -> ok.

process_policy_content_data(<<>>) ->
	ok;

process_policy_content_data(Data) ->
	case binary:split(Data, <<"\n">>) of
		[] ->
			ok;
		TXs ->
			lists:foreach(fun(ID) ->
				case catch ar_util:decode(ID) of
					DecID when is_binary(DecID) ->
						ar_storage:delete_tx(DecID);
					_ ->
						ok
				end
			end, TXs)
	end.
