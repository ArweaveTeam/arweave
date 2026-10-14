-module(ar_data_root_verification).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%%% Slowly re-check the data roots stored in the default store against the
%%% blocks' transaction paths while data-roots syncing is enabled, and remove
%%% the data roots of blocks whose sizes and start offsets do not match so that
%%% data-roots syncing fetches them again. Earlier releases accepted such data
%%% roots from peers. A copy of the removed data roots is kept in the
%%% migrations index (see ar_data_roots:get_removed_block/2). Only blocks below
%%% the disk pool threshold are checked: peers only provide data roots for
%%% those, and the node builds the rest itself. The position is kept in the
%%% migrations index, so the check continues where it left off after a restart
%%% and picks up blocks stored later.

-record(state, {
                cursor,      % The offset to continue from, or undefined.
                checked = 0, % Blocks checked since the last completed pass.
                mismatches = 0 % Mismatching blocks since the last pass.
               }).

%% The number of blocks checked at a time.
-define(BLOCKS_PER_STEP, 5).

-ifdef(AR_TEST).
-define(MIN_START_DELAY_MS, 0).
-define(MAX_START_DELAY_MS, 1000).
-define(STEP_INTERVAL_MS, 100).
-define(DISABLED_INTERVAL_MS, 1000).
-define(IDLE_INTERVAL_MS, 1000).
-else.
%% The first step runs after a random delay in this range, keeping the check out
%% of the node startup and spreading it across the nodes restarted together.
-define(MIN_START_DELAY_MS, 120_000).
-define(MAX_START_DELAY_MS, 7_200_000).
-define(STEP_INTERVAL_MS, 1000).
%% How often to look at the option and whether the node has joined.
-define(DISABLED_INTERVAL_MS, 60_000).
%% How long to wait before looking for blocks stored after a completed pass,
%% or before retrying after an error.
-define(IDLE_INTERVAL_MS, 600_000).
-endif.

-define(CURSOR_KEY, <<"data_root_verification_cursor">>).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
    Delay = ?MIN_START_DELAY_MS
        + rand:uniform(?MAX_START_DELAY_MS - ?MIN_START_DELAY_MS + 1) - 1,
    arweave_util:cast_after(Delay, self(), step),
    {ok, #state{}}.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE},
                  {request, Request}]),
    {reply, ignored, State}.

handle_cast(step, State) ->
    {Delay, State2} =
        case ar_node:is_joined() andalso
                arweave_config:get(
                  [gossip, data_roots, syncing_enabled]) of
            true ->
                safe_step(State);
            false ->
                {?DISABLED_INTERVAL_MS, State}
        end,
    arweave_util:cast_after(Delay, self(), step),
    {noreply, State2};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE},
                  {message, Message}]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Run a step, backing off on an unexpected exception instead of crashing:
%% a restart would retry at once and could exhaust the supervisor's restart
%% intensity, which also stops data-roots syncing.
safe_step(State) ->
    try
        step(State)
    catch Class:Reason:Stacktrace ->
        ?LOG_ERROR([{event, data_root_verification_step_failed},
                    {class, Class},
                    {reason, io_lib:format("~p", [Reason])},
                    {stacktrace, io_lib:format("~p", [Stacktrace])}]),
        {?IDLE_INTERVAL_MS, State}
    end.

step(#state{ cursor = undefined } = State) ->
    case read_cursor() of
        {ok, Cursor} ->
            step(State#state{ cursor = Cursor });
        {error, Reason} ->
            ?LOG_WARNING([{event, failed_to_read_data_root_verification_cursor},
                          {reason, io_lib:format("~p", [Reason])}]),
            {?IDLE_INTERVAL_MS, State}
    end;
step(#state{ cursor = Cursor, checked = Checked,
             mismatches = Mismatches } = State) ->
    Threshold = ar_disk_pool:get_threshold(),
    case ar_data_roots:verify_stored_blocks(Cursor, Threshold, ?BLOCKS_PER_STEP,
                                            ?DEFAULT_MODULE) of
        {Status, Cursor2, StepChecked, StepMismatches} ->
            [log_mismatch(Mismatch) || Mismatch <- StepMismatches],
            case write_cursor(Cursor2) of
                ok ->
                    ok;
                {error, WriteReason} ->
                    ?LOG_WARNING([
                        {event, failed_to_store_data_root_verification_cursor},
                        {reason, io_lib:format("~p", [WriteReason])}])
            end,
            State2 = State#state{
                       cursor = Cursor2,
                       checked = Checked + StepChecked,
                       mismatches = Mismatches + length(StepMismatches) },
            case Status of
                more ->
                    {?STEP_INTERVAL_MS, State2};
                done ->
                    {?IDLE_INTERVAL_MS, complete_pass(State2)}
            end;
        {error, Reason} ->
            ?LOG_WARNING([{event, failed_to_verify_stored_data_roots},
                          {cursor, Cursor},
                          {reason, io_lib:format("~p", [Reason])}]),
            {?IDLE_INTERVAL_MS, State}
    end.

complete_pass(#state{ checked = 0 } = State) ->
    State;
complete_pass(#state{ cursor = Cursor, checked = Checked,
                      mismatches = Mismatches } = State) ->
    ?LOG_INFO([{event, data_root_verification_pass_complete},
               {blocks_checked, Checked}, {mismatching_blocks, Mismatches},
               {cursor, Cursor}]),
    State#state{ checked = 0, mismatches = 0 }.

log_mismatch({BlockStart, BlockEnd, Reason, removed}) ->
    ?LOG_WARNING([{event, removed_mismatching_data_roots},
                  {block_start_offset, BlockStart},
                  {block_end_offset, BlockEnd},
                  {reason, Reason},
                  {resync, resync_expectation(BlockStart, BlockEnd)}]);
log_mismatch({BlockStart, BlockEnd, Reason, kept}) ->
    ?LOG_ERROR([{event, unexpected_data_root_mismatch},
                {block_start_offset, BlockStart},
                {block_end_offset, BlockEnd},
                {reason, Reason}]).

%% @doc Return whether data-roots syncing will fetch the given block again:
%% it only scans the ranges of the configured storage modules.
resync_expectation(BlockStart, BlockEnd) ->
    case ar_storage_module:get_all(BlockStart, BlockEnd) of
        [] ->
            no_storage_module;
        _ ->
            storage_module
    end.

read_cursor() ->
    case ar_kv:get(ar_data_sync:migration_db(?DEFAULT_MODULE), ?CURSOR_KEY) of
        {ok, << Cursor:?OFFSET_KEY_BITSIZE >>} ->
            {ok, Cursor};
        not_found ->
            {ok, 0};
        {error, _} = Error ->
            Error
    end.

write_cursor(Cursor) ->
    ar_kv:put(ar_data_sync:migration_db(?DEFAULT_MODULE), ?CURSOR_KEY,
              << Cursor:?OFFSET_KEY_BITSIZE >>).
