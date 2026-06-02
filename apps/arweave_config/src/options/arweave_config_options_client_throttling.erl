-module(arweave_config_options_client_throttling).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	validate/0,
	group_ids/0
]).

-ifdef(AR_TEST).
-define(TEST_DEFAULT_REMAINING, 9000).
-endif.

-define(DEFAULT_QUOTA_GENERAL, 450).
-define(DEFAULT_QUOTA_CHUNK, 6000).
-define(DEFAULT_QUOTA_DATA_SYNC_RECORD, 20).
-define(DEFAULT_QUOTA_RECENT_HASH_LIST_DIFF, 120).
-define(DEFAULT_QUOTA_BLOCK_INDEX, 1).
-define(DEFAULT_QUOTA_WALLET_LIST, 1).
-define(DEFAULT_QUOTA_GET_VDF, 90).
-define(DEFAULT_QUOTA_GET_VDF_SESSION, 30).
-define(DEFAULT_QUOTA_GET_PREVIOUS_VDF_SESSION, 30).

%% Hard cap on the number of waiting callers we are willing to queue per
%% peer. Calls received over the cap are rejected immediately with
%% {error, queue_full}.
-define(ARWEAVE_CLIENT_THROTTLING_DEFAULT_MAX_QUEUE_LENGTH, 1000).

%% Window during which two `update_quota' messages are considered to
%% describe the same logical batch of concurrent in-flight requests. Inside
%% the window we take the minimum of the reported `remaining' values
%% (because the smallest one is the most recent server-side view). Outside
%% the window we trust the new value.
-define(ARWEAVE_CLIENT_THROTTLING_DEFAULT_CONCURRENCY_WINDOW_MS, 1000).

specs() ->
    [spec_for(GroupID, Field, Default) ||
        {GroupID, Fields} <- maps:to_list(default_groups()),
        {Field, Default} <- maps:to_list(Fields)].

spec_for(GroupID, Field, Default) ->
    #{
      enabled => true,
      option_key => [client_throttling, GroupID, Field],
      type => type_for(Field),
      default => Default,
      short_description => short_description_for(Field),
      long_description => group_coverage_for(GroupID)
     }.

group_description() ->
	<<"HTTP API client throttling groups.">>.

validate() ->
    ok.

type_for(id) -> atom;
type_for(_) -> pos_integer.


group_coverage_for(chunk) ->
	<<"Group covers: /chunk, /chunk2.">>;
group_coverage_for(data_sync_record) ->
	<<"Group covers: /data_sync_record.">>;
group_coverage_for(recent_hash_list_diff) ->
	<<"Group covers: /recent_hash_list_diff.">>;
group_coverage_for(block_index) ->
	<<"Group covers: /hash_list, /hash_list2, /block_index, "
	  "/block_index2, /block/{type}/{id}/hash_list.">>;
group_coverage_for(wallet_list) ->
	<<"Group covers: /wallet_list, "
	  "/block/{type}/{id}/wallet_list.">>;
group_coverage_for(get_vdf) ->
	<<"Group covers: /vdf, /vdf2.">>;
group_coverage_for(get_vdf_session) ->
	<<"Group covers: /vdf/session, /vdf2/session, "
	  "/vdf3/session, /vdf4/session.">>;
group_coverage_for(get_previous_vdf_session) ->
	<<"Group covers: /vdf/previous_session, "
	  "/vdf2/previous_session, /vdf4/previous_session.">>;
group_coverage_for(metrics) ->
	<<"Group covers: /metrics and its sub-paths.">>;
group_coverage_for(general) ->
	<<"Catch-all group for HTTP endpoints not routed to any other "
	  "throttling group.">>;
group_coverage_for(local_peers) ->
	<<"Applies to every request from a peer listed under "
	  "[peers, <peer>, local], regardless of path. With "
	  "`no_limit` true (the default) these peers bypass rate "
	  "limiting entirely.">>.

short_description_for(id) ->
    <<"GroupId">>;
short_description_for(initial_remaining) ->
	<<"Per-peer request budget within the sliding window; traffic "
	  "beyond this falls through to the leaky bucket.">>;
short_description_for(max_queue_length) ->
	<<"Hard cap on the number of waiting callers we are willing to queue "
          "per peer. Calls received over the cap are rejected immediately.">>;
short_description_for(concurrency_window_ms) ->
	<<"Window during which two `update_quota' messages are considered to "
          "describe the same logical batch of concurrent in-flight requests. "
          "Inside the window we take the minimum of the reported `remaining' "
          "values (because the smallest one is the most recent server-side "
          "view). Outside the window we trust the new value.">>.

group_ids() ->
    maps:keys(default_groups()).

default_groups() ->
    #{chunk => standard(chunk),
      data_sync_record => standard(data_sync_record),
      recent_hash_list_diff => standard(recent_hash_list_diff),
      block_index => standard(block_index),
      wallet_list => standard(wallet_list),
      get_vdf => standard(get_vdf),
      get_vdf_session => standard(get_vdf),
      get_previous_vdf_session => standard(get_previous_vdf_session),
      general => standard(general)
     }.

standard(ID) ->
    #{
        id => ID,
        initial_remaining =>
            default_remaining(ID),
        max_queue_length =>
            ?ARWEAVE_CLIENT_THROTTLING_DEFAULT_MAX_QUEUE_LENGTH,
        concurrency_window_ms =>
            ?ARWEAVE_CLIENT_THROTTLING_DEFAULT_CONCURRENCY_WINDOW_MS
    }.

%% Start with defaults for the server side.
%% FIXME: Perhaps find a better way to extract this from arweave_config
-ifdef(AR_TEST).
default_remaining(_) -> ?TEST_DEFAULT_REMAINING.
-else.
default_remaining(general) ->
    ?DEFAULT_QUOTA_GENERAL;
default_remaining(chunk) ->
    ?DEFAULT_QUOTA_CHUNK;
default_remaining(data_sync_record) ->
    ?DEFAULT_QUOTA_DATA_SYNC_RECORD;
default_remaining(recent_hash_list_diff) ->
    ?DEFAULT_QUOTA_RECENT_HASH_LIST_DIFF;
default_remaining(block_index) ->
    ?DEFAULT_QUOTA_BLOCK_INDEX;
default_remaining(wallet_list) ->
    ?DEFAULT_QUOTA_WALLET_LIST;
default_remaining(get_vdf) ->
    ?DEFAULT_QUOTA_GET_VDF;
default_remaining(get_vdf_session) ->
    ?DEFAULT_QUOTA_GET_VDF_SESSION;
default_remaining(get_previous_vdf_session) ->
    ?DEFAULT_QUOTA_GET_PREVIOUS_VDF_SESSION;
default_remaining(_ID) ->
    %% NOTE: the initial value for this is not so relevant, because
    %% it would get updated after the first successful request sent.
    %% We set this so higher load endpoints can operate immediately,
    %% and lower ones don't get spammed even if start up requires to do so.
    ?DEFAULT_QUOTA_GENERAL.
-endif.
