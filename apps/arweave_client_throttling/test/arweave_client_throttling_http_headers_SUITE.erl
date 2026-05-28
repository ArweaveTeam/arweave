%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Tests for `arweave_client_throttling_http_headers'.
%%%
%%% Exercises both the pure `parse/1' path and the
%%% `update_quota_from_headers/3' path that applies the parsed quota
%%% to a running group. The header fixtures match the exact wire
%%% format produced by `arweave_limiter_http_headers'.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_http_headers_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
    parse_well_formed/1,
    parse_case_insensitive_names/1,
    parse_accepts_map/1,
    parse_missing_header/1,
    parse_malformed_limit/1,
    update_applies_quota_on_match/1,
    update_rejects_group_mismatch/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_client_throttling_http_headers).
-define(GROUP, general).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_client_throttling_http_headers"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

all() ->
    [
        parse_well_formed,
        parse_case_insensitive_names,
        parse_accepts_map,
        parse_missing_header,
        parse_malformed_limit,
        update_applies_quota_on_match,
        update_rejects_group_mismatch
    ].

%% @doc A well-formed header set parses into the expected components,
%% with the group id recovered from the policy comment.
parse_well_formed(_Config) ->
    Headers = headers(<<"general">>, 200, 42, 7),
    {ok, Parsed} = ?M:parse(Headers),
    ?assertEqual(#{group_id => <<"general">>,
                   total => 200,
                   remaining => 42,
                   reset_seconds => 7}, Parsed),
    ok.

%% @doc Header names are matched case-insensitively.
parse_case_insensitive_names(_Config) ->
    Headers = [{<<"RaTeLiMiT-LiMiT">>,
                limit_value(<<"data_sync_record">>, 10)},
               {<<"ratelimit-remaining">>, <<"3">>},
               {<<"RATELIMIT-RESET">>, <<"1">>}],
    {ok, Parsed} = ?M:parse(Headers),
    ?assertEqual(#{group_id => <<"data_sync_record">>,
                   total => 10,
                   remaining => 3,
                   reset_seconds => 1}, Parsed),
    ok.

%% @doc The headers may be supplied as a map as well as a proplist.
parse_accepts_map(_Config) ->
    Headers = maps:from_list(headers(<<"general">>, 100, 99, 0)),
    {ok, Parsed} = ?M:parse(Headers),
    ?assertMatch(#{group_id := <<"general">>,
                   total := 100,
                   remaining := 99,
                   reset_seconds := 0}, Parsed),
    ok.

%% @doc A missing header is reported, not silently defaulted.
parse_missing_header(_Config) ->
    Headers = [{<<"ratelimit-limit">>, limit_value(<<"general">>, 10)},
               {<<"ratelimit-reset">>, <<"1">>}],
    ?assertEqual({error, {missing_header, <<"ratelimit-remaining">>}},
                 ?M:parse(Headers)),
    ok.

%% @doc A RateLimit-Limit value without a parseable policy comment is
%% rejected as malformed.
parse_malformed_limit(_Config) ->
    Headers = [{<<"ratelimit-limit">>, <<"not a valid limit">>},
               {<<"ratelimit-remaining">>, <<"3">>},
               {<<"ratelimit-reset">>, <<"1">>}],
    ?assertEqual({error, malformed_headers}, ?M:parse(Headers)),
    ok.

%% @doc How the headers are parsed into an internal quota map.
update_applies_quota_on_match(_Config) ->
    Headers = headers(<<"general">>, 200, 42, 0),

    ?assertMatch(
       #{total := 200,
         remaining := 42,
         reset_seconds := 0},
       ?M:quota_from_headers(?GROUP, Headers)),

    ok.

%% @doc When the remote accounted the request under a different group
%% than expected, the mismatch is reported.
update_rejects_group_mismatch(_Config) ->
    Headers = headers(<<"data_sync_record">>, 200, 42, 0),
    ?assertEqual({error, {group_mismatch, ?GROUP, <<"data_sync_record">>}},
                 ?M:quota_from_headers(?GROUP, Headers)),

    ok.

%% Helpers

%% Build the full RateLimit-* header proplist exactly as
%% arweave_limiter_http_headers would emit it.
headers(GroupBin, Total, Remaining, Reset) ->
    [{<<"RateLimit-Limit">>, limit_value(GroupBin, Total)},
     {<<"RateLimit-Remaining">>, integer_to_binary(Remaining)},
     {<<"RateLimit-Reset">>, integer_to_binary(Reset)}].

%% RateLimit-Limit value: the expiring-limit followed by the three
%% quota-policy comments, each tagged with the group id.
limit_value(GroupBin, ExpiringLimit) ->
    iolist_to_binary(
      io_lib:format(
        "~B, 10;w=1;policy=\"~s sliding window\", "
        "450;w=1;burst=450;policy=\"~s leaky bucket\" "
        "500;w=1;policy=\"~s concurrency\" ",
        [ExpiringLimit, GroupBin, GroupBin, GroupBin])).
