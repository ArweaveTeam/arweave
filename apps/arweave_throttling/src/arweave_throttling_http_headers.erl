%%% @doc Client-side counterpart to `arweave_limiter_http_headers'.
%%%
%%% Parses the RateLimit-* response headers advertised by a remote
%%% Arweave node (per draft-polli-ratelimit-headers-02, as emitted by
%%% `arweave_limiter_http_headers') and turns them into an
%%% `arweave_throttling_group:update_quota/3' call.
%%%
%%% The remote encodes its limiting group id inside every
%%% `policy="<id> <type>"' quota-comment of the RateLimit-Limit
%%% header. We extract that id so the caller can detect a mismatch
%%% between the group it issued the request under and the group the
%%% remote actually accounted the request against.
%%%
%%% == Header shape ==
%%%
%%% ```
%%% RateLimit-Limit: 100, 100;w=60;policy="general sliding window",
%%%                  50;w=60;burst=50;policy="general leaky bucket"
%%%                  150;w=1;policy="general concurrency"
%%% RateLimit-Remaining: 42
%%% RateLimit-Reset: 7
%%% '''
%%%
%%% The leading integer of RateLimit-Limit is the "expiring-limit"
%%% (mapped to `total'); RateLimit-Remaining maps to `remaining' and
%%% RateLimit-Reset to `reset_seconds'.
%%% @end
-module(arweave_throttling_http_headers).

-export([parse/1, quota_from_headers/2]).

-type headers() :: #{binary() | string() => binary() | string()}
                | [{binary() | string(), binary() | string()}].

%% @doc Parse the RateLimit-* headers into the components needed to
%% refresh a peer's quota. The `group_id' is returned as the raw
%% binary read from the policy comment so the caller can compare it
%% against an expected group without minting atoms from remote input.
-spec parse(headers()) ->
        {ok, #{group_id := binary(),
                total := non_neg_integer(),
                remaining := non_neg_integer(),
                reset_seconds := non_neg_integer()}}
            | {error, term()}.
parse(Headers) when is_list(Headers) ->
    parse(maps:from_list(Headers));
parse(Headers0) ->
    try
        Headers = lowercase_keys(Headers0),
        Limit = fetch(<<"ratelimit-limit">>, Headers),
        Remaining = fetch(<<"ratelimit-remaining">>, Headers),
        Reset = fetch(<<"ratelimit-reset">>, Headers),
        {Total, GroupId} = parse_limit(Limit),
        {ok, #{group_id => GroupId,
            total => Total,
            remaining => to_integer(Remaining),
            reset_seconds => to_integer(Reset)}}
    catch
        throw:{missing_header, _} = Reason ->
            {error, Reason};
        throw:missing_group_id = Reason ->
            {error, Reason};
        throw:malformed_policy = Reason ->
            {error, Reason};
        _E:_R ->
            {error, malformed_headers}
    end.

%% @doc Parse `Headers' and, provided the group id encoded by the
%% remote matches `GroupId', refresh the peer's quota via
%% `arweave_throttling_group:update_quota/3'.
%%
%% Returns `{error, {group_mismatch, Expected, Got}}' when the remote
%% accounted the request under a different group than the caller
%% expected, or `{error, Reason}' when the headers cannot be parsed.
-spec quota_from_headers(atom(), headers()) ->
        ok | {error, term()}.
quota_from_headers(GroupId, Headers) when is_atom(GroupId) ->
    case parse(Headers) of
        {ok, #{group_id := HeaderGroup,
            total := Total,
            remaining := Remaining,
            reset_seconds := Reset}} ->
            case atom_to_binary(GroupId, utf8) =:= HeaderGroup of
                true ->
                    #{total => Total,
                    remaining => Remaining,
                    reset_seconds => Reset};
                false ->
                    {error, {group_mismatch, GroupId, HeaderGroup}}
            end;
        {error, _} = Error ->
            Error
    end.

%% Internals

%% RateLimit-Limit = expiring-limit *( "," quota-policy ). The leading
%% integer is the expiring-limit (`total'); the group id is the first
%% token of the first `policy="..."' quota-comment.
parse_limit(Limit) ->
    [Expiring | _] = binary:split(Limit, <<",">>),
    Total = to_integer(Expiring),
    GroupId = parse_group_id(Limit),
    {Total, GroupId}.

parse_group_id(Limit) ->
    [_, AfterPolicy] = binary:split(Limit, <<"policy=\"">>),
    [Comment | _] = binary:split(AfterPolicy, <<"\"">>),
    case strip_policy_type(string:trim(Comment)) of 
        <<"">> ->
            throw(missing_group_id);
        GroupID ->
            GroupID
    end.                

strip_policy_type(Comment) ->
    case lists:search(fun(Type) -> is_suffix(Type, Comment) end, policy_types()) of
        {value, Type} ->
            Prefix = binary:part(Comment, 0, byte_size(Comment) - byte_size(Type)),
            string:trim(Prefix);
        false ->
            %% Unrecognised comment shape — return it verbatim rather
            %% than guessing, so a mismatch surfaces to the caller.
            throw(malformed_policy)
    end.

policy_types() ->
    [<<"usage">>, <<"concurrency">>].

is_suffix(Suffix, Bin) ->
    SuffixSize = byte_size(Suffix),
    BinSize = byte_size(Bin),
    BinSize >= SuffixSize
        andalso binary:part(Bin, BinSize - SuffixSize, SuffixSize) =:= Suffix.

to_integer(Bin) ->
    binary_to_integer(string:trim(to_bin(Bin))).

lowercase_keys(Headers) ->
    maps:fold(fun(K, V, AccIn) ->
            AccIn#{string:lowercase(K) => V}
        end, #{}, Headers).

fetch(Key, Headers) ->
    case maps:get(Key, Headers, badkey) of
        badkey ->
            throw({missing_header, Key});
        Value ->
            to_bin(Value)
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(I) when is_integer(I) -> integer_to_binary(I).
