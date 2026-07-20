%%% @doc Single home for rendering the Prometheus registry into an
%%% exposition and negotiating its format/encoding. Wraps
%%% `prometheus_http_impl:reply/1' so `arweave_metrics_cache' (background
%%% render) and `arweave_metrics_cowboy_handler' (request serving) stay
%%% thin and neither re-derives the reply request shape or the content
%%% negotiation.
-module(arweave_metrics_render).
-test_category([fast]).

-export([render/1, reply/2, is_text_format/1, negotiate_encoding/1]).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% API
%% ===================================================================

%% @doc Render `Registry' into a cacheable exposition: a map with the
%% raw text body (`identity'), its `gzip', and the `content_type'.
%% Forces the text format and identity encoding so the cache holds the
%% uncompressed body and compresses it once per interval rather than
%% once per scrape. Returns `{error, Reply}' if the library declines to
%% render (any non-200 reply).
render(Registry) ->
    case reply(Registry, fun text_identity_header/2) of
        {200, RespHeaders, Body} ->
            Identity = iolist_to_binary(Body),
            {ok, #{content_type => proplists:get_value(content_type, RespHeaders),
                   identity => Identity, gzip => zlib:gzip(Identity)}};
        Other ->
            {error, Other}
    end.

%% @doc Drive `prometheus_http_impl:reply/1' for `Registry', looking up
%% request headers through `HeadersFun'. Collects and formats on the
%% calling process. The request shape (`path'/`standalone') lives here
%% so both callers share it.
reply(Registry, HeadersFun) ->
    prometheus_http_impl:reply(#{path => true, headers => HeadersFun,
                                 registry => Registry, standalone => false}).

%% @doc Whether the client's Accept header negotiates to the plain-text
%% format (rather than protobuf) — i.e. whether the cached body can
%% answer the request.
is_text_format(Accept) ->
    case negotiate_format(Accept) of
        prometheus_text_format -> true;
        _ -> false
    end.

%% @doc The transfer encoding to serve for an Accept-Encoding header:
%% one of `<<"identity">>', `<<"gzip">>', `<<"deflate">>', or
%% `undefined' if none is acceptable.
negotiate_encoding(AcceptEncoding) ->
    accept_encoding_header:negotiate(AcceptEncoding,
                                     [<<"identity">>, <<"gzip">>, <<"deflate">>]).

%% ===================================================================
%% Private functions
%% ===================================================================

negotiate_format(Accept) ->
    case prometheus_http_config:format() of
        auto ->
            accept_header:negotiate(Accept, prometheus_http_config:allowed_formats());
        Format ->
            Format
    end.

%% Header lookup for the background render: force the plain-text format
%% and no transfer encoding so `render/1' gets the raw exposition.
text_identity_header("accept", _Default) -> "text/plain";
text_identity_header("accept-encoding", _Default) -> "identity";
text_identity_header(_Name, Default) -> Default.

%%%===================================================================
%%% Tests.
%%%===================================================================

is_text_format_test() ->
    ?assert(is_text_format(<<"text/plain">>)),
    ?assert(is_text_format(<<"*/*">>)),
    ?assertNot(is_text_format(
                 <<"application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;"
                   "encoding=delimited">>)).

negotiate_encoding_test() ->
    ?assertEqual(<<"gzip">>, negotiate_encoding(<<"gzip">>)),
    ?assertEqual(<<"identity">>, negotiate_encoding(undefined)),
    ?assertEqual(<<"deflate">>, negotiate_encoding(<<"deflate">>)),
    ?assertEqual(<<"gzip">>, negotiate_encoding(<<"deflate, gzip">>)).
