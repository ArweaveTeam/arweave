%% @doc Elli middleware for collecting stats via Prometheus.
%% @author Eric Bailey
%% @author Ilya Khaprov
%% @version 0.1.1
%% @reference <a href="https://prometheus.io">Prometheus</a>
%% @copyright 2016 elli-lib team
-module(ar_metrics).
-author("Eric Bailey").
-author("Ilya Khaprov").

-behaviour(elli_handler).

%% elli_handler callbacks
-export([handle/2, handle_event/3]).

-include("ar.hrl").

%% Service Metric
-define(UP, elli_up).

%% Metrics for successful requests
-define(TOTAL, http_requests_total).
-define(REQUEST_DURATION, http_request_duration_microseconds).
-define(REQUEST_HEADERS_DURATION, http_request_headers_microseconds).
-define(REQUEST_BODY_DURATION, http_request_body_microseconds).
-define(REQUEST_USER_DURATION, http_request_user_microseconds).
-define(RESPONSE_SEND_DURATION, http_response_send_microseconds).

-define(RESPONSE_SIZE, http_response_size_bytes).
-define(RESPONSE_HEADERS_SIZE, http_response_headers_size_bytes).
-define(RESPONSE_BODY_SIZE, http_response_body_size_bytes).

%% Metrics for failed requests
-define(FAILED_TOTAL, http_requests_failed_total).
-define(BAD_REQUEST_TOTAL, http_bad_requests_total).
-define(CLIENT_CLOSED_TOTAL, http_client_closed_total).
-define(CLIENT_TIMEOUT_TOTAL, http_client_timeout_total).

-define(SCRAPE_DURATION, telemetry_scrape_duration_seconds).
-define(SCRAPE_SIZE, telemetry_scrape_size_bytes).
-define(SCRAPE_ENCODED_SIZE, telemetry_scrape_encoded_size_bytes).
%elli_prometheus_config
-define(DEFAULT_PATH, <<"/metrics">>).
-define(DEFAULT_FORMAT, auto).
-define(DEFAULT_DURATION_BUCKETS, [10, 100, 1000, 10000, 100000, 300000, 500000,
                                   750000, 1000000, 1500000, 2000000, 3000000]).
-define(DEFAULT_LABELS, [method, path, status_class, handler]).
-define(DEFAULT_CONFIG, [{path, ?DEFAULT_PATH},
                         {format, ?DEFAULT_FORMAT},
                         {duration_buckets, ?DEFAULT_DURATION_BUCKETS},
                         {labels, ?DEFAULT_LABELS}]).
%%%===================================================================
%%% Config
%%%===================================================================
path() -> get_value(path, ?DEFAULT_PATH).

format() -> get_value(format, ?DEFAULT_FORMAT).

allowed_formats() ->
  [{prometheus_text_format:content_type(), prometheus_text_format},
   {prometheus_protobuf_format:content_type(), prometheus_protobuf_format}].

duration_buckets() -> get_value(duration_buckets, ?DEFAULT_DURATION_BUCKETS).

labels() -> get_value(labels, ?DEFAULT_LABELS).

%%%===================================================================
%%% Private functions
%%%===================================================================

get_value(Key, Default) -> proplists:get_value(Key, config(), Default).

config() -> application:get_env(prometheus, elli_exporter, ?DEFAULT_CONFIG).
%%%===================================================================
%%% elli_handler callbacks
%%%===================================================================

%% @doc Handle requests to `/metrics' and ignore all others.
%% TODO: Describe format.
%% TODO: Add links to Prometheus and Prometheus.erl docs.
handle(Req, _Config) ->
  case ar_meta_db:get(metrics) of
	  false -> ignore;
	  _ ->
		  Path = path(),
		  case {elli_request:method(Req), elli_request:raw_path(Req)} of
			{'GET', Path} -> format_metrics(Req);
			_             -> ignore
		  end
  end.

handle_event(request_complete, Args, Config) ->
  handle_full_response(request_complete, Args, Config);
handle_event(chunk_complete, Args, Config) ->
  handle_full_response(chunk_complete, Args, Config);

handle_event(request_closed, _, _) ->
  count_failed_request(request_closed);
handle_event(request_timeout, _, _) ->
  count_failed_request(request_timeout);
handle_event(request_parse_error, _, _) ->
  count_failed_request(request_parse_error);
handle_event(client_closed, [RequestPart], _) ->
  prometheus_counter:inc(?CLIENT_CLOSED_TOTAL, [RequestPart]),
  count_failed_request(client_closed);
handle_event(client_timeout, [RequestPart], _) ->
  prometheus_counter:inc(?CLIENT_TIMEOUT_TOTAL, [RequestPart]),
  count_failed_request(client_timeout);
handle_event(bad_request, [{Reason, _}], _) ->
  prometheus_counter:inc(?BAD_REQUEST_TOTAL, [Reason]),
  count_failed_request(bad_request);
handle_event(elli_startup, _Args, _Config) ->
  Labels        = labels(),
  Buckets       = duration_buckets(),
  UP            = [{name, ?UP},
                   {help, "Elli is up?"}],
  RequestCount  = metric(?TOTAL, Labels, "request count"),
  RequestDuration = metric(?REQUEST_DURATION, [response_type | Labels], Buckets,
                           " latencies in microseconds"),
  RequestHeadersDuration = metric(?REQUEST_HEADERS_DURATION,
                                  Labels, Buckets,
                                  "time spent receiving and parsing headers"),
  RequestBodyDuration = metric(?REQUEST_BODY_DURATION,
                               Labels, Buckets,
                               "time spent receiving and parsing body"),
  RequestUserDuration = metric(?REQUEST_USER_DURATION,
                               Labels, Buckets,
                               "time spent in user callback"),
  ResponseSendDuration = metric(?RESPONSE_SEND_DURATION,
                                [response_type | Labels], Buckets,
                                "time spent sending reply"),
  ResponseSize = metric(?RESPONSE_SIZE,
                        [response_type | Labels],
                        "total response size"),
  ResponseHeaders = metric(?RESPONSE_HEADERS_SIZE,
                           [response_type | Labels],
                           "response headers size"),
  ResponseBody = metric(?RESPONSE_BODY_SIZE,
                        [response_type | Labels],
                        "response body size"),
  prometheus_gauge:declare(UP),
  prometheus_gauge:set(?UP, 1),
  prometheus_counter:declare(RequestCount),
  prometheus_histogram:declare(RequestDuration),
  prometheus_histogram:declare(RequestHeadersDuration),
  prometheus_histogram:declare(RequestBodyDuration),
  prometheus_histogram:declare(RequestUserDuration),

  prometheus_histogram:declare(ResponseSendDuration),
  prometheus_summary:declare(ResponseSize),
  prometheus_summary:declare(ResponseHeaders),
  prometheus_summary:declare(ResponseBody),

  FailedRequestCount = metric(?FAILED_TOTAL,
                              [reason], [],
                              "failed total count."),
  BadRequestTotal = metric(?BAD_REQUEST_TOTAL,
                           [reason], [],
                           "\"bad_request\" errors count"),
  ClientClosedTotal = metric(?CLIENT_CLOSED_TOTAL,
                             [request_part], [],
                             "\"client_closed\" errors count"),
  ClientTimeoutTotal = metric(?CLIENT_TIMEOUT_TOTAL,
                              [request_part], [],
                              "\"client_timeout\" errors count"),
  prometheus_counter:declare(FailedRequestCount),
  prometheus_counter:declare(BadRequestTotal),
  prometheus_counter:declare(ClientClosedTotal),
  prometheus_counter:declare(ClientTimeoutTotal),

  Registry = default,

  ScrapeDuration = [{name, ?SCRAPE_DURATION},
                    {help, "Scrape duration"},
                    {labels, ["registry", "content_type"]},
                    {registry, Registry}],
  ScrapeSize = [{name, ?SCRAPE_SIZE},
                {help, "Scrape size, not encoded"},
                {labels, ["registry", "content_type"]},
                {registry, Registry}],
  ScrapeEncodedSize = [{name, ?SCRAPE_ENCODED_SIZE},
                       {help, "Scrape size, encoded"},
                       {labels, ["registry", "content_type", "encoding"]},
                       {registry, Registry}],
  prometheus_summary:declare(ScrapeDuration),
  prometheus_summary:declare(ScrapeSize),
  prometheus_summary:declare(ScrapeEncodedSize),
  ok;
handle_event(_Event, _Args, _Config) -> ok.

%%%===================================================================
%%% Private functions
%%%===================================================================

handle_full_response(Type, [Req, Code, _Hs, _B, {Timings, Sizes}], _Config) ->
%%handle_full_response(Type, [Req, Code, _Hs, _B, Timings], _Config) ->
	Path = path(),
	case {elli_request:method(Req), elli_request:raw_path(Req)} of
		{'GET', Path} -> ok;
		_ ->
			Labels = labels(Req, Code),
			TypedLabels = case Type of
							request_complete -> ["full" | Labels];
							chunk_complete   -> ["chunks" | Labels] %;
							%% _ -> Labels
						end,
			prometheus_counter:inc(?TOTAL, Labels),

			ReqTime = duration(Timings, request),
			ReqSize = size(Sizes, response),

			prometheus_histogram:observe(
				?REQUEST_DURATION, TypedLabels, ReqTime),
			prometheus_histogram:observe(
				?REQUEST_HEADERS_DURATION, Labels, duration(Timings, headers)),
			prometheus_histogram:observe(
				?REQUEST_BODY_DURATION, Labels, duration(Timings, body)),
			prometheus_histogram:observe(
				?REQUEST_USER_DURATION, Labels, duration(Timings, user)),
			prometheus_histogram:observe(
				?RESPONSE_SEND_DURATION, TypedLabels, duration(Timings, send)),
			prometheus_summary:observe(
				?RESPONSE_SIZE, TypedLabels, ReqSize),
			prometheus_summary:observe(
				?RESPONSE_HEADERS_SIZE, TypedLabels, size(Sizes, response_headers)),
			prometheus_summary:observe(
				?RESPONSE_BODY_SIZE, TypedLabels, size(Sizes, response_body)),

			Peer = ar_http_iface_server:elli_request_to_peer(Req),
			P = case ar_meta_db:get({peer, Peer}) of
				not_found -> #performance{};
				X -> X
			end,
			ar_meta_db:put({peer, Peer},
				P#performance {
				transfers = P#performance.transfers + 1,
				time = P#performance.time + ReqTime,
				bytes = P#performance.bytes + ReqSize,
				timeout = os:system_time(seconds)
				}),
			ok
	end.

count_failed_request(Reason) ->
  prometheus_counter:inc(?FAILED_TOTAL, [Reason]).

format_metrics(Req) ->
  case negotiate_format(Req) of
    undefined ->
      throw({406, [], <<>>});

    Format ->
      {ContentType, Scrape} = render_format(Format),
      case negotiate_encoding(Req) of
        undefined ->
          throw({406, [], <<>>});
        Encoding ->
          encode_format(ContentType, Encoding, Scrape)
      end
  end.

negotiate_format(Req) ->
  case format() of
    auto ->
      Accept = elli_request:get_header(<<"Accept">>, Req, "text/plain"),
      Alternatives = allowed_formats(),
      accept_header:negotiate(Accept, Alternatives);
    undefined -> undefined;
    Format0 -> Format0
  end.

negotiate_encoding(Req) ->
  AcceptEncoding = elli_request:get_header(
                     <<"Accept-Encoding">>, Req, ""),
  accept_encoding_header:negotiate(AcceptEncoding, [<<"gzip">>,
                                                    <<"deflate">>,
                                                    <<"identity">>]).

render_format(Format) ->
  Registry = default,
  ContentType = Format:content_type(),

  Scrape = prometheus_summary:observe_duration(
             Registry,
             ?SCRAPE_DURATION,
             [Registry, ContentType],
             fun () -> Format:format(Registry) end),
  prometheus_summary:observe(Registry,
                             ?SCRAPE_SIZE,
                             [Registry, ContentType],
                             iolist_size(Scrape)),
  {ContentType, Scrape}.

encode_format(ContentType, Encoding, Scrape) ->
  Encoded = encode_format_(Encoding, Scrape),
  Registry = default,
  prometheus_summary:observe(Registry,
                             ?SCRAPE_ENCODED_SIZE,
                             [Registry, ContentType, Encoding],
                             iolist_size(Encoded)),
  {ok, [{<<"Content-Type">>, ContentType},
        {<<"Content-Encoding">>, Encoding}], Encoded}.

encode_format_(<<"gzip">>, Scrape) ->
  zlib:gzip(Scrape);
encode_format_(<<"deflate">>, Scrape) ->
  ZStream = zlib:open(),
  zlib:deflateInit(ZStream),
  try
    zlib:deflate(ZStream, Scrape, finish)
  after
    zlib:deflateEnd(ZStream)
  end;
encode_format_(<<"identity">>, Scrape) ->
  Scrape.

duration(Timings, request) ->
  duration(request_start, request_end, Timings);
duration(Timings, headers) ->
  duration(headers_start, headers_end, Timings);
duration(Timings, body) ->
  duration(body_start, body_end, Timings);
duration(Timings, user) ->
  duration(user_start, user_end, Timings);
duration(Timings, send) ->
  duration(send_start, send_end, Timings).

duration(StartKey, EndKey, Timings) ->
  Start = proplists:get_value(StartKey, Timings),
  End   = proplists:get_value(EndKey, Timings),
  End - Start.

size(Sizes, response) ->
	size_safe_add(size(Sizes, response_headers), size(Sizes, response_body));
size(Sizes, response_headers) ->
	proplists:get_value(resp_headers, Sizes);
size(Sizes, response_body) ->
	case proplists:get_value(chunks, Sizes) of
		undefined ->
			case proplists:get_value(file, Sizes) of
				undefined ->
					proplists:get_value(resp_body, Sizes);
				FileSize -> FileSize
			end;
		ChunksSize -> ChunksSize
	end.

size_safe_add(undefined, undefined) -> 0;
size_safe_add(undefined, Y) -> Y;
size_safe_add(X, undefined) -> X;
size_safe_add(X, Y) -> X + Y.

metric(Name, Labels, Desc) -> metric(Name, Labels, [], Desc).

metric(Name, Labels, Buckets, Desc) ->
  [{name, Name},
   {labels, Labels},
   {help, "HTTP request " ++ Desc},
   {buckets, Buckets}].

labels(Req, StatusCode) ->
  Labels = labels(),
  [label(Label, Req, StatusCode) || Label <- Labels].

label(handler, _, _) -> "server";
label(method,  Req, _) -> elli_request:method(Req);
label(path, Req, _) ->
  case elli_request:path(Req) of
    [H|_] -> H;
    []    -> ""
  end;
label(status_code,  _, StatusCode) -> StatusCode;
label(status_class, _, StatusCode) -> prometheus_http:status_class(StatusCode).

%% request_start
%% headers_start
%% headers_end
%% body_start
%% body_end
%% user_start
%% user_end
%% send_start
%% send_end
%% request_end


%% resp_headers
%% resp_body
%% file
%% chunk

%% exclusive event
%% `request_closed' is sent if the client closes the connection when
%% Elli is waiting for the next request on a keep alive connection.
%%
%% `request_timeout' is sent if the client times out when
%% Elli is waiting for the request.
%%
%% `request_parse_error' fires if the request is invalid and cannot be parsed by
%% [`erlang:decode_packet/3`][decode_packet/3] or it contains a path Elli cannot
%% parse or does not support.
%% `client_closed' can be sent from multiple parts of the request
%% handling. It's sent when the client closes the connection or if for
%% any reason the socket is closed unexpectedly. The `Where' atom
%% tells you in which part of the request processing the closed socket
%% was detected: `receiving_headers', `receiving_body' or `before_response'.
%%
%% `client_timeout' can as with `client_closed' be sent from multiple
%% parts of the request handling. If Elli tries to receive data from
%% the client socket and does not receive anything within a timeout,
%% this event fires and the socket is closed.
%%
%% `bad_request' is sent when Elli detects a request is not well
%% formatted or does not conform to the configured limits. Currently
%% the `Reason' variable can be `{too_many_headers, Headers}'
%% or `{body_size, ContentLength}'.
%% @doc elli_prometheus config helpers.
%% @author Ilya Khaprov
%% @version 0.1.1
%% @see elli_prometheus
%% @copyright 2016 elli-lib team
%% Macros.
