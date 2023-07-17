-module(ar_http_req).

-include_lib("arweave/include/ar.hrl").

-export([body/2, read_body_chunk/3, body_read_time/1]).

-define(AR_HTTP_REQ_BODY, '_ar_http_req_body').
-define(AR_HTTP_REQ_BODY_READ_TIME, '_ar_http_req_body_read_time').

body(Req, SizeLimit) ->
	case maps:get(?AR_HTTP_REQ_BODY, Req, not_set) of
		not_set ->
			Opts = #{
				acc => [],
				counter => 0,
				size_limit => SizeLimit,
				start_time => erlang:monotonic_time() },
			read_complete_body(Req, Opts);
		Body ->
			{ok, Body, Req}
	end.

%% @doc The elapsed time (in native units) to read the request body via `read_complete_body()`
body_read_time(Req) ->
	maps:get(?AR_HTTP_REQ_BODY_READ_TIME, Req, undefined).

read_body_chunk(Req, Size, Timeout) ->
	case cowboy_req:read_body(Req, #{ length => Size, period => Timeout }) of
		{_, Chunk, Req2} when byte_size(Chunk) >= Size ->
			prometheus_counter:inc(http_server_accepted_bytes_total,
					[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req2 })], Size),
			{ok, Chunk, Req2};
		{_, Chunk, Req2} ->
			prometheus_counter:inc(http_server_accepted_bytes_total,
					[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req2 })],
					byte_size(Chunk)),
			exit(timeout)
	end.

read_complete_body(Req, #{ acc := Acc, counter := C } = Opts) ->
	{MoreOrOk, Data, ReadReq} = cowboy_req:read_body(Req),
	DataSize = byte_size(Data),
	prometheus_counter:inc(
		http_server_accepted_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		DataSize
	),
	read_complete_body(MoreOrOk, Opts#{ acc := [Acc | Data],  counter := C + DataSize }, ReadReq).

read_complete_body(_, #{ counter := C, size_limit := SizeLimit }, _) when C > SizeLimit ->
	{error, body_size_too_large};
read_complete_body(more, Data, Req) ->
	read_complete_body(Req, Data);
read_complete_body(ok, #{ acc := Acc, start_time := StartTime }, Req) ->
	Body = iolist_to_binary(Acc),
	BodyReadTime = erlang:monotonic_time() - StartTime,
	{ok, Body, with_body_req_fields(Req, Body, BodyReadTime)}.

with_body_req_fields(Req, Body, BodyReadTime) ->
	Req#{
		?AR_HTTP_REQ_BODY => Body,
		?AR_HTTP_REQ_BODY_READ_TIME => BodyReadTime }.
