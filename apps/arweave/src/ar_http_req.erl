-module(ar_http_req).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_http_req.hrl").

-export([body/2, read_body_chunk/3]).

body(Req, SizeLimit) ->
	case maps:get(?AR_HTTP_REQ_BODY, Req, not_set) of
		not_set ->
			read_complete_body(Req, #{ acc => [], counter => 0, size_limit => SizeLimit });
		Body ->
			{ok, Body, Req}
	end.

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
read_complete_body(ok, #{ acc := Acc }, Req) ->
	Body = iolist_to_binary(Acc),
	{ok, Body, with_body_req_field(Req, Body)}.

with_body_req_field(Req, Body) ->
	Req#{ ?AR_HTTP_REQ_BODY => Body }.
