-module(ar_http_req).

-include("ar.hrl").
-include("ar_http_req.hrl").

-export([body/1, read_body_chunk/3]).

body(Req) ->
	case maps:get(?AR_HTTP_REQ_BODY, Req, not_set) of
		not_set ->
			read_complete_body(Req, <<>>);
		Body ->
			{ok, Body, Req}
	end.

read_body_chunk(Req, Size, Timeout) ->
	Reply = cowboy_req:read_body(Req, #{ length => Size, period => Timeout }),
	prometheus_counter:inc(
		http_server_accepted_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		Size
	),
	Reply.

read_complete_body(Req, Acc) ->
	{MoreOrOk, Data, ReadReq} = cowboy_req:read_body(Req),
	prometheus_counter:inc(
		http_server_accepted_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		byte_size(Data)
	),
	NewAcc = <<Acc/binary, Data/binary>>,
	read_complete_body(MoreOrOk, NewAcc, ReadReq).

read_complete_body(_, Data, _Req) when byte_size(Data) > ?MAX_BODY_SIZE ->
	{error, body_size_too_large};
read_complete_body(more, Data, Req) ->
	read_complete_body(Req, Data);
read_complete_body(ok, Data, Req) ->
	{ok, Data, with_body_req_field(Req, Data)}.

with_body_req_field(Req, Body) ->
	Req#{ ?AR_HTTP_REQ_BODY => Body }.
