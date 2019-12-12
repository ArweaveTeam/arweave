-module(ar_http_body_middleware).
-behavior(cowboy_middleware).
-include("ar.hrl").
-include("ar_http_req.hrl").
-export([execute/2]).

%%%===================================================================
%%% Cowboy middleware callback.
%%%===================================================================

execute(Req, Env) ->
	case read_complete_body(Req) of
		{ok, Body, ReadReq} -> {ok, with_body_req_field(ReadReq, Body), Env};
		{error, _, BadReq} -> {stop, cowboy_req:reply(413, #{}, <<"Payload too large">>, BadReq)}
	end.

read_complete_body(Req) ->
	do_read_complete_body(Req, <<>>).

do_read_complete_body(Req, Acc) ->
	{MoreOrOk, Data, ReadReq} = cowboy_req:read_body(Req),
	prometheus_counter:inc(
		http_server_accepted_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		byte_size(Data)
	),
	NewAcc = <<Acc/binary, Data/binary>>,
	do_read_complete_body(MoreOrOk, NewAcc, ReadReq).

do_read_complete_body(_, Data, Req) when byte_size(Data) > ?MAX_BODY_SIZE ->
	{error, body_size_too_large, Req};
do_read_complete_body(more, Data, Req) ->
	do_read_complete_body(Req, Data);
do_read_complete_body(ok, Data, Req) ->
	{ok, Data, Req}.

with_body_req_field(Req, Body) ->
	Req#{ ?AR_HTTP_REQ_BODY => Body }.
