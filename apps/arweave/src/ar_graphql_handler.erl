-module(ar_graphql_handler).
-behaviour(cowboy_handler).
-include("ar.hrl").

-define(PREFLIGHT_RESPONSE_HEADERS, #{
	<<"access-control-allow-methods">> => <<"GET, POST">>,
	<<"access-control-allow-headers">> => <<"Content-Type">>
}).

%% Cowboy Handler Interface
-export([init/2]).

init(Req, State) ->
	case cowboy_req:method(Req) of
		<<"OPTIONS">> ->
			handle_preflight_request(Req, State);
		<<"GET">> ->
			handle_graphql_request(Req, State);
		<<"POST">> ->
			handle_graphql_request(Req, State)
	end.

handle_preflight_request(Req, State) ->
	Req1 = cowboy_req:set_resp_headers(?CORS_HEADERS, Req),
	Req2 = cowboy_req:set_resp_headers(?PREFLIGHT_RESPONSE_HEADERS, Req1),
	Req3 = cowboy_req:set_resp_body(<<"OK">>, Req2),
	Reply = cowboy_req:reply(200, Req3),
	{ok, Reply, State}.

handle_graphql_request(Req, #{ arql_semaphore := Semaphore } = State) ->
	ar_semaphore:acquire(Semaphore, 5000),
	case gather_query_params(Req) of
		{error, body_size_too_large} ->
			err(413, <<"Payload too large">>, Req, State);
		{error, Reason} ->
			err(400, Reason, Req, State);
		{ok, Req2, Decoded} ->
			run_query(Decoded, Req2, State)
	end.

gather_query_params(Req) ->
	case ar_http_req:body(Req, ?MAX_BODY_SIZE) of
		{ok, Body, Req2} ->
			Params = maps:from_list(cowboy_req:parse_qs(Req)),
			case bin_to_json(Body) of
				{ok, JSON} ->
					gather_query_params(Req2, JSON, Params);
				error ->
					{error, invalid_json_body}
			end;
		{error, body_size_too_large} = Err ->
			Err
	end.

gather_query_params(Req, Body, Params) ->
	QueryDocument = document([Params, Body]),
	case variables([Params, Body]) of
		{ok, Vars} ->
			Operation = operation_name([Params, Body]),
			{ok, Req, #{
				document => QueryDocument,
				vars => Vars,
				operation_name => Operation
			}};
		{error, Reason} ->
			{error, Reason}
	end.

document([#{ <<"query">> := Q }|_]) -> Q;
document([_|Next]) -> document(Next);
document([]) -> undefined.

variables([#{ <<"variables">> := Vars } | _]) ->
	case Vars of
		BinVars when is_binary(BinVars) ->
			case bin_to_json(BinVars) of
				{ok, JSON} ->
					{ok, JSON};
				error ->
					{error, invalid_json}
			end;
		MapVars when is_map(MapVars) ->
			{ok, MapVars};
		null ->
			{ok, #{}}
	end;
variables([_ | Next]) ->
	variables(Next);
variables([]) ->
	{ok, #{}}.

operation_name([#{ <<"operationName">> := OpName } | _]) ->
	OpName;
operation_name([_ | Next]) ->
	operation_name(Next);
operation_name([]) ->
	undefined.

bin_to_json(<<>>) ->
	{ok, #{}};
bin_to_json(Bin) ->
	try
		{ok, jiffy:decode(Bin, [return_maps])}
	catch
		{error, _} -> error
	end.

run_query(#{ document := undefined }, Req, State) ->
	err(400, no_query_supplied, Req, State);
run_query(#{ document := Doc } = ReqCtx, Req, State) ->
	case graphql:parse(Doc) of
		{ok, AST} ->
			run_preprocess(ReqCtx#{ document := AST }, Req, State);
		{error, Reason} ->
			err(400, Reason, Req, State)
	end.

run_preprocess(#{ document := AST } = ReqCtx, Req, State) ->
	try
		{ok, #{ fun_env := FunEnv, ast := AST2 }} = graphql:type_check(AST),
		ok = graphql:validate(AST2),
		run_execute(ReqCtx#{ document := AST2, fun_env => FunEnv }, Req, State)
	catch
		throw:Err ->
			err(400, Err, Req, State)
	end.

run_execute(#{
	document := AST,
	fun_env := FunEnv,
	vars := Vars,
	operation_name := OpName
}, Req, State) ->
	Coerced = graphql:type_check_params(FunEnv, OpName, Vars),
	Ctx = #{ params => Coerced, operation_name => OpName },
	Response = graphql:execute(Ctx, AST),
	ResponseBody = jiffy:encode(Response),
	Req2 = cowboy_req:set_resp_headers(?CORS_HEADERS, Req),
	Req3 = cowboy_req:set_resp_body(ResponseBody, Req2),
	Reply = cowboy_req:reply(200, Req3),
	{ok, Reply, State}.

err(Code, Msg, Req, State) ->
	Formatted = iolist_to_binary(io_lib:format("~p", [Msg])),
	Err = #{ type => error, message => Formatted },
	Body = jiffy:encode(#{ errors => [Err] }),
	Req2 = cowboy_req:set_resp_headers(?CORS_HEADERS, Req),
	Req3 = cowboy_req:set_resp_body(Body, Req2),
	Reply = cowboy_req:reply(Code, Req3),
	{ok, Reply, State}.
