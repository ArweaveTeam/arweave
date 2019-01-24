-module(app_ipfs_daemon_server).
-export([handle/3]).

handle(Method, Path, Req) ->
	_QueryJson = elli_request:body(Req),
	Status = 200,
	Headers = [],
	RespBody = <<"OK">>,
	{Status, Headers, RespBody}.
