-module(ar_ipfs).
-export([add_data/2, add_data/4, add_file/1, add_file/3]).
-export([dht_provide/1, dht_provide/3]).

-define(BOUNDARY, "------------qwerasdfzxcv").

add_data(Data, Filename) ->
	add_data("127.0.0.1", "5001", Data, Filename).

add_data(IP, Port, DataB, Filename) ->
    URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/add",
    Data = binary_to_list(DataB),
    Boundary = ?BOUNDARY,
    Body = format_multipart_formdata(Boundary, [{Filename, Data}]),
    ContentType = lists:concat(["multipart/form-data; boundary=", Boundary]),
    Headers = [{"Content-Length", integer_to_list(length(Body))}],
    request(post, {URL, Headers, ContentType, Body}).

add_file(Path) ->
	add_file("127.0.0.1", "5001", Path).

add_file(IP, Port, Path)->
    {ok, Data} = file:read_file(Path), 
	Filename = filename:basename(Path),
	add_data(IP, Port, Data, Filename).

dht_provide(Key) ->
	dht_provide("127.0.0.1", "5001", Key).
    
dht_provide(IP, PORT, Key) ->
  URL = "http://" ++ IP ++ ":" ++ PORT ++ "/api/v0/dht/provide?arg=" ++ Key,
  request(get, {URL, []}).

%%% private

format_multipart_formdata(Boundary,  Files) ->
	FileParts = lists:append(lists:map(
					fun({FileName, FileContent}) ->
							[lists:concat(["--", Boundary]),
							lists:concat(["Content-Disposition: file; name=\"","path","\"; filename=\"",FileName,"\""]),
							lists:concat(["Content-Type: ", "application/octet-stream"]),
							"",
							FileContent]
					end,
					Files)),
	Suffix = [lists:concat(["--", Boundary, "--"]), ""],
	Parts = lists:append([FileParts, Suffix]),
	string:join(Parts, "\r\n").

request(Method, Request) ->
    R = httpc:request(Method, Request, [{ssl,[{verify,0}]}], []),
    response(R).

response(Response) ->
	case Response of
		{ok, {_, _, Body}} ->
			B1 = list_to_binary(Body),
			io:format(B1),
			{ok, jiffy:decode([B1])};
		_ -> {error, reasons_not_yet_implemented}
	end.
