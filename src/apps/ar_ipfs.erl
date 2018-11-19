%%% @doc ipfs module
%%% @private
%%% @end
%%%
%%% Copyright (c) 2017, Hendry Rodriguez
%%%
%%% The MIT License
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in
%%% all copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%%% THE SOFTWARE.
%%%
%%%---------------------------------------------------------------------------------------
-module(ar_ipfs).
-compile([export_all]).
-define(BOUNDARY, "------------e897glvjfEoq").


add(Path, Filename) ->
	add("127.0.0.1", "5001", Path, Filename).

add(IP, PORT, PathFile, FileName)->
    URL = "http://" ++ IP ++ ":" ++ PORT ++ "/api/v0/add",
    {ok, Cont} = file:read_file(PathFile), 
    Data = binary_to_list(Cont),
    Boundary = ?BOUNDARY,
    Body = format_multipart_formdata(Boundary, [{ FileName, Data}]),
    ContentType = lists:concat(["multipart/form-data; boundary=", Boundary]),
    Headers = [{"Content-Length", integer_to_list(length(Body))}],
    request(post, {URL, Headers, ContentType, Body}).

dht_provide(Key) ->
	dht_provide("127.0.0.1", "5001", Key).
    
dht_provide(IP, PORT, Key) ->
  URL = "http://" ++ IP ++ ":" ++ PORT ++ "/api/v0/dht/provide?arg=" ++ Key,
  request(get, {URL, []}).

%%% private

format_multipart_formdata(Boundary,  Files) ->
    FileParts = lists:map(fun({FileName, FileContent}) ->
                                  [lists:concat(["--", Boundary]),
                                   lists:concat(["Content-Disposition: file; name=\"","path","\"; filename=\"",FileName,"\""]),
                                   lists:concat(["Content-Type: ", "application/octet-stream"]),
                                   "",
                                   FileContent]
                          end, Files),
    FileParts2 = lists:append(FileParts),
    EndingParts = [lists:concat(["--", Boundary, "--"]), ""],
    Parts = lists:append([ FileParts2, EndingParts]),
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
