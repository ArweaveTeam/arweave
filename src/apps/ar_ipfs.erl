-module(ar_ipfs).
-export([daemon_start/0, daemon_stop/0, daemon_stop/2]).
-export([add_data/2, add_data/4, add_file/1, add_file/3]).
-export([cat_data_by_hash/1, cat_data_by_hash/3]).
-export([config_set_identity/1]).
-export([key_gen/1, key_gen/3]).
-export([pin_ls/0, pin_ls/2]).
-export([ep_get_ipfs_hashes/2, hashes_only/1]).

-define(BOUNDARY, "------------qwerasdfzxcv").
-define(IPFS_HOST, "127.0.0.1").
-define(IPFS_PORT, "5001").

daemon_start() ->
	Pid = spawn(os, cmd, ["ipfs daemon"]),
	{ok, Pid}.

daemon_stop() ->
	daemon_stop(?IPFS_HOST, ?IPFS_PORT).

daemon_stop(IP, Port) ->
    URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/shutdown",
    {ok, _} = request(post, {URL, [], "", ""}).

ep_get_ipfs_hashes(N, From) ->
	{ok, _} = application:ensure_all_started(ssl),
	URL = "https://mainnet.libertyblock.io:7777/v1/chain/get_table_rows",
	Headers = [],
	ContentType = [],
	ReqProps = [
		{scope, <<"eparticlectr">>},
		{code, <<"eparticlectr">>},
		{table, <<"wikistbl">>},
		{json, true},
		{lower_bound, From},
		{limit, N}
	],
	Body = jiffy:encode({ReqProps}),
	{ok, Response} = request(post, {URL, Headers, ContentType, Body}),
	{RespProps} = response_to_json(Response),
	MaybeMore = case lists:keyfind(<<"more">>, 1, RespProps) of
		{<<"more">>, More} -> More;
		false              -> false
	end,
	HashTups = case lists:keyfind(<<"rows">>, 1, RespProps) of
		false              -> [];
		{<<"rows">>, Rows} -> lists:map(fun row_to_hash_tup/1, Rows)
	end,
	{HashTups, MaybeMore}.

hashes_only(HashTups) ->
	lists:flatten(lists:map(fun
		({_,H,<<>>}) -> H;
		({_,H,P})    -> [H,P]
	end, HashTups)).

row_to_hash_tup({Props}) ->
	case lists:sort(Props) of
		[{<<"hash">>, Hash},{<<"id">>, Id},{<<"parent_hash">>, PHash}] ->
			{Id, Hash, PHash};
		_ ->
			{none, <<>>, <<>>}
	end.

add_data(Data, Filename) ->
	add_data(?IPFS_HOST, ?IPFS_PORT, Data, Filename).

add_data(IP, Port, DataB, FilenameB) ->
    URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/add?pin=true",
    Data = binary_to_list(DataB),
	Filename = thing_to_list(FilenameB),
    Boundary = ?BOUNDARY,
    Body = format_multipart_formdata(Boundary, [{Filename, Data}]),
    ContentType = lists:concat(["multipart/form-data; boundary=", Boundary]),
    Headers = [{"Content-Length", integer_to_list(length(Body))}],
    {ok, Response} = request(post, {URL, Headers, ContentType, Body}),
	{Props} = response_to_json(Response),
	{<<"Hash">>, Hash} = lists:keyfind(<<"Hash">>, 1, Props),
	{ok, Hash}.

add_file(Path) ->
	add_file(?IPFS_HOST, ?IPFS_PORT, Path).

add_file(IP, Port, Path)->
    {ok, Data} = file:read_file(Path),
	Filename = filename:basename(Path),
	add_data(IP, Port, Data, Filename).

cat_data_by_hash(Hash) ->
	cat_data_by_hash(?IPFS_HOST, ?IPFS_PORT, Hash).

cat_data_by_hash(IP, Port, Hash) ->
	URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/cat?arg=" ++ binary_to_list(Hash),
	{ok, _Data} = request(get, {URL, []}).

config_set_identity(Key) ->
	os:cmd("ipfs config Identity.PeerID " ++ thing_to_list(Key)).

key_gen(Name) ->
	key_gen(?IPFS_HOST, ?IPFS_PORT, Name).

key_gen(IP, Port, Name) ->
	URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/key/gen?type=rsa&arg=" ++ Name,
	{ok, Response} = request(get, {URL, []}),
	{Props} = response_to_json(Response),
	case lists:keyfind(<<"Id">>, 1, Props) of
		{<<"Id">>, Key} -> {ok, Key};
		false           -> {error, Props}
	end.

pin_ls() ->
	pin_ls(?IPFS_HOST, ?IPFS_PORT).

pin_ls(IP, Port) ->
    URL = "http://" ++ IP ++ ":" ++ Port ++ "/api/v0/pin/ls",
    {ok, Response} = request(post, {URL, [], [], ""}),
	{[{<<"Keys">>, {Props}}]} = response_to_json(Response),
	Hashes = [K || {K, _} <- Props],
	Hashes.


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
    Response = httpc:request(Method, Request, [{ssl,[{verify,0}]}], []),
	case Response of
		{ok, {_, _, Body}} ->
			{ok, list_to_binary(Body)};
		_Error ->
			%% example errors:
			%% {error,{failed_connect,[{to_address,{"127.0.0.1",5001}},
			%%                         {inet,[inet],econnrefused}]}}
			%% io:format("httpc:request error:~n~n~p",[Error]),
			{error, reasons_not_yet_implemented}
	end.

response_to_json(Response) ->
	jiffy:decode(Response).

thing_to_list(X) when is_list(X) -> X;
thing_to_list(X) when is_binary(X) -> binary_to_list(X).
