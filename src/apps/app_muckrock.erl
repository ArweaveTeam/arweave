-module(app_muckrock).
-export([start/1]).
-include("ar.hrl").

-define(CHECK_TIME, 60 * 5).

%%% Scrapes and uploads the latest responses received by the Muckrock FOIA
%%% collection group.

start(WalletFile) ->
	ssl:start(),
	Wallet = ar_wallet:load_keyfile(WalletFile),
	Queue = app_queue:start(Wallet),
	spawn(fun() -> server(Queue, []) end).

server(Queue, AlreadySeen) ->
	LatestItems = get_latest(),
	NewItems = LatestItems -- AlreadySeen,
	lists:foreach(
		fun(Item) ->
			try submit_item(Queue, Item)
			catch _:_ -> ok end
		end,
		NewItems
	),
	receive
		stop -> ok
	after ?CHECK_TIME * 1000 ->
		server(Queue, AlreadySeen ++ NewItems)
	end.

submit_item(Queue, Item) ->
	io:format("Submitting item ~s... ", [Item]),
	{ok, {{_, 200, _}, _, Body}} = httpc:request(Item),
	app_queue:add(Queue,
		#tx {
			tags =
				[
					{"app_name", "MuckRock"},
					{"Original-File-Location", Item},
					{"Content-Type", "application/pdf"}
				],
			data = list_to_binary(Body)
		}
	),
	io:format("Done!~n").

get_latest() ->
	{ok, {{_, 200, _}, _, Body}} =
		httpc:request("https://www.muckrock.com/api_v1/foia/?ordering=-id&status=done"),
	case re:run(Body, "([^ \"]*?\.pdf)", [global, {capture, all_but_first, list}]) of
		nomatch -> [];
		{match, Matches} -> lists:map(fun erlang:hd/1, Matches)
	end.
