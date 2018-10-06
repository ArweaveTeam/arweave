-module(app_internet_archive).
-export([store/2]).
-export([id_to_item/1]).
-export([generate_items/0, generate_items/1, generate_items/2]).
-include("ar.hrl").

%%% An application that archives hashes and (optionally) torrents from the
%%% Internet Archive, allowing for
%%% A). Verifiability and trustless timestamping of data stored in the archive.
%%% B). Where torrent are available, fully decentralised access to the IA.

%% Represents an item from the IA to be stored inside the Arweave.
-record(item, {
    id,
    meta_data = [],
    torrent = <<>>
}).

%% @doc Start an archiving node. Takes a list of Items to archive and the
%% location of a wallet.
store(Wallet, Items) ->
    store(whereis(http_entrypoint_node), Wallet, Items).
store(Node, WalletLoc, Items) when not is_tuple(WalletLoc) ->
    store(Node, ar_wallet:load_keyfile(WalletLoc), Items);
store(Node, Wallet, Items) ->
    ssl:start(),
    Queue = app_queue:start(Node, Wallet),
    ar:report([{storing_ia_items, length(Items)}]),
    lists:foreach(
        fun(I) -> app_queue:add(Queue, item_to_tx(I)) end,
        Items
    ),
    Queue.

%% @doc Generate an Arweave transaction for an IA item.
item_to_tx(I) ->
    #tx {
        tags =
            [
                {"app_name", "InternetArchive"},
                {"id", I#item.id},
                {"Content-Type", "application/x-bittorrent"}
            ] ++ I#item.meta_data,
        data = I#item.torrent
    }.

%% @doc When given an ID, build an item from it using the API.
id_to_item(ItemID)  when is_binary(ItemID) -> id_to_item(binary_to_list(ItemID));
id_to_item(ItemID) ->
    json_to_item(
        ar_serialize:dejsonify(get_url("https://archive.org/metadata/" ++ ItemID))
    ).

%% @doc Parse a JSONStruct into an #item{}.
json_to_item({JSON}) ->
    {Metadata} = proplists:get_value(<<"metadata">>, JSON),
    Files = proplists:get_value(<<"files">>, JSON),
    ID = proplists:get_value(<<"identifier">>, Metadata),
    [Torrent|_] = lists:filtermap(fun is_torrent_file/1, Files),
    #item {
        id = ID,
        meta_data = Metadata,
        torrent =
            list_to_binary(
                get_url(
                    "https://archive.org/download/"
                        ++ binary_to_list(ID)
                        ++ "/"
                        ++ binary_to_list(Torrent)
                )
            )
    }.

%% @doc Determine whether the given JSONStruct representation of a file contains
%% a torrent file. If it does, return the file name. If it does not, return false.
is_torrent_file({File}) ->
    case re:run(Name = proplists:get_value(<<"name">>, File), ".+\.torrent$") of
        nomatch -> false;
        _ -> {true, Name}
    end.

%% @doc Get a page, assuming 200 response (crashing if not).
get_url(URL) ->
    ar:report([{getting_url, URL}]),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(URL),
    Body.

%% @doc Generate a list of items to store in the Arweave.
%% For the moment, this is static test data, until we have a way to dynamically
%% extract it.
%% TODO: Generate this list from an index.
generate_items() -> generate_items(1000).
generate_items(Count) -> generate_items(Count, <<>>).
generate_items(Count, Cursor) ->
    {JSON} = 
        ar_serialize:dejsonify(
            get_url(
                "https://archive.org/services/search/v1/scrape?q=bittorrent"
                    ++ "&sorts=downloads"
                    ++ "&count=" ++ integer_to_list(Count)
                    ++ if Cursor == <<>> -> ""; true -> "&cursor=" ++ binary_to_list(Cursor) end
            )
        ),
    {
        proplists:get_value(<<"cursor">>, JSON),
        lists:filtermap(
            fun({[{<<"identifier">>, ID}]}) ->
                try {true, id_to_item(ID)} catch _:_ -> false end
            end,
            ar:d(proplists:get_value(<<"items">>, JSON))
        )
    }.