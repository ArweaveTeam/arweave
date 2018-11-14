-module(ar_tx_db).
-export([start/0, get/1, put/2, maybe_add/1, remove/1]).
-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Defines a small in-memory metadata table for Archain nodes.
%%% Typically used to store small peices of globally useful information
%%% (for example: the port number used by the node).

%% @doc Initialise the metadata storage service.
start() ->
	spawn(
		fun() ->
			ar:report([starting_tx_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.

%% @doc Put an Erlang term into the meta DB. Typically these are
%% write-once values.
put(Key, Val) ->
    ets:insert(?MODULE, {Key, Val}),
    timer:apply_after(1800*1000, ?MODULE, remove, [Key]).
%% @doc Retreive a term from the meta db.
get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> Obj;
		[] -> not_found
	end.

maybe_add(Key) ->
    case ets:lookup(?MODULE, Key) of
        [{Key, Value}] -> ok;
        _ -> put(Key, ["unknown_error "])
    end.

remove(Key) ->
    ets:delete(?MODULE, Key).

tx_db_test() ->
  	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	OrphanedTX = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	TX = OrphanedTX#tx { owner = Pub1 , signature = <<"BAD">>},
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	ar_tx:verify(TX, 8, B0#block.wallet_list),
	timer:sleep(500),
    ["tx_too_cheap ","tx_fields_too_large ","tag_field_illegally_specified ","last_tx_not_valid "] = get(TX#tx.id),
    ar_tx:verify(SignedTX, 8, B0#block.wallet_list).
