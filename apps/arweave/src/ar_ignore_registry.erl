%%% @doc The module offers an interface to the "ignore registry" -
%%% an in-memory storage used for avoiding redundant processing of
%%% blocks and transactions, in the setting of historically synchronous
%%% POST /block and POST /tx requests. An incoming block or transaction is
%%% temporary placed in the registry. Requests with the same identifiers
%%% are ignored for the time. After a block or a transaction is validated
%%% a permanent record can be inserted into the registry.
%%% @end
-module(ar_ignore_registry).

-export([add/1, add_ref/2, remove/1, remove_ref/2,
		add_temporary/2, remove_temporary/1, member/1,
		permanent_member/1]).

%% @doc Put a permanent ID record into the registry.
add(ID) ->
	ets:insert(ignored_ids, {ID, permanent}).

%% @doc Remove a permanent ID record from the registry.
remove(ID) ->
	catch ets:delete_object(ignored_ids, {ID, permanent}).

%% @doc Put a referenced ID record into the registry.
%% The record may be removed by ar_ignore_registry:remove_ref/2.
add_ref(ID, Ref) when is_reference(Ref) ->
	ets:insert(ignored_ids, {ID, {ref, Ref}}).

%% @doc Remove a referenced ID record from the registry.
remove_ref(ID, Ref) when is_reference(Ref) ->
	catch ets:delete_object(ignored_ids, {ID, {ref, Ref}}).

%% @doc Put a temporary ID record into the registry.
%% The record expires after Timeout milliseconds.
add_temporary(ID, Timeout) ->
	ets:insert(ignored_ids, {ID, temporary}),
	timer:apply_after(Timeout, ar_ignore_registry, remove_temporary, [ID]).

%% @doc Remove the temporary record from the registry.
remove_temporary(ID) ->
	catch ets:delete_object(ignored_ids, {ID, temporary}).

%% @doc Check if there is a temporary or a permanent record in the registry.
member(ID) ->
	case ets:lookup(ignored_ids, ID) of
		[] ->
			false;
		_ ->
			true
	end.

%% @doc Check if there is a permanent record in the registry.
permanent_member(ID) ->
	Entries = ets:lookup(ignored_ids, ID),
	lists:member({ID, permanent}, Entries).