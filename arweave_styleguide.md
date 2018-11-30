# Arweave code style

The main development language of the Arweave client is Erlang, and as the number of developers of the project continues to grow this stlye guide will act as a means of keeping the codebase clean and comprehensible. 



## Code comprehensibility



### Module header comments

Each module should have a simplistic comment at the top that encompasses and describes the set of functions that can be found within it.

Module description comments should be prixed with '%%%' .

```erlang
%% Example: head of ar_serialize.

-module(ar_serialize).
-export([full_block_to_json_struct/1, block_to_json_struct/1, ...]).
-export([tx_to_json_struct/1, json_struct_to_tx/1]).
-export([wallet_list_to_json_struct/1, hash_list_to_json_struct/1, ...]).
-export([jsonify/1, dejsonify/1]).
-export([query_to_json_struct/1, json_struct_to_query/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions for use in the HTTP server.
```



### Function clause comments


Function clause comments should be placed above the header.

Every function should have a comment describing its purpose, unless the function signature explains it well enough.

Function comments should not include implementation details unless absolutely required, the code itself should be the main conveyor of the specific implementation.

It is more important to comment exported functions.

A specification (`-spec`) may be used to document the function, as an alternative or an addition to the comment.

Function description comments should be prefixed with  '%% @doc'. 

```erlang
%% Example

%% @doc Takes a list containing tx records and returns the number of those 'not_found'.
count_unavailable_txs(TXList) ->
    length([TX || TX <- TXList, TX == not_found]).
```



### Sparing use of comments inside function bodies

Comments should only need to be used inside functions if the code being described has high complexity and without description would take reasonable time to trace and understand.

If the written code does have high complexity consider if descriptive variable names, code abstraction and or basic refactoring could improve the comprehensibility before resorting to commenting the code block.

```erlang
%% Bad
sign_verify_test(Keypair) ->
	% Deconstructs keypair into two separate terms, Pub and Priv.
	{Priv, Pub} = Keypair,
	% Generates an integer between 1 and 100 to sign and then verify.
	Data = floor(rand:uniform() * 100),
	% Sign the data generated above.
	SignedData = sign(Priv, Data),
	% Verify the signed data.
	verify(Pub, SignedData).

%% Good
sign_verify_test({Priv, Pub}) ->
	Data = floor(rand:uniform() * 100),
	SignedData = sign(Priv, Data),
	verify(Pub, SignedData).
```



### Use the minimal descriptive words for function names

Function names should be descriptive enough to explain the high-level purpose of the function whilst remaining short enough as to not hinder the code readability. 

The name of the module can sometimes be used to help increase clarity without increasing the wordiness of the function name. 

```erlang
%% Bad function names
ar_tx:generate_data_segment_for_signing(TX).
ar_util:pretty_print_internal_ip_representation(IPAddr).
ar_retarget:is_current_block_retarget_block(Block).

%% Good function names
ar_tx:to_binary(TX).
ar_block:generate_block_from_shadow(BShadow).
ar_serialize:block_to_json_struct(Block).
```



### Tests should be defined at the tail of the module

Tests should be defined as the last thing present within a module and should be prefixed with the following comment. 

```erlang
% Tests: {module name}
```



### Maximum of eighty characters per line

A maximum of eighty characters should be present on any singular line. 

To help enforce this styling consider using a ruler, most extensible editors will have this functionality by default or a simple plugin should be available to help.



### Try to avoid deeply nested code

Deeply nested code should be avoided as it can mask a large set of alternative code paths and can become very difficult to debug.

Code that uses if, case or receive structures should aim for a singular level of nesting and at most two levels of depth.

```erlang
%% Bad
contains_data_tx([]) -> false;
contains_data_tx(TXList) ->
	[TX|Rest] = TXList,
    case is_record(TX, tx) of
        true ->
            if
            	length(TX#tx.data) > 0 -> true;
            	false -> contains_data_tx(Rest)
            end;
        false -> error_not_tx.
    end.

%% Better
contains_data_tx([]) -> false;
contains_data_tx([TX|Rest]) when is_record(TX, tx) ->
	if
		length(TX#tx.data > 0) -> true;
		false -> contains_data_tx(Rest)
	end;
contains_data_tx(_) ->
	error_not_tx.
```



### Deconstruct arguments in the function header

The maximum number of variables should be deconstructed within the function clause header and not the clause body. 

This makes the arguments to the function explicit and helps debugging as should the wrong form of data be provided no matching function clause will be found.

```erlang
%% Bad
server(State, Keypair) ->
    Keypair = {Priv, Pub},
	State#state { peers = Peers, heard = HeardMsg, ignored = IgnoredMsg },
	...
	
%% Good
server(State#state {
    peers = Peers,
    heard = HeardMsg,
    ignored = IgnoredMsg
}, {Priv, Pub}) ->
	...
```



### Atoms should be lowercase and separated by underscores

For easy recognisability the Arweave codebase uses descriptive lowercase atoms where multiple words are separated by the underscore character. 

```erlang
%% Bad atoms
'iAtom'
'block not found'

%% Good atoms
unavailable
block_not_found
```



### Record definitions should include descriptions of fields

When a new record is defined the information regarding the purpose of each field should be included in a comment inline with the field it pertains to. These comments should be prefixed with a singular '%' character and aim to be as concise as possible.

```erlang
-record(tx, {
	id = <<>>, 			% TX UID (Hash of signature)
	last_tx = <<>>, 	% Wallets last TX hash.
	owner = <<>>, 		% Public key of transaction owner.
	tags = [], 			% Indexable TX category identifiers.
	target = <<>>, 		% Wallet address of target of the tx.
	quantity = 0, 		% Amount of Winston to send
	data = <<>>, 		% Data body (if data transaction).
	signature = <<>>, 	% Transaction signature.
	reward = 0 			% Transaction mining reward.
}).
```



### Redundant or deprecated code should be removed

Should existing code be made redundant with the implementation of new developments, this old code should be removed. It should not be left cluttering the code base as either code or comment. 

If reference to these old implementation details is still required they will remain present in the project repositories version control.



### Modules should export the minimal number of functions

Modules should export the minimum number of functions in which are externally required and these exports should be logically ordered.

This helps show the interface that the module exposes. Via looking at the exports other engineers should be able to identify which functions they need understand and are available for external use.

```erlang
%% Bad function exporting
-compile(export_all).

%% Good function exporting
-export([sign/2, verify/3]).
-export([to_address/1]).
```



### Variable names should be descriptive of the data they contain

Variable names should be descriptive of the data in which they are representing.

This helps in understanding the purpose of a codeblock without the need for verbose comments detailing its purpose.

```erlang
%% Bad variables
sign_data(X, Y) ->
	{A, B} = X,
	sign(A, Y). 

%% Good variables
sign_data(Keypair, Data) ->
    {Priv, Pub} = Keypair,
    sign(Priv, Data).
```

 

 ### Use ar:report to present information to the end user

Presenting information to the user from the Arweave client is done via a live view of the log. Variables can be printed to the screen via the report function in the ar.erl module.

```erlang
%% Example, ar:report from ar_node.erl

ar:report(
	[
		fork_recovered_successfully,
		{height, NewB#block.height}
	]
),
```



### Tuple construction/deconstruction

When constructing or deconstructing tuples ensure a space between each comma separated element.

```erlang
%% Bad
{ one,two,three, A, B, C}

%% Good
{one, two, three, A, B, C}
```



### List deconstruction

When deconstructing a list into head and tail ensure that a space is placed either side of the '|' separation character.

```erlang
%% Bad
[Head|Tail]

%% Good
[Head | Tail]
```



### Record construction/deconstruction

When constructing or deconstructing a record ensure that a space is placed eithter side of each field being handled.

```erlang
%% Bad
State#state {first="hello", second="world"}

%% Good
State#state { first = "hello", second = "world" },
```



### Function arguments on new lines

If the arguments for a given function call exceed the previously stated line length limit (80 characters) or contain an inline function split the arguments each on to new lines.

```erlang
%% Bad
example() ->
	TotalTime = lists:foldl(fun(X, Acc) -> X + Acc end, 0, [12, 15, 8, 21, 35, 33, 14]),
	...
    
%% Better
example() ->
	TotalTime = lists:foldl(
		fun(X, Acc) -> X + Acc end,
		0,
		[12, 15, 8, 21, 35, 33, 14]
	),
	...
```

## Error handling

Functions with side effects (in Erlang it boils down to IO) should return an `{ok, ...}` tuple upon successful execution, and `error_code` or `{error_code, ...}` otherwise.

When invoking functions with side effects, failing fast by only pattern matching against `{ok, ...}` is encouraged.

In rare cases when even unexpected failures have to be processed, like in the HTTP event loop, `try/catch` may be used.


## Version control

The Arweave client codebase is hosted on Github, the below standards define the criteria for committed code. We aim to adhere to these standards as to make it as easy possible for new contributors to get involved.


### All committed code must be commented

All committed code should be fully commented and should aim to fit the styling as detailed in this document. Committing uncommented code is unhelpful to all those maintaining or exploring the project.



### Code pushed to master must work

All code committed to the master branch of the Arweave project should be fully functioning.

This is a **strict** requirement as this is the prime location of where end users will be obtaining the software to join and participate in the network.



### Commits should aim to be as atomic as possible

Code commits should aim to be a single logical change or addition to the codebase, though if not possible all logical alterations should be explained in the commit message, each separated by a comma.

```
- Added generic protocol implementation.
- Removed ar_deprecated.
- Added block shadows, refactored HTTP iface.
```

### Commit message syntax

To keep the repository clean a set structure for commit messages has been decided.

- The first character should be capitalized. 
- The message should be succinct.
- The message should be in the imperative mood.
- Multiple actions should be comma separated.

### Commit description

In addition to a message, a commit should have a description focusing on why the change was made rather than what was made.

### Commit example

```
Add arweave style guide

Inconsistent styling made it hard for us to view, comprehend, and edit the code so we had a discussion and agreed on the common style.
```







