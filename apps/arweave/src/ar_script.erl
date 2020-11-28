-module(ar_script).
-export([run/2, address/1]).
-export([encode_input/1, decode_input/1]).
-export([encode_script/1, decode_script/1]).
-include("ar.hrl").

%%% A stateless, Turing Complete VM for acceptance of Arweave 
%%% transactions. Execution uses a simple stack machine, where the input fields
%%% are loaded (top-to-bottom) as the initial state.
%%% 
%%% This is not how you build smart contracts on Arweave. That should, in 
%%% general, be done using the base layer for program and input storage, and a
%%% second layer for compute.
%%% 
%%% There are no gas fees in Arweave script, but a simple execution limit for 
%%% for all transactions. Executions below the limit will succeed, those above 
%%% it will fail. ?ARSCRIPT_MAX_REDS defines the maximum number of reductions 
%%% the VM will take.
%%% 
%%% The machine works with two data types: integers (bignums), and binaries.
%%% 
%%% The maximum size for scripts and inputs is defined in ?ARSCRIPT_MAX_SZ.
%%% 
%%% Currently accepted opcodes are:
%%% 
%%% Cryptography:
%%%     hash/0 -> push ( hash(pop) )
%%%     sig_tx/1: addr -> push ( sig_validate true -> 1; false -> 0)
%%%     sig_state/1: addr -> push ( sig_validate true -> 1; false -> 0)
%%%     sig_data/2: addr, data<32 bytes> ->
%%%         push ( sig_validate true -> 1; false -> 0)
%%% Arithmetic:
%%%     add/0 -> push (pop + pop)
%%%     sub/0 -> push (pop - pop)
%%%     idiv/0 -> push (pop div pop)
%%%     mul/0 -> push (pop * pop)
%%%     sum/0 -> push (stack0 + ... + stackn)
%%% Conditions:
%%%     eq/0 -> push (if pop == pop -> 1; else -> 0)
%%%     gt/0 -> push (if pop > pop -> 1; else -> 0)
%%%     geq/0 -> push (if pop >= pop -> 1; else -> 0)
%%%     lt/0 -> push (if pop < pop -> 1; else -> 0)
%%%     leq/0 -> push (if pop =< pop -> 1; else -> 0)
%%%     not/0 -> push (if pop == 0 -> 1; else -> 0)
%%% Data stack pushes:
%%%     dup/0 -> push (pop), push (pop)
%%%     push/1: data<32 bytes|int> -> push data
%%%     height/0 -> push B#block.height
%%%     weave_size/0 -> push B#block.height
%%%     timestamp/0 -> push B#block.timestamp
%%%     indep_hash/0 -> push B#block.indep_hash
%%%     dep_hash/0 -> push B#block.dep_hash
%%%     last_block/0 -> push B#block.last_hash
%%%     reward_pool/0 -> push B#block.reward_pool
%%%     diff/0 -> push B#block.diff
%%%     label_addr/1: label -> push ([address of label in program text])
%%%     data_at/1: int -> push ([data at addr])
%%% Control:
%%%     jmp/1 label -> move to label
%%%     condjmp/1 label -> if(pop == 1 -> [move to label]; else -> no_op)
%%%     halt/0 -> [terminate execution and trigger stack inspection]
%%% Conversions:
%%%     to_binary/0 -> push (bignum_to_binary(pop))
%%%     to_int/0 -> push (binary_to_bignum(pop))
%%%
%%% After execution has reached the `halt` opcode, the first value on the stack 
%%% will is inspected.
%%%     1 -> Accept the transaction.
%%%     0 -> Transaction failed.

%% Does a transaction's script and input validate correctly?
run(TX, B) ->
   apply_ops(TX, B, TX#tx.script, TX#tx.script, TX#tx.input, 0).

apply_ops(_TX, _B, _, [halt|_], [1|_], _) -> true;
apply_ops(_TX, _B, _, [halt|_], [0|_], _) -> false;
apply_ops(TX, B, FullScript, [Op|Pipeline], Stack, Reds) ->
    case Reds + reds(Op, TX, B, Stack) of
        Exceeds when Exceeds > ?ARSCRIPT_MAX_REDS ->
            false;
        NewReds ->
            try apply_op(Op, Stack, TX, B, FullScript) of
                {NewPipeline, NewStack} ->
                    apply_ops(TX, B, FullScript, NewPipeline, NewStack, NewReds);
                NewStack ->
                    apply_ops(TX, B, FullScript, Pipeline, NewStack, NewReds)
            catch _:_ -> false
            end
    end;
apply_ops(_TX, _B, _FullScript, _Pipeline, _Stack, _Reds) -> false.

% Arithmetic operations
apply_op(add, [ S0, S1 | Stack ], _, _, _) -> [ S0 + S1 | Stack ];
apply_op(sub, [ S0, S1 | Stack ], _, _, _) -> [ S0 - S1 | Stack ];
apply_op(idiv, [ S0, S1 | Stack ], _, _, _) -> [ S0 div S1 | Stack ];
apply_op(mul, [ S0, S1 | Stack ], _, _, _) -> [ S0 * S1 | Stack ];
apply_op(sum, Stack, _, _, _) -> [ lists:sum(Stack) ];
% Conditions operations
apply_op(eq, [ S0, S0 | Stack ], _, _, _) -> [ 1 | Stack ];
apply_op(eq, [ _, _ | Stack ], _, _, _) -> [ 0 | Stack ];
apply_op(gt, [ S0, S1 | Stack ], _, _, _) ->
    if S0 > S1 -> [ 1 | Stack ];
    true -> [ 0 | Stack ]
    end;
% ...
apply_op('not', [ 0 | Stack ], _, _, _) -> [ 1 | Stack ];
apply_op('not', [ _ | Stack ], _, _, _) -> [ 0 | Stack ];
% Data stack pushes
apply_op(dup, [ S0 | Stack ], _, _, _) -> [ S0, S0 | Stack ];
apply_op({push, Data}, Stack, _, _, _) -> [ Data | Stack ];
apply_op(height, Stack, _, B, _) -> [ B#block.height | Stack ];
% ...
apply_op({label_addr, Label}, Stack, _, _, Script) ->
    [ label_addr(Label, Script) | Stack ];
apply_op({data_at, Seek}, Stack, _, _, FullScript) ->
    [ {data, Data} | _ ] = seek(Seek, FullScript),
    [ Data | Stack ];
% Control
apply_op({jmp, Seek}, Stack, _, _, FullScript) ->
    { seek(Seek, FullScript), Stack };
apply_op({cjmp, Seek}, [ 1 | Stack ], _, _, FullScript) ->
    { seek(Seek, FullScript), Stack };
apply_op({cjmp, _}, [ _ | Stack ], _, _, _) -> Stack;
% Else, the instruction is not implemented.
apply_op(_, _, _, _, _) ->
    error(opcode_not_recognised).

label_addr(Label, [{label, Label}|_]) -> 0;
label_addr(Label, [_|Ops]) ->
    1 + label_addr(Label, Ops).

seek(Pos, Script) when is_integer(Pos) ->
    seek_pos(Pos, Script);
seek(Label, Script) ->
    seek_label(Label, Script).

seek_pos(0, Pipeline) -> Pipeline;
seek_pos(ToGo, [_|Ops]) ->
    seek_pos(ToGo - 1, Ops).

seek_label(Label, [{label, Label}|Pipeline]) -> Pipeline;
seek_label(Label, [_|Ops]) ->
    seek_label(Label, Ops).

%% Return the number of reductions for executing an opcode.
reds(height, _, _, _) -> 1.

%% Generate a transaction's 'subject' address from its script.
address(TX) ->
    ar_deep_hash:hash(TX#tx.script).

encode_input(_) -> [].

decode_input(_) -> undefined.

encode_script(_) -> [].

decode_script(_) -> undefined.