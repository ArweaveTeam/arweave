-module(ar_parser).

-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%
% A suite of functions for handling arithmetical expressions
%



-type expr() :: {'equals',string(),string()}
             |  {'and',expr(),expr()}
             |  {'or',expr(),expr()}.


-spec print(expr()) -> string().

print({num,N}) ->
    integer_to_list(N);
print({var,A}) ->
    atom_to_list(A);
print({add,E1,E2}) ->
    "("++ print(E1) ++ "+" ++ print(E2) ++")";
print({mul,E1,E2}) ->
    "("++ print(E1) ++ "*" ++ print(E2) ++")".

%
% parsing
%

% recognise expressions
% deterministic, recursive descent, parser.

% the function returns two things
%   - an expression recognised at the beginning of the string
%     (in fact, the longers such expression)
%   - whatever of the string is left
%
% for example, parse("(-55*eeee)+1111)") is             
%   {{mul,{num,-55},{var,eeee}} , "+1111)"}


% recognise a fully-bracketed expression, with no spaces etc.

-spec parse(string()) -> {expr(), string()}.

parse([$(|Rest]) ->                            % starts with a '('
      {E1,Rest1}     = parse(Rest),            % then an expression
      [Op|Rest2]     = Rest1,                  % then an operator, '+' or '*'
      {E2,Rest3}     = parse(Rest2),           % then another expression
      [$)|RestFinal] = Rest3,                  % starts with a ')'
      {case Op of
	  $+ -> {add,E1,E2};
	  $* -> {mul,E1,E2}
        end,
       RestFinal};

% recognise an integer, a sequence of digits
% with an optional '-' sign at the start

parse([Ch|Rest]) when ($0 =< Ch andalso Ch =< $9) orelse Ch==$- ->
    {Succeeds,Remainder} = get_while(fun is_digit/1,Rest),
    {{num, list_to_integer([Ch|Succeeds])}, Remainder};


% recognise a variable: an atom built of small letters only.

parse([Ch|Rest])  when $a =< Ch andalso Ch =< $z ->
    {Succeeds,Remainder} = get_while(fun is_alpha/1,Rest),
    {{var, list_to_atom([Ch|Succeeds])}, Remainder}.

% auxiliary functions

% recognise a digit

-spec is_digit(integer()) -> boolean().

is_digit(Ch) ->
    $0 =< Ch andalso Ch =< $9.

% recognise a small letter

-spec is_alpha(integer()) -> boolean().

is_alpha(Ch) ->
    $a =< Ch andalso Ch =< $z.

% the longest initial segment of a list in which all
% elements have property P. Used in parsing integers
% and variables

-spec get_while(fun((T) -> boolean()),[T]) -> {[T],[T]}.    
%-spec get_while(fun((T) -> boolean()),[T]) -> [T].    
			 
get_while(P,[Ch|Rest]) ->
    case P(Ch) of
	true ->
	    {Succeeds,Remainder} = get_while(P,Rest),
	    {[Ch|Succeeds],Remainder};
	false ->
	    {[],[Ch|Rest]}
    end;
get_while(_P,[]) ->
    {[],[]}.

%
% Evaluate an expression
%

% First version commented out.

eval(true) -> true;
eval(false) -> false;
eval([]) -> [];
eval({equals, Key, Value}) ->
    app_search:search_by_exact_tag(Key, Value);
eval({'and',E1,E2}) ->
    sets:to_list(
        sets:intersection(
            sets:from_list(eval(E1)),
            sets:from_list(eval(E2))
        )
    );
eval({'or',E1,E2}) ->
    sets:to_list(
        sets:union(
            sets:from_list(eval(E1)),
            sets:from_list(eval(E2))
        )
    ).
	     