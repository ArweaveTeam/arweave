%%% @doc The module with utilities for performing computations on decimal fractions.
-module(ar_decimal).

-export([
	pow/2,
	natural_exponent/2,
	factorial/1
]).

%%%===================================================================
%%% Types.
%%%===================================================================

-type decimal() :: {integer(), integer()}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Compute the given power of the given integer.
-spec pow(X::integer(), P::integer()) -> integer().
pow(_X, 0) ->
	1;
pow(X, 1) ->
	X;
pow(X, 2) ->
	X * X;
pow(X, 3) ->
	X * X * X;
pow(X, 4) ->
	X * X * X * X;
pow(X, 5) ->
	X * X * X * X * X;
pow(X, 6) ->
	X * X * X * X * X * X;
pow(X, 7) ->
	X * X * X * X * X * X * X;
pow(X, 8) ->
	X * X * X * X * X * X * X * X;
pow(X, 9) ->
	X * X * X * X * X * X * X * X * X;
pow(X, 10) ->
	X * X * X * X * X * X * X * X * X * X;
pow(X, 11) ->
	X * X * X * X * X * X * X * X * X * X * X;
pow(X, 12) ->
	X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 13) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 14) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 15) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 16) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 17) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 18) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 19) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, 20) ->
	X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X * X;
pow(X, N) ->
	X * pow(X, N - 1).

%% @doc Compute the X's power of e by summing up the terms of the Taylor series where
%% the last term is a multiple of X to the power of P.
-spec natural_exponent(X::decimal(), P::integer()) -> decimal().
natural_exponent(X, P) ->
	{natural_exponent_dividend(X, P, 0, 1), natural_exponent_divisor(X, P)}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

natural_exponent_dividend(_X, -1, _K, _M) ->
	0;
natural_exponent_dividend({Dividend, Divisor} = X, P, K, M) ->
	pow(Dividend, P) * pow(Divisor, K) * M + natural_exponent_dividend(X, P - 1, K + 1, M * P).

natural_exponent_divisor({_Dividend, Divisor}, P) ->
	pow(Divisor, P) * factorial(P).

factorial(0) ->
	1;
factorial(1) ->
	1;
factorial(9) ->
	362880;
factorial(10) ->
	3628800;
factorial(20) ->
	2432902008176640000;
factorial(N) ->
	N * factorial(N - 1).
