%%% @doc The module with utilities for performing computations on fractions.
-module(ar_fraction).

-export([pow/2, natural_exponent/2, factorial/1, minimum/2, maximum/2, multiply/2, reduce/2,
		add/2]).

%%%===================================================================
%%% Types.
%%%===================================================================

-type fraction() :: {integer(), integer()}.

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
pow(X, N) ->
	case N rem 2 of
		0 ->
			pow(X * X, N div 2);
		1 ->
			X * pow(X * X, N div 2)
	end.

%% @doc Compute the X's power of e by summing up the terms of the Taylor series where
%% the last term is a multiple of X to the power of P.
-spec natural_exponent(X::fraction(), P::integer()) -> fraction().
natural_exponent({0, _Divisor}, _P) ->
	{1, 1};
natural_exponent(X, P) ->
	{natural_exponent_dividend(X, P, 0, 1), natural_exponent_divisor(X, P)}.

%% @doc Return the smaller of D1 and D2.
-spec minimum(D1::fraction(), D2::fraction()) -> fraction().
minimum({Dividend1, Divisor1} = D1, {Dividend2, Divisor2} = D2) ->
	case Dividend1 * Divisor2 < Dividend2 * Divisor1 of
		true ->
			D1;
		false ->
			D2
	end.

%% @doc Return the bigger of D1 and D2.
-spec maximum(D1::fraction(), D2::fraction()) -> fraction().
maximum(D1, D2) ->
	case minimum(D1, D2) of
		D1 ->
			D2;
		D2 ->
			D1
	end.

%% @doc Return the product of D1 and D2.
-spec multiply(D1::fraction(), D2::fraction()) -> fraction().
multiply({Dividend1, Divisor1}, {Dividend2, Divisor2}) ->
	{Dividend1 * Dividend2, Divisor1 * Divisor2}.

%% @doc Reduce the fraction until both the divisor and dividend are smaller than
%% or equal to Max. Return at most Max or at least 1 / Max.
-spec reduce(D::fraction(), Max::integer()) -> fraction().
reduce({0, Divisor}, _Max) ->
	{0, Divisor};
reduce({Dividend, Divisor}, Max) ->
	GCD = gcd(Dividend, Divisor),
	reduce2({Dividend div GCD, Divisor div GCD}, Max).

%% @doc Return the sum of two fractions.
-spec add(A::fraction(), B::integer()) -> fraction().
add({Dividend1, Divisor1}, {Dividend2, Divisor2}) ->
	{Dividend1 * Divisor2 + Dividend2 * Divisor1, Divisor1 * Divisor2}.

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

reduce2({Dividend, Divisor}, Max) when Dividend > Max ->
	case Divisor div 2 of
		0 ->
			{Max, 1};
		_ ->
			reduce({Dividend div 2, Divisor div 2}, Max)
	end;
reduce2({Dividend, Divisor}, Max) when Divisor > Max ->
	case Dividend div 2 of
		0 ->
			{1, Max};
		_ ->
			reduce({Dividend div 2, Divisor div 2}, Max)
	end;
reduce2(R, _Max) ->
	R.

gcd(A, B) when B > A ->
	gcd(B, A);
gcd(A, B) when A rem B > 0 ->
	gcd(B, A rem B);
gcd(A, B) when A rem B =:= 0 ->
	B.
