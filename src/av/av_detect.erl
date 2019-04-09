-module(av_detect).
-export([is_infected/2]).
-include("av_recs.hrl").

%%% Perform the illicit content detection.
%%% Given it's task, we should optimise this as much as possible.

%% Given a binary and a set of signatures, test whether the binary
%% contains illicit content. If not, return false, if it is, return true and
%% the matching signatures.
is_infected(Binary, {Sigs, BinaryPattern, HashPattern}) ->
	Hash = av_utils:md5sum(Binary),
	case quick_check(Binary, BinaryPattern) orelse quick_check(Hash, HashPattern) of
		true ->
			%% The full check makes sure it is not a false positive, and collects matching
			%% signatures.
			full_check(Binary, byte_size(Binary), Hash, Sigs);
		_ ->
			false
	end.

%% Perform a quick check. This only tells us whether there is probably an
%% infection, not what that infection is, if there is one. Has a very low
%% false-positive rate. If there is illicit content, returns true in 100% of the cases.
%% There is a small chance related to joining the independent chunks together before
%% compiling them that it will return true when there is no illicit content.
%% If the pattern is no_pattern, returns true indicating the need for a full check.
quick_check(_, no_pattern) -> true;
quick_check(Data, Pattern) ->
	binary:match(Data, Pattern) =/= nomatch.

%% Perform a full, slow check. This returns false, or true with a list of matched signatures.
full_check(Bin, Sz, Hash, Sigs) ->
	Res =
		lists:filtermap(
			fun(Sig) -> check_sig(Bin, Sz, Hash, Sig) end,
			Sigs
		),
	case Res of
		[] -> false;
		MatchedSigs -> {true, [ get_sig_name(S) || S <- MatchedSigs ]}
	end.

get_sig_name(S) ->
	Name = S#sig.name,
	case Name of
		undefined ->
			Type = S#sig.type,
			case Type of
				binary ->
					iolist_to_binary(io_lib:format("Contains ~s", [(S#sig.data)#binary_sig.binary]));
				hash ->
					iolist_to_binary(io_lib:format("Has hash ~s", [(S#sig.data)#hash_sig.hash]))
			end;
		_ ->
			Name
	end.

%% Perform the actual check.
check_sig(Bin, _Sz, _Hash, S = #sig { type = binary, data = D }) ->
	case binary:match(Bin, D#binary_sig.binary) of
		nomatch -> false;
		{FoundOffset, _} ->
			% The file is infected. If the offset was set, check it.
			case D#binary_sig.offset of
				any -> {true, S};
				FoundOffset -> {true, S};
				_ -> false
			end
	end;
check_sig(_Bin, Sz, Hash,
		S = #sig { type = hash, data = D = #hash_sig { hash = SigHash } }) ->
	%% Check the binary size first, as this is very low cost.
	case D#hash_sig.size of
		Sz -> check_hash(Hash, SigHash, S);
		any -> check_hash(Hash, SigHash, S);
		_ -> false
	end.

%% Perform a hash check, returning filtermap format results.
check_hash(Hash, SigHash, S) ->
	if SigHash == Hash -> {true, S};
	true -> false
	end.
