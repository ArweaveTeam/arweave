-module(av_detect).
-export([is_infected/2]).
-include("av_recs.hrl").

%%% Perform the malware detection.
%%% Given it's task, we should optimise this as much as possible.

%% Given a filename and a set of signatures, test whether the file
%% is infected. If not, return false, if it is, return true and
%% the matching sigs.
%% Only files can be infected, return false for directories.
% is_infected(FileName, Sigs) ->
% 	case try_read(FileName) of
% 		error -> pass;
% 		Bin -> do_is_infected(FileName, Bin, Sigs)
% 	end.

is_infected(Obj, Sigs) ->
	do_is_infected(Obj, Sigs).

%% Try to read the file. If this fails, ignore it.
try_read(FileName) ->
	try element(2, {ok, _} = file:read_file(FileName))
	catch _:_ -> error
	end.

%% Actually perform the check.
% do_is_infected(FileName, Bin, {CPs, Sigs}) ->
% 	case quick_check(CPs, Bin, Hash = av_utils:md5sum(Bin)) of
% 		false -> false;
% 		true ->
% 			full_check(Bin, filelib:file_size(FileName), Hash, Sigs)
% 	end.

do_is_infected(Bin, Sigs) ->
	full_check(Bin, byte_size(Bin), av_utils:md5sum(Bin), Sigs).
%% Perform a quick check. This only tells us whether there is probably an
%% infection, not what that infection is, if there is one. Has a very low
%% false-positive rate.
quick_check(CPs, Bin, Hash) ->
	lists:any(
		fun({no_pattern, _Data}) -> false;
		   ({CP, Data}) ->
			binary:match(Data, CP) =/= nomatch
		end,
		lists:zip(CPs, [Bin, Hash])
	).

%% Perform a full, slow check. This returns false, or a list of infections.
full_check(Bin, Sz, Hash, Sigs) ->
	Res =
		lists:filtermap(
			fun(Sig) -> check_sig(Bin, Sz, Hash, Sig) end,
			Sigs
		),
	case Res of
		[] -> false;
		MatchedSigs -> {true, [ S#sig.name || S <- MatchedSigs ]}
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
	% Check the binary size first, as this is very low cost.
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
