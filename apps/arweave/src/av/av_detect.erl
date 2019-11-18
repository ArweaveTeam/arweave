-module(av_detect).
-export([is_infected/2]).
-include("av_recs.hrl").

%%% Perform the illicit content detection.
%%% Given it's task, we should optimise this as much as possible.

%% Given a list of binaries and a set of signatures, test whether some binaries
%% contain illicit content or have illicit hash. If not, return false, if yes, return true and
%% the matching signatures.
is_infected(Subjects, {Sigs, BinaryPattern}) ->
	MatchingSigs = lists:append(
		matching_hash_sigs(Subjects, Sigs),
		matching_binary_sigs(Subjects, Sigs, BinaryPattern)
	),
	case MatchingSigs of
		[] ->
			false;
		_ ->
			{true, MatchingSigs}
	end.

matching_hash_sigs(Subjects, Sigs) ->
	matching_sigs(Subjects, hash_sigs(Sigs)).

matching_binary_sigs(Subjects, Sigs, BinaryPattern) ->
	case binary_sigs(Sigs) of
		[] ->
			[];
		BinarySigs ->
			case quick_check(Subjects, BinaryPattern) of
				maybe_infected ->
					%% The full check makes sure it is not a false positive, and collects matching
					%% signatures.
					matching_sigs(Subjects, BinarySigs);
				not_infected ->
					[]
			end
	end.

hash_sigs(Sigs) ->
	[Sig || Sig <- Sigs, Sig#sig.type == hash].

binary_sigs(Sigs) ->
	[Sig || Sig <- Sigs, Sig#sig.type == binary].

%% Performs a quick check. It's faster than matching_sigs/2 because we
%% join all subjects together. The return of not_infected means it's defenitely
%% not infected. It can also return maybe_infected, which means it's most likely
%% that one of the subjects is infected, but we need to run a matching_sigs/2 to be sure.
quick_check(Subjects, Pattern) ->
	CompositeSubject = << Subject || Subject <- Subjects >>,
	case binary:match(CompositeSubject, Pattern) of
		nomatch ->
			not_infected;
		_ ->
			maybe_infected
	end.

%% Perform a full, slow check. Every subject is checked against every signature.
%% Returns a list of matching signatures.
matching_sigs(Subjects, Sigs) ->
	lists:flatten(
		lists:map(
			fun(Subject) ->
				Hash = av_utils:md5sum(Subject),
				matching_sigs(Subject, Hash, Sigs)
			end,
			Subjects
		)
	).

matching_sigs(Subject, Hash, Sigs) ->
	MatchingSigs =
		lists:filtermap(
			fun(Sig) ->
				case is_sig_matching(Subject, Hash, Sig) of
					false -> false;
					true -> {true, Sig}
				end
			end,
			Sigs
		),
	[get_sig_name(S) || S <- MatchingSigs].

get_sig_name(S) ->
	case S#sig.name of
		undefined ->
			case S#sig.type of
				binary ->
					iolist_to_binary(io_lib:format("Contains ~s", [(S#sig.data)#binary_sig.binary]));
				hash ->
					iolist_to_binary(io_lib:format("Has hash ~s", [(S#sig.data)#hash_sig.hash]))
			end;
		Name ->
			Name
	end.

%% Perform the actual check.
is_sig_matching(Subject, _Hash, #sig { type = binary, data = SigData }) ->
	case binary:match(Subject, SigData#binary_sig.binary) of
		nomatch -> false;
		{FoundOffset, _} ->
			% The file is infected. If the offset was set, check it.
			case SigData#binary_sig.offset of
				any -> true;
				FoundOffset -> true;
				_ -> false
			end
	end;
is_sig_matching(Subject, Hash,
		#sig { type = hash, data = SigData = #hash_sig { hash = SigHash } }) ->
	SubjectSize = byte_size(Subject),
	case SigData#hash_sig.size of
		SubjectSize ->
			Hash == SigHash;
		any ->
			Hash == SigHash;
		_ ->
			false
	end.
