-module(ar_tx_blacklist_utils).

%%% ==================================================================
%%% API
%%% ==================================================================

-export([
	unique/1,
	hex_to_binary/1,
	binary_to_hex/1,
	md5sum/1,
	load_sign/1,
	is_infected/2
]).

%%% ==================================================================
%%% Includes
%%% ==================================================================

-include("ar_tx_blacklist.hrl").

%%% ==================================================================
%%% API
%%% ==================================================================

%% Delete repeated elements in a list.
unique(L) ->
	sets:to_list(sets:from_list(L)).

%% Convert a binary into a hex text string.
binary_to_hex(Bin) ->
	lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).

%% Convert a hex string into a binary.
hex_to_binary(S) ->
	hex_to_binary(S, []).

%% Calculate the MD5 sum for a file, returning it in binary format.
%% We should improve this later to only load 1mb chunks at a time.
%% We could even spawn 2 processes, one to read from the HD, and
%% the other to do the hashing. That might be much more efficient
%% for large files.
md5sum(FileName) when is_list(FileName) ->
	{ok, Bin} = file:read_file(FileName),
	md5sum(Bin);
md5sum(Bin) ->
	erlang:md5(Bin).

%% Load the signatures from the configured list of files and compiles them into the binary
%% scan objects, for efficient search.
load_sign(Files) ->
	Sigs = unique(
		lists:append(
	  		lists:map(
				fun do_load/1,
				Files
			)
		)
	),
	BinarySigs = [(Sig#sig.data)#binary_sig.binary || Sig <- binary_sigs(Sigs)],
	BinaryPattern = case BinarySigs of
		[] ->
			no_pattern;
		_ ->
			binary:compile_pattern(BinarySigs)
	end,
	{Sigs, BinaryPattern}.

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

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

hex_to_binary([], Acc) ->
	list_to_binary(lists:reverse(Acc));
hex_to_binary([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	hex_to_binary(T, [V | Acc]).

%% Load the signatures from a specified file, throwing away signatures
%% that we are not able to process.
do_load(File) ->
	case filelib:is_file(File) of
		false ->
			warn_on_load(File, lookup_file, [invalid_file]),
			[];
		true ->
			Fun =
				case filename:extension(File) of
					".ndb" -> fun create_binary_sig_from_hex/1;
					".hdb" -> fun create_hash_sig/1;
					".hsb" -> fun create_hash_sig/1;
					".fp" -> fun create_hash_sig/1;
					".txt" -> fun create_binary_sig/1;
					Other -> {extension_not_supported, Other}
				end,
			case Fun of
				{extension_not_supported, Extension} ->
					warn_on_load(File, check_file_extension, [{file_extension_not_supported, Extension}]),
					[];
				_ ->
					try
						lists:filtermap(
							fun(Row) ->
								try
									S = Fun(Row),
									{true, S}
								catch Type:Pattern ->
									warn_on_load(File, parse_row, [{row, Row}, {exception, {Type, Pattern}}]),
									false
								end
							end,
							parse_csv_file(File, $:)
						)
					catch Type:Pattern ->
						warn_on_load(File, load_file, [{exception, {Type, Pattern}}]),
						[]
					end
			end
	end.

warn_on_load(File, Step, Context) ->
	Warning = [{load_content_policies, Step}, {file, File}] ++ Context,
	ar:warn(Warning),
	ar:console(Warning).

%% Take a CSV row and return a binary sig object.
create_binary_sig_from_hex([Name, Type, Offset, Sig]) ->
	#sig {
		name = Name,
		type = binary,
		data =
			#binary_sig {
				target_type = Type,
				offset =
					if Offset == "*" -> any;
					true -> list_to_integer(Offset)
					end,
				binary = hex_to_binary(Sig)
			}
	};
create_binary_sig_from_hex([Sig, Name]) ->
	#sig {
		name = Name,
		type = binary,
		data =
			#binary_sig {
				offset = any,
				binary = hex_to_binary(Sig)
			}
	}.

create_binary_sig([Sig]) ->
	#sig {
		type = binary,
		data =
			#binary_sig {
				offset = any,
				binary = list_to_binary(Sig)
			}
	}.

%% Take a CSV row and return a hash sig object.
create_hash_sig([Hash, Size, Name]) ->
	#sig {
		name = Name,
		type = hash,
		data =
			#hash_sig {
				size = list_to_integer(Size),
				hash = hex_to_binary(Hash)
			}
	};
create_hash_sig([Hash, Name]) ->
	#sig {
		name = Name,
		type = hash,
		data =
			#hash_sig {
				size = any,
				hash = hex_to_binary(Hash)
			}
	}.

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
				Hash = md5sum(Subject),
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

%% Takes a filename and returns a parsed list of CSV rows.
parse_csv_file(File, SepChar) ->
	{ok, Bin} = file:read_file(File),
	parse_csv(binary_to_list(Bin), SepChar).

%% Takes a string in CSV format, returning a list of rows of data.
parse_csv(Str, SepChar) ->
	lists:map(
		fun(Row) -> string:tokens(Row, [SepChar]) end,
		string:tokens(Str, "\n")
	).
