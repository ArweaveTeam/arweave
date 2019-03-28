-module(av_sigs).
-export([load/1]).
-include("av_recs.hrl").

%% Load the signatures from the configured list of files and compiles them into the binary
%% scan objects, for efficient search.
load(Files) ->
	Sigs = av_utils:unique(
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
	HashSigs = [(Sig#sig.data)#hash_sig.hash || Sig <- hash_sigs(Sigs)],
	HashPattern = case HashSigs of
		[] ->
			no_pattern;
		_ ->
			binary:compile_pattern(HashSigs)
	end,
	{Sigs, BinaryPattern, HashPattern}.

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
							av_csv:parse_file(File, $:)
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

binary_sigs(Sigs) ->
	[Sig || Sig <- Sigs, Sig#sig.type == binary].

hash_sigs(Sigs) ->
	[Sig || Sig <- Sigs, Sig#sig.type == hash].

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
				binary = av_utils:hex_to_binary(Sig)
			}
	};
create_binary_sig_from_hex([Sig, Name]) ->
	#sig {
		name = Name,
		type = binary,
		data =
			#binary_sig {
				offset = any,
				binary = av_utils:hex_to_binary(Sig)
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
				hash = av_utils:hex_to_binary(Hash)
			}
	};
create_hash_sig([Hash, Name]) ->
	#sig {
		name = Name,
		type = hash,
		data =
			#hash_sig {
				size = any,
				hash = av_utils:hex_to_binary(Hash)
			}
	}.
