-module(av_sigs).
-export([quick/0, deep/0, all/0, load/1]).
-include("av_recs.hrl").
-include("ar.hrl").

%%% Loads signature definition files from the sigs/ directory.
%%% Supports multiple definition types, returning a #sig record
%%% for each signature in the database. These uniform sig objects
%%% can then be passed to av_detect.

%% Defines the default directory in which the signature files are
%% normally located.
-define(SIG_DB_LOC, "src/av/sigs").
%% Define all of the supported file formats.
-define(SUPPORTED_FORMATS, [".ndb", ".hdb", ".hsb", ".fp"]).

%% Return a set of signatures that will not take long to execute.
quick() -> quick(all()).
quick(Sigs) ->
	[ S || S <- Sigs, S#sig.type == hash ].

%% Return the full set of 'deep' probes. Runs impossibly slowly for
%% any 'real' use.
deep() -> deep(all()).
deep(Sigs) ->
	[ S || S <- Sigs, S#sig.type == binary ].

%% Load a set of signatures (either quick, deep, or all at the moment).
load(Str) when is_list(Str) -> load(list_to_existing_atom(Str));
load(Type) ->
	Sigs =
		case Type of
			quick -> quick();
			deep -> deep();
			all -> all();
			full -> all()
		end,
	% Return a list of compiled quick checks to perform, along with the
	% real signatures.
	{
		[
			compile(
				[ (S#sig.data)#binary_sig.binary || S <- deep(Sigs) ]),
			compile(
				[ (S#sig.data)#hash_sig.hash || S <- quick(Sigs) ])
		],
		Sigs
	}.

%% Take a list of binaries, create a compiled pattern.
compile([]) -> no_pattern;
compile(Bins) -> binary:compile_pattern(Bins).

%% Process all of the signature databases in the default directory.
all() ->
	av_utils:unique(
		lists:append(
	  		lists:map(
				fun do_load/1,
				[ ?SIG_DB_LOC ++ "/" ++ File ||
					File <- filter_dbs(file:list_dir(?SIG_DB_LOC))]
			)
		)
	).

%% Ignore files that we cannot read.
filter_dbs({ok, DBs}) -> filter_dbs(DBs);
filter_dbs(DBs) ->
	[ DB || DB <- DBs,
		lists:member(filename:extension(DB), ?SUPPORTED_FORMATS) ].

%% Load the signatures from a specified file, throwing away signatures
%% that we are not able to process.
do_load(File) ->
	Fun =
		case filename:extension(File) of
			".ndb" -> fun create_binary_sig/1;
			".hdb" -> fun create_hash_sig/1;
			".hsb" -> fun create_hash_sig/1;
			".fp" -> fun create_hash_sig/1
		end,
	lists:filtermap(
		fun(Row) ->
			try
				S = Fun(Row),
				{true, S}
			catch _:_ ->
				%sv:log(warning, "Unable to process row.", []),
				false
			end
		end,
		av_csv:parse_file(File, $:)
	).

%% Take a CSV row and return a binary sig object.
create_binary_sig([Name, Type, Offset, Sig]) ->
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
create_binary_sig([Sig, Name]) ->
	#sig {
		name = Name,
		type = binary,
		data =
			#binary_sig {
				offset = any,
				binary = av_utils:hex_to_binary(Sig)
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
