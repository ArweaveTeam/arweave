%%% @doc Specs for the `defrag` option group. Options for
%%% coordinating maintenance of chunk storage files.
-module(arweave_config_options_defragmentation).
-behaviour(arweave_config_options).
-export([specs/0, group/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [defrag, enabled],
			default => false,
			type => boolean,
			legacy => run_defragmentation,
			short_description =>
				<<"Run defragmentation of chunk storage files at "
				  "startup.">>,
			long_description =>
				<<"Master switch for the defragmentation pass. "
				  "Modules to defrag are picked up from "
				  "`storage_modules.[list_item].defrag = true` "
				  "(legacy: the "
				  "`defragment_module` / `defragmentation_modules` "
				  "list).">>
		},
		#{
			enabled => true,
			option_key => [defrag, threshold],
			default => 1_500_000_000,
			type => pos_integer,
			legacy => defragmentation_trigger_threshold,
			short_description =>
				<<"File size threshold in bytes above which a chunk "
				  "storage file is eligible for defragmentation.">>
		}
	].

validate() ->
	ok.

group() ->
	defrag.

group_description() ->
	<<"Control storage defragmentation behavior.">>.
