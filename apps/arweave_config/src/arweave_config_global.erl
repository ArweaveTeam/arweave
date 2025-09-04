%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global).
-compile(export_all).

init(Args) ->
	ok.

subcommands() -> [<<"show">>, <<"set">>, <<"get">>].

subcommands(<<"show">>) ->
	{next, arweave_config_global_show};
subcommands(<<"set">>) ->
	{next, arweave_config_global_set};
subcommands(<<"get">>) ->
	{next, arweave_config_global_get};
subcommands(<<"export">>) ->
	{next, arweave_config_global_export};
subcommands(<<"import">>) ->
	{next, arweave_config_global_import}.

short_description() ->
	{ok, "Configure arweave global configuration."}.

long_description() ->
	{ok, ""}.
