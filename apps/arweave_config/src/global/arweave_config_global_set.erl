-module(arweave_config_global_set).
-compile(export_all).

parent() -> arweave_config_global.

name() -> set.

arguments() ->
	#{
		key => #{
			position => 1,
			required => true,
			type => string,
			check => fun(_, _) -> ok end,
			help => "arweave configuration key."
		},
		value => #{
			position => 2,
			required => true,
			type => string,
			check => fun(_, _) -> ok end,
			help => "arweave configuration value."

		}
	 }.
