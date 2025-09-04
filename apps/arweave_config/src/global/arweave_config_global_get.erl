-module(arweave_config_global_get).
-compile(export_all).

% parent() -> arweave_config_global.
% 
% name() -> get.
% 
% arguments() ->
% 	#{
% 		key => #{
% 			position => 1,
% 			required => true,
% 			type => string,
% 			check => fun(_, _) -> ok end,
% 			help => "arweave configuration key."
% 		},
% 		verbosity => #{
% 			default => 0,
% 			required => false,
% 			length => 1,
% 			short => [<<"-v">>],
% 			long => [<<"--verbosity">>],
% 			type => pos_integer,
% 			check => fun(_, _) -> ok end,
% 			help => "increase or decrease verbosity.",
% 			examples => [{0, "disable verbosity"}]
% 		},
% 		debug => #{
% 			   default => false,
% 			   required => false,
% 			   length => 0,
% 			   short => [<<"-d">>],
% 			   long => [<<"--debug">>],
% 			   type => boolean,
% 			   check => fun(_, _) -> ok end,
% 			   help => "active command debugging."
% 		}
% 	}.
% 	
% % final action
% handle(State) ->
% 	Value = arweave_config_store:get(Key),
% 	{ok, Value}.
