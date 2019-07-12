-module(ar_graphql_tag).

-include("ar_graphql.hrl").

%% API
-export([execute/4]).

%% Utility functions
-export([from_raw_tag/2]).

execute(_, #ar_graphql_tag { name = Name }, <<"name">>, #{}) ->
	{ok, Name};
execute(_, #ar_graphql_tag { value = Value }, <<"value">>, #{}) ->
	{ok, Value}.

from_raw_tag(Name, Value) ->
	case sanitize_bin_string(Name) of
		{ok, SanitizedName} ->
			case Value of
				BinValue when is_binary(BinValue) ->
					case sanitize_bin_string(BinValue) of
						{ok, SanitizedValue} ->
							#ar_graphql_tag {
								name = SanitizedName,
								value = SanitizedValue
							};
						error ->
							error
					end;
				OtherValue ->
					#ar_graphql_tag {
						name = SanitizedName,
						value = OtherValue
					}
			end;
		error ->
			error
	end.

sanitize_bin_string(Bin) ->
	case unicode:characters_to_binary(Bin) of
		UnicodeString when is_binary(UnicodeString) ->
			{ok, UnicodeString};
		_ ->
			error
	end.
