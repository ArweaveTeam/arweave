-module(ar_graphql).

%% API
-export([load_schema/0]).

load_schema() ->
	PrivDir = code:priv_dir(arweave),
	Filename = filename:join([PrivDir, "schema.graphql"]),
	{ok, SchemaData} = file:read_file(Filename),
	Mapping = mapping_rules(),
	ok = graphql:load_schema(Mapping, SchemaData),
	ok = setup_root(),
	ok = graphql:validate_schema(),
	ok.

mapping_rules() ->
	#{ objects => #{
		'Transaction' => ar_graphql_transaction,
		'Tag' => ar_graphql_tag,
		'Query' => ar_graphql_query
	} }.

setup_root() ->
	Root = {root, #{ query => 'Query' }},
	ok = graphql:insert_schema_definition(Root),
	ok.
