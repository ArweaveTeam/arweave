-module(ar_webhook).
-export([start/1, new_block/2, new_transaction/2]).

start(Configs) ->
	Workers = lists:map(
		fun(Config) ->
			{ok, Worker} = ar_webhook_worker:start_link(Config),
			Worker
		end,
		Configs
	),
	adt_simple:start(?MODULE, Workers).

new_block(Workers, Block) ->
	lists:foreach(
		fun(Worker) ->
			ok = ar_webhook_worker:cast_webhook(Worker, {block, Block})
		end,
		Workers
	),
	Workers.

new_transaction(Workers, TX) ->
	lists:foreach(
		fun(Worker) ->
			ok = ar_webhook_worker:cast_webhook(Worker, {transaction, TX})
		end,
		Workers
	),
	Workers.
