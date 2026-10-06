-module(arweave_entropy).
-behaviour(application).
-export([start/2, stop/1, child_spec/0, generate/4, generate_entropies/2, generate_entropies/4, generate_entropy_keys/2, entropy_offsets/2, map_entropies/8]).
start(_Type, _Args) -> arweave_entropy_sup:start_link().
stop(_State) -> ok.
child_spec() -> arweave_entropy_lifecycle:child_spec().
generate(A, B, C, D) -> arweave_entropy_generation:generate(A, B, C, D).
generate_entropies(A, B) -> arweave_entropy_preparation:generate_entropies(A, B).
generate_entropies(A, B, C, D) -> arweave_entropy_preparation:generate_entropies(A, B, C, D).
generate_entropy_keys(A, B) -> arweave_entropy_preparation:generate_entropy_keys(A, B).
entropy_offsets(A, B) -> arweave_entropy_preparation:entropy_offsets(A, B).
map_entropies(A, B, C, D, E, F, G, H) -> arweave_entropy_preparation:map_entropies(A, B, C, D, E, F, G, H).
