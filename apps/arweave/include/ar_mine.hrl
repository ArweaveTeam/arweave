%% The difficuly a hash pointing to a candidate SPoA has to satisfy.
-define(SPORA_SLOW_HASH_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_2_4()
	},
	case Forks of
		{Fork_2_4} when Height >= Fork_2_4 ->
			0
	end
end()).

%% The size of the search space - a share of the weave randomly sampled
%% at every block. The solution must belong to the search space.
-define(SPORA_SEARCH_SPACE_SIZE(Height, SearchSpaceUpperBound), fun() ->
	Forks = {
		ar_fork:height_2_4()
	},
	case Forks of
		{Fork_2_4} when Height >= Fork_2_4 ->
			SearchSpaceUpperBound div 10 % 10% of the weave.
	end
end()).

%% The number of contiguous subspaces of the search space, a roughly equal
%% share of the search space is sampled from each of the subspaces.
-define(SPORA_SEARCH_SPACE_SUBSPACES_COUNT(Height), fun() ->
	Forks = {
		ar_fork:height_2_4()
	},
	case Forks of
		{Fork_2_4} when Height >= Fork_2_4 ->
			1024
	end
end()).

%% The minimum difficulty allowed.
-define(SPORA_MIN_DIFFICULTY(Height), fun() ->
	Forks = {
		ar_fork:height_2_4()
	},
	case Forks of
		{Fork_2_4} when Height >= Fork_2_4 ->
			21
	end
end()).

%% Recall bytes are only picked from the subspace up to the size
%% of the weave at the block of the depth defined by this constant.
-define(SEARCH_SPACE_UPPER_BOUND_DEPTH(Height), fun() ->
	Forks = {
		ar_fork:height_2_4()
	},
	case Forks of
		{Fork_2_4} when Height >= Fork_2_4 ->
			50
	end
end()).
