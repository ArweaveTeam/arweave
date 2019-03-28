%% The definition of illicit data signature. Contains all of the information
%% required to determine whether a file contains illicit data.
-record(sig, {
	name, % The name of the signature, sometimes including a type tag.
	type, % Currently either hash or binary.
	data % Either a binary_sig or hash_sig.
}).

%% A record that stores md5-sig-specific information.
-record(hash_sig, {
	hash,
	size
}).

%% A record that stores hex- or plain text-sig-specific information.
-record(binary_sig, {
	target_type,
	offset,
	binary
}).
