////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
static ERL_NIF_TERM vdf_sha2_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary Salt, Seed;
	int checkpointCount;
	int skipCheckpointCount;
	int hashingIterations;

	if (argc != 5) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[0], &Salt)) {
		return enif_make_badarg(envPtr);
	}
	if (Salt.size != SALT_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[1], &Seed)) {
		return enif_make_badarg(envPtr);
	}
	if (Seed.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &skipCheckpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &hashingIterations)) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	size_t outCheckpointSize = VDF_SHA_HASH_SIZE*checkpointCount;
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}

static ERL_NIF_TERM vdf_parallel_sha_verify_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	ErlNifBinary Salt, Seed, InCheckpoint, InRes;
	int checkpointCount;
	int skipCheckpointCount;
	int hashingIterations;
	int maxThreadCount;

	if (argc != 8) {
		return enif_make_badarg(envPtr);
	}

	// copypasted from vdf_sha2_nif
	if (!enif_inspect_binary(envPtr, argv[0], &Salt)) {
		return enif_make_badarg(envPtr);
	}
	if (Salt.size != SALT_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[1], &Seed)) {
		return enif_make_badarg(envPtr);
	}
	if (Seed.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &skipCheckpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &hashingIterations)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[5], &InCheckpoint)) {
		return enif_make_badarg(envPtr);
	}
	if (InCheckpoint.size != checkpointCount*VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[6], &InRes)) {
		return enif_make_badarg(envPtr);
	}
	if (InRes.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[7], &maxThreadCount)) {
		return enif_make_badarg(envPtr);
	}
	if (maxThreadCount < 1) {
		return enif_make_badarg(envPtr);
	}

	// NOTE last paramemter will be array later
	size_t outCheckpointSize = VDF_SHA_HASH_SIZE*(1+checkpointCount)*(1+skipCheckpointCount);
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	bool res = vdf_parallel_sha_verify(Salt.data, Seed.data, checkpointCount, skipCheckpointCount, hashingIterations, InRes.data, InCheckpoint.data, outCheckpoint, maxThreadCount);
	// TODO return all checkpoints
	if (!res) {
		return error(envPtr, "verification failed");
	}

	return ok_tuple(envPtr, outputTermCheckpoint);
}

static ERL_NIF_TERM vdf_parallel_sha_verify_with_reset_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	ErlNifBinary Salt, Seed, InCheckpoint, InRes, ResetSalt, ResetSeed;
	int checkpointCount;
	int skipCheckpointCount;
	int hashingIterations;
	int maxThreadCount;

	if (argc != 10) {
		return enif_make_badarg(envPtr);
	}

	// copypasted from vdf_sha2_nif
	if (!enif_inspect_binary(envPtr, argv[0], &Salt)) {
		return enif_make_badarg(envPtr);
	}
	if (Salt.size != SALT_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[1], &Seed)) {
		return enif_make_badarg(envPtr);
	}
	if (Seed.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[3], &skipCheckpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[4], &hashingIterations)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[5], &InCheckpoint)) {
		return enif_make_badarg(envPtr);
	}
	if (InCheckpoint.size != checkpointCount*VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[6], &InRes)) {
		return enif_make_badarg(envPtr);
	}
	if (InRes.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[7], &ResetSalt)) {
		return enif_make_badarg(envPtr);
	}
	if (ResetSalt.size != 32) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[8], &ResetSeed)) {
		return enif_make_badarg(envPtr);
	}
	if (ResetSeed.size != VDF_SHA_HASH_SIZE) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_int(envPtr, argv[9], &maxThreadCount)) {
		return enif_make_badarg(envPtr);
	}
	if (maxThreadCount < 1) {
		return enif_make_badarg(envPtr);
	}

	// NOTE last paramemter will be array later
	size_t outCheckpointSize = VDF_SHA_HASH_SIZE*(1+checkpointCount)*(1+skipCheckpointCount);
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	bool res = vdf_parallel_sha_verify_with_reset(Salt.data, Seed.data, checkpointCount, skipCheckpointCount, hashingIterations, InRes.data, InCheckpoint.data, outCheckpoint, ResetSalt.data, ResetSeed.data, maxThreadCount);
	// TODO return all checkpoints
	if (!res) {
		return error(envPtr, "verification failed");
	}

	return ok_tuple(envPtr, outputTermCheckpoint);
}
