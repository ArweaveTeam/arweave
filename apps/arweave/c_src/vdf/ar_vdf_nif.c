#include <erl_nif.h>
#include <string.h>
#include <openssl/sha.h>
#include <ar_nif.h>
#include "vdf.h"

#if defined(__x86_64__) || defined(__amd64__) || defined(__i386__)
#include <cpuid.h>
#endif
#if defined(__linux__)
	#include <sys/auxv.h>
#endif
#if defined(__APPLE__)
	#include <sys/types.h>
	#include <sys/sysctl.h>
#endif

////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
typedef void (*vdf_sha2_exp_fn)(
	unsigned char* saltBuffer,
	unsigned char* seed,
	unsigned char* out,
	unsigned char* outCheckpoint,
	int checkpointCount,
	int skipCheckpointCount,
	int hashingIterations
);
static vdf_sha2_exp_fn vdf_sha2_exp_ptr = NULL;

static int vdf_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info) {
	#if defined(__x86_64__) || defined(__i386__)
		{
			unsigned int eax, ebx, ecx, edx;
			// leaf 7, subleaf 0
			if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx) && (ebx & (1u << 29))) {
				vdf_sha2_exp_ptr = vdf_sha2_exp_x86;
				return 0;
			}
		}
	#endif

	#if defined(__aarch64__) || defined(__arm__)
		#if defined(__linux__)
			if (getauxval(AT_HWCAP) & HWCAP_SHA2) {
				vdf_sha2_exp_ptr = vdf_sha2_exp_arm;
				return 0;
			}
		#elif defined(__APPLE__)
			{
				int val = 0; size_t len = sizeof(val);
				if (sysctlbyname("hw.optional.arm.FEAT_SHA256", &val, &len, NULL, 0) == 0 && val != 0) {
					vdf_sha2_exp_ptr = vdf_sha2_exp_arm;
					return 0;
				}
			}
		#endif
	#endif

	vdf_sha2_exp_ptr = vdf_sha2;
	return 0;
}
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
static ERL_NIF_TERM vdf_sha2_exp_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
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
	vdf_sha2_exp_ptr(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
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
	unsigned char* outCheckpoint = enif_make_new_binary(
		envPtr, outCheckpointSize, &outputTermCheckpoint);
	bool res = vdf_parallel_sha_verify_with_reset(
		Salt.data, Seed.data, checkpointCount, skipCheckpointCount, hashingIterations,
		InRes.data, InCheckpoint.data, outCheckpoint, ResetSalt.data, ResetSeed.data,
		maxThreadCount);
	// TODO return all checkpoints
	if (!res) {
		return error_tuple(envPtr, "verification failed");
	}

	return ok_tuple(envPtr, outputTermCheckpoint);
}

static ErlNifFunc nif_funcs[] = {
	{"vdf_sha2_nif", 5, vdf_sha2_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_sha2_exp_nif", 5, vdf_sha2_exp_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_parallel_sha_verify_with_reset_nif", 10, vdf_parallel_sha_verify_with_reset_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_vdf_nif, nif_funcs, vdf_load, NULL, NULL, NULL);
