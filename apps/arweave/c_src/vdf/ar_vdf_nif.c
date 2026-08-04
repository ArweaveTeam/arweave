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

// The number of checkpoints skipped in between two reported checkpoints. Unlike
// checkpointCount - which vdf_parallel_sha_verify_with_reset_nif pins to the size of the
// caller's InCheckpoint binary - nothing bounds this one, and it multiplies both the hashing
// work and the size of the verify output buffer:
//   VDF_SHA_HASH_SIZE * (1 + checkpointCount) * (1 + skipCheckpointCount)
// enif_make_new_binary aborts the emulator instead of returning NULL when that product
// exceeds what can be allocated. ar_vdf:compute/3 passes 0 and ar_vdf:verify/8 passes
// ?VDF_CHECKPOINT_COUNT_IN_STEP - 1 = 24, so this leaves ample headroom.
#define MAX_SKIP_CHECKPOINT_COUNT 1024

// The implementations do not agree below 2 iterations. The reference implementation
// (_vdf_sha2 in vdf.cpp) hashes an unconditional first block - and, without skips, an
// unconditional last one - around its inner loop, so it never runs fewer than 2 rounds per
// checkpoint; the fused/hiopt implementations run exactly hashingIterations and return the
// seed unhashed at 0. Rather than touch the optimised implementations, reject the range
// where they disagree: it is far below any production VDF difficulty.
#define MIN_HASHING_ITERATIONS 2

////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
typedef void (*vdf_sha2_fn)(
	unsigned char* saltBuffer,
	unsigned char* seed,
	unsigned char* out,
	unsigned char* outCheckpoint,
	unsigned int checkpointCount,
	unsigned int skipCheckpointCount,
	unsigned int hashingIterations
);
static vdf_sha2_fn vdf_sha2_fused_ptr = NULL;
static vdf_sha2_fn vdf_sha2_hiopt_ptr = NULL;

static int vdf_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info) {
	#if defined(__x86_64__) || defined(__i386__)
		{
			unsigned int eax, ebx, ecx, edx;
			// leaf 7, subleaf 0
			if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx) && (ebx & (1u << 29))) {
				printf("VDF arch x86\n");
				vdf_sha2_fused_ptr = vdf_sha2_fused_x86;
				vdf_sha2_hiopt_ptr = vdf_sha2_fused_x86; // fallback
				return 0;
			}
		}
	#endif

	#if defined(__aarch64__) || defined(__arm__)
		#if defined(__linux__)
			if (getauxval(AT_HWCAP) & HWCAP_SHA2) {
				printf("VDF arch ARM linux\n");
				vdf_sha2_fused_ptr = vdf_sha2_fused_arm;
				vdf_sha2_hiopt_ptr = vdf_sha2_hiopt_arm;
				return 0;
			}
		#elif defined(__APPLE__)
			{
				int val = 0; size_t len = sizeof(val);
				if (sysctlbyname("hw.optional.arm.FEAT_SHA256", &val, &len, NULL, 0) == 0 && val != 0) {
					printf("VDF arch ARM macos\n");
					vdf_sha2_fused_ptr = vdf_sha2_fused_arm;
					vdf_sha2_hiopt_ptr = vdf_sha2_hiopt_arm;
					return 0;
				}
			}
		#endif
	#endif

	printf("VDF arch unknown\n");
	vdf_sha2_fused_ptr = vdf_sha2;
	vdf_sha2_hiopt_ptr = vdf_sha2;
	return 0;
}
static ERL_NIF_TERM vdf_sha2_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary Salt, Seed;
	unsigned int checkpointCount;
	unsigned int skipCheckpointCount;
	unsigned int hashingIterations;

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
	if (!enif_get_uint(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[3], &skipCheckpointCount) ||
		skipCheckpointCount > MAX_SKIP_CHECKPOINT_COUNT) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[4], &hashingIterations) ||
		hashingIterations < MIN_HASHING_ITERATIONS) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	size_t outCheckpointSize = (size_t)VDF_SHA_HASH_SIZE*checkpointCount;
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}
static ERL_NIF_TERM vdf_sha2_fused_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary Salt, Seed;
	unsigned int checkpointCount;
	unsigned int skipCheckpointCount;
	unsigned int hashingIterations;

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
	if (!enif_get_uint(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[3], &skipCheckpointCount) ||
		skipCheckpointCount > MAX_SKIP_CHECKPOINT_COUNT) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[4], &hashingIterations) ||
		hashingIterations < MIN_HASHING_ITERATIONS) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	size_t outCheckpointSize = (size_t)VDF_SHA_HASH_SIZE*checkpointCount;
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2_fused_ptr(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}
static ERL_NIF_TERM vdf_sha2_hiopt_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary Salt, Seed;
	unsigned int checkpointCount;
	unsigned int skipCheckpointCount;
	unsigned int hashingIterations;

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
	if (!enif_get_uint(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[3], &skipCheckpointCount) ||
		skipCheckpointCount > MAX_SKIP_CHECKPOINT_COUNT) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[4], &hashingIterations) ||
		hashingIterations < MIN_HASHING_ITERATIONS) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	size_t outCheckpointSize = (size_t)VDF_SHA_HASH_SIZE*checkpointCount;
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2_hiopt_ptr(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}

static ERL_NIF_TERM vdf_parallel_sha_verify_with_reset_nif(
	ErlNifEnv* envPtr,
	int argc,
	const ERL_NIF_TERM argv[]
) {
	ErlNifBinary Salt, Seed, InCheckpoint, InRes, ResetSalt, ResetSeed;
	unsigned int checkpointCount;
	unsigned int skipCheckpointCount;
	unsigned int hashingIterations;
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
	if (!enif_get_uint(envPtr, argv[2], &checkpointCount)) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[3], &skipCheckpointCount) ||
		skipCheckpointCount > MAX_SKIP_CHECKPOINT_COUNT) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_get_uint(envPtr, argv[4], &hashingIterations) ||
		hashingIterations < MIN_HASHING_ITERATIONS) {
		return enif_make_badarg(envPtr);
	}
	if (!enif_inspect_binary(envPtr, argv[5], &InCheckpoint)) {
		return enif_make_badarg(envPtr);
	}
	if (InCheckpoint.size != (size_t)checkpointCount*VDF_SHA_HASH_SIZE) {
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

	size_t outCheckpointSize =
		(size_t)VDF_SHA_HASH_SIZE*(1+(size_t)checkpointCount)*(1+(size_t)skipCheckpointCount);
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(
		envPtr, outCheckpointSize, &outputTermCheckpoint);
	bool res = vdf_parallel_sha_verify_with_reset(
		Salt.data, Seed.data, checkpointCount, skipCheckpointCount, hashingIterations,
		InRes.data, InCheckpoint.data, outCheckpoint, ResetSalt.data, ResetSeed.data,
		maxThreadCount);
	if (!res) {
		return error_tuple(envPtr, "verification failed");
	}

	return ok_tuple(envPtr, outputTermCheckpoint);
}

static ErlNifFunc nif_funcs[] = {
	{"vdf_sha2_nif", 5, vdf_sha2_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_sha2_fused_nif", 5, vdf_sha2_fused_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_sha2_hiopt_nif", 5, vdf_sha2_hiopt_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"vdf_parallel_sha_verify_with_reset_nif", 10, vdf_parallel_sha_verify_with_reset_nif,
		ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(ar_vdf_nif, nif_funcs, vdf_load, NULL, NULL, NULL);
