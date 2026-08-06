#include <erl_nif.h>
#include <string.h>
#include <stdint.h>
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

#define VDF_MAX_CHECKPOINTS 1000000
#define VDF_MAX_SKIP_CHECKPOINTS 1000000
#define VDF_MAX_THREADS 4096

static int vdf_validate_common_params(int checkpointCount, int skipCheckpointCount, int hashingIterations)
{
	if (checkpointCount < 0 || checkpointCount > VDF_MAX_CHECKPOINTS) {
		return 0;
	}
	if (skipCheckpointCount < 0 || skipCheckpointCount > VDF_MAX_SKIP_CHECKPOINTS) {
		return 0;
	}
	/* OpenSSL reference path supports hashingIterations >= 1; fused ARM needs a fallback below 2 */
	if (hashingIterations < 1) {
		return 0;
	}
	return 1;
}

static int vdf_mul_size_checkpoint_output(int checkpointCount, size_t *out_size)
{
	if (checkpointCount < 0) {
		return 0;
	}
	size_t n = (size_t)checkpointCount;
	size_t sz = n * (size_t)VDF_SHA_HASH_SIZE;
	if (checkpointCount != 0 && sz / (size_t)VDF_SHA_HASH_SIZE != n) {
		return 0;
	}
	*out_size = sz;
	return 1;
}

static int vdf_parallel_verify_output_size(int checkpointCount, int skipCheckpointCount, size_t *out_size)
{
	if (checkpointCount < 0 || skipCheckpointCount < 0) {
		return 0;
	}
	size_t a = (size_t)checkpointCount + 1u;
	size_t b = (size_t)skipCheckpointCount + 1u;
	if (a > SIZE_MAX / b) {
		return 0;
	}
	size_t prod = a * b;
	if (prod > SIZE_MAX / (size_t)VDF_SHA_HASH_SIZE) {
		return 0;
	}
	*out_size = prod * (size_t)VDF_SHA_HASH_SIZE;
	return 1;
}

static int vdf_in_checkpoint_size_matches(int checkpointCount, size_t bin_size)
{
	size_t expected;
	if (!vdf_mul_size_checkpoint_output(checkpointCount, &expected)) {
		return 0;
	}
	return bin_size == expected;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
typedef void (*vdf_sha2_fn)(
	unsigned char* saltBuffer,
	unsigned char* seed,
	unsigned char* out,
	unsigned char* outCheckpoint,
	int checkpointCount,
	int skipCheckpointCount,
	int hashingIterations
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
	if (!vdf_validate_common_params(checkpointCount, skipCheckpointCount, hashingIterations)) {
		return enif_make_badarg(envPtr);
	}

	size_t outCheckpointSize;
	if (!vdf_mul_size_checkpoint_output(checkpointCount, &outCheckpointSize)) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}
static ERL_NIF_TERM vdf_sha2_fused_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
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
	if (!vdf_validate_common_params(checkpointCount, skipCheckpointCount, hashingIterations)) {
		return enif_make_badarg(envPtr);
	}

	size_t outCheckpointSize;
	if (!vdf_mul_size_checkpoint_output(checkpointCount, &outCheckpointSize)) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
	ERL_NIF_TERM outputTermCheckpoint;
	unsigned char* outCheckpoint = enif_make_new_binary(envPtr, outCheckpointSize, &outputTermCheckpoint);
	vdf_sha2_fused_ptr(Salt.data, Seed.data, temp_result, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);

	return ok_tuple2(envPtr, make_output_binary(envPtr, temp_result, VDF_SHA_HASH_SIZE), outputTermCheckpoint);
}
static ERL_NIF_TERM vdf_sha2_hiopt_nif(ErlNifEnv* envPtr, int argc, const ERL_NIF_TERM argv[])
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
	if (!vdf_validate_common_params(checkpointCount, skipCheckpointCount, hashingIterations)) {
		return enif_make_badarg(envPtr);
	}

	size_t outCheckpointSize;
	if (!vdf_mul_size_checkpoint_output(checkpointCount, &outCheckpointSize)) {
		return enif_make_badarg(envPtr);
	}

	unsigned char temp_result[VDF_SHA_HASH_SIZE];
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
	if (!vdf_validate_common_params(checkpointCount, skipCheckpointCount, hashingIterations)) {
		return enif_make_badarg(envPtr);
	}
	if (!vdf_in_checkpoint_size_matches(checkpointCount, InCheckpoint.size)) {
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
	if (maxThreadCount < 1 || maxThreadCount > VDF_MAX_THREADS) {
		return enif_make_badarg(envPtr);
	}

	size_t outCheckpointSize;
	if (!vdf_parallel_verify_output_size(checkpointCount, skipCheckpointCount, &outCheckpointSize)) {
		return enif_make_badarg(envPtr);
	}

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
