#include <openssl/rand.h>
#include <secp256k1.h>
#include <ar_nif.h>
#include <memory.h>

#define SECP256K1_PUBKEY_UNCOMPRESSED_SIZE 65
#define SECP256K1_PUBKEY_COMPRESSED_SIZE 33
#define SECP256K1_SIGNATURE_SIZE 64
#define SECP256K1_PRIVKEY_SIZE 32
#define SECP256K1_CONTEXT_SEED_SIZE 32
#define SECP256K1_DIGEST_SIZE 32

static int secp256k1_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info) {
	return 0;
}

#if defined(_MSC_VER)
// For SecureZeroMemory
#include <Windows.h>
#endif
/* Cleanses memory to prevent leaking sensitive info. Won't be optimized out. */
static void secure_erase(void *ptr, size_t len) {
#if defined(_MSC_VER)
	/* SecureZeroMemory is guaranteed not to be optimized out by MSVC. */
	SecureZeroMemory(ptr, len);
#elif defined(__GNUC__)
	/* We use a memory barrier that scares the compiler away from optimizing out the memset.
	 *
	 * Quoting Adam Langley <agl@google.com> in commit ad1907fe73334d6c696c8539646c21b11178f20f
	 * in BoringSSL (ISC License):
	 *	As best as we can tell, this is sufficient to break any optimisations that
	 *	might try to eliminate "superfluous" memsets.
	 * This method used in memzero_explicit() the Linux kernel, too. Its advantage is that it is
	 * pretty efficient, because the compiler can still implement the memset() efficiently,
	 * just not remove it entirely. See "Dead Store Elimination (Still) Considered Harmful" by
	 * Yang et al. (USENIX Security 2017) for more background.
	 */
	memset(ptr, 0, len);
	__asm__ __volatile__("" : : "r"(ptr) : "memory");
#else
	void *(*volatile const volatile_memset)(void *, int, size_t) = memset;
	volatile_memset(ptr, 0, len);
#endif
}

static ERL_NIF_TERM sign(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 2) {
		return enif_make_badarg(env);
	}
	ErlNifBinary Digest, PrivateBytes;
	if (!enif_inspect_binary(env, argv[0], &Digest)) {
		return enif_make_badarg(env);
	}
	if (Digest.size != SECP256K1_DIGEST_SIZE) {
		return enif_make_badarg(env);
	}

	if (!enif_inspect_binary(env, argv[1], &PrivateBytes)) {
		return enif_make_badarg(env);
	}
	if (PrivateBytes.size != SECP256K1_PRIVKEY_SIZE) {
		return enif_make_badarg(env);
	}

	char *error = NULL;
	unsigned char seed[SECP256K1_CONTEXT_SEED_SIZE];
	unsigned char digest[SECP256K1_DIGEST_SIZE];
	unsigned char privbytes[SECP256K1_PRIVKEY_SIZE];
	unsigned char signature[SECP256K1_SIGNATURE_SIZE];
	secp256k1_ecdsa_signature s;
	secp256k1_context* ctx = secp256k1_context_create(SECP256K1_CONTEXT_NONE);

	memcpy(digest, Digest.data, SECP256K1_DIGEST_SIZE);
	memcpy(privbytes, PrivateBytes.data, SECP256K1_PRIVKEY_SIZE);

	if (!RAND_priv_bytes(seed, sizeof(seed))) {
		error = "Failed to generate random seed for context.";
		goto cleanup;
	}

	if (!secp256k1_context_randomize(ctx, seed)) {
		error = "Failed to randomize context.";
		goto cleanup;
	}

	if(!secp256k1_ecdsa_sign(ctx, &s, digest, privbytes, NULL, NULL)) {
		error = "Failed to create signature.";
		goto cleanup;
	}

	if(!secp256k1_ecdsa_signature_serialize_compact(ctx, signature, &s)) {
		error = "Failed to serialize signature.";
		goto cleanup;
	}

	ERL_NIF_TERM signature_term = make_output_binary(env, signature, SECP256K1_SIGNATURE_SIZE);

cleanup:
	secp256k1_context_destroy(ctx);
	secure_erase(seed, sizeof(seed));
	secure_erase(privbytes, sizeof(privbytes));
	memset(signature, 0, SECP256K1_SIGNATURE_SIZE);

	if (error) {
		return error_tuple(env, error);
	}
	return ok_tuple(env, signature_term);
}

