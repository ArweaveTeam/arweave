#include <string.h>

#include <openssl/rand.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>

#include <ar_nif.h>

#define SECP256K1_PUBKEY_UNCOMPRESSED_SIZE 65
#define SECP256K1_PUBKEY_COMPRESSED_SIZE 33
#define SECP256K1_SIGNATURE_COMPACT_SIZE 64
#define SECP256K1_SIGNATURE_RECOVERABLE_SIZE 65
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

static ERL_NIF_TERM sign_recoverable(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
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
	unsigned char signature_compact[SECP256K1_SIGNATURE_COMPACT_SIZE];
	unsigned char signature_recoverable[SECP256K1_SIGNATURE_RECOVERABLE_SIZE];
	int recid;
	secp256k1_ecdsa_recoverable_signature s;
	secp256k1_context* ctx = secp256k1_context_create(SECP256K1_CONTEXT_NONE);

	memcpy(digest, Digest.data, SECP256K1_DIGEST_SIZE);
	memcpy(privbytes, PrivateBytes.data, SECP256K1_PRIVKEY_SIZE);

	if (!secp256k1_ec_seckey_verify(ctx, privbytes)) {
		error = "secp256k1 key is invalid.";
		goto cleanup;
	}

	if (!RAND_priv_bytes(seed, sizeof(seed))) {
		error = "Failed to generate random seed for context.";
		goto cleanup;
	}

	if (!secp256k1_context_randomize(ctx, seed)) {
		error = "Failed to randomize context.";
		goto cleanup;
	}

	if(!secp256k1_ecdsa_sign_recoverable(ctx, &s, digest, privbytes, NULL, NULL)) {
		error = "Failed to create signature.";
		goto cleanup;
	}

	if(!secp256k1_ecdsa_recoverable_signature_serialize_compact(ctx, signature_compact, &recid, &s)) {
		error = "Failed to serialize signature.";
		goto cleanup;
	}
	memcpy(signature_recoverable, signature_compact, SECP256K1_SIGNATURE_COMPACT_SIZE);
	signature_recoverable[64] = (unsigned char)(recid);

	ERL_NIF_TERM signature_term = make_output_binary(env, signature_recoverable, SECP256K1_SIGNATURE_RECOVERABLE_SIZE);

cleanup:
	secp256k1_context_destroy(ctx);
	secure_erase(seed, sizeof(seed));
	secure_erase(privbytes, sizeof(privbytes));
	memset(signature_compact, 0, SECP256K1_SIGNATURE_COMPACT_SIZE);
	memset(signature_recoverable, 0, SECP256K1_SIGNATURE_RECOVERABLE_SIZE);

	if (error) {
		return error_tuple(env, error);
	}
	return ok_tuple(env, signature_term);
}

static ERL_NIF_TERM recover_pk_and_verify(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
	if (argc != 2) {
		return enif_make_badarg(env);
	}
	ErlNifBinary Digest, Signature;
	if (!enif_inspect_binary(env, argv[0], &Digest)) {
		return enif_make_badarg(env);
	}
	if (Digest.size != SECP256K1_DIGEST_SIZE) {
		return enif_make_badarg(env);
	}

	if (!enif_inspect_binary(env, argv[1], &Signature)) {
		return enif_make_badarg(env);
	}
	if (Signature.size != SECP256K1_SIGNATURE_RECOVERABLE_SIZE) {
		return enif_make_badarg(env);
	}

	char *error = NULL;
	unsigned char digest[SECP256K1_DIGEST_SIZE];
	unsigned char signature_recoverable[SECP256K1_SIGNATURE_RECOVERABLE_SIZE];
	unsigned char signature_compact[SECP256K1_SIGNATURE_COMPACT_SIZE];
	unsigned char pubbytes[SECP256K1_PUBKEY_COMPRESSED_SIZE];
	int recid;
	secp256k1_ecdsa_recoverable_signature rs;
	secp256k1_ecdsa_signature s;
	secp256k1_pubkey pubkey;

	memcpy(digest, Digest.data, SECP256K1_DIGEST_SIZE);
	memcpy(signature_recoverable, Signature.data, SECP256K1_SIGNATURE_RECOVERABLE_SIZE);

	memcpy(signature_compact, signature_recoverable, SECP256K1_SIGNATURE_COMPACT_SIZE);
	recid = (int)signature_recoverable[64];

	if (recid < 0 || recid > 3) {
		error = "Invalid signature recid. recid >= 0 && recid <= 3.";
		goto cleanup;
	}

	if (!secp256k1_ecdsa_recoverable_signature_parse_compact(secp256k1_context_static, &rs, signature_compact, recid)) {
		error = "Failed to deserialize/parse recoverable signature.";
		goto cleanup;
	}

	if (!secp256k1_ecdsa_recover(secp256k1_context_static, &pubkey, &rs, digest)) {
		error = "Failed to recover public key.";
		goto cleanup;
	}
	size_t l = SECP256K1_PUBKEY_COMPRESSED_SIZE;
	if (!secp256k1_ec_pubkey_serialize(secp256k1_context_static, pubbytes, &l, &pubkey, SECP256K1_EC_COMPRESSED)) {
		error = "Failed to serialize the recovered public key.";
		goto cleanup;
	}

	if (!secp256k1_ecdsa_recoverable_signature_convert(secp256k1_context_static, &s, &rs)) {
		error = "Failed to convert recoverable signature to compact signature.";
		goto cleanup;
	}

	// NOTE. https://github.com/bitcoin-core/secp256k1/blob/f79f46c70386c693ff4e7aef0b9e7923ba284e56/src/secp256k1.c#L461
	// Verify performs check for low-s
	int is_valid = secp256k1_ecdsa_verify(secp256k1_context_static, &s, digest, &pubkey);
	ERL_NIF_TERM pubkey_term = make_output_binary(env, pubbytes, SECP256K1_PUBKEY_COMPRESSED_SIZE);

cleanup:
	memset(digest, 0, SECP256K1_DIGEST_SIZE);
	memset(pubbytes, 0, SECP256K1_PUBKEY_COMPRESSED_SIZE);
	memset(signature_compact, 0, SECP256K1_SIGNATURE_COMPACT_SIZE);
	memset(signature_recoverable, 0, SECP256K1_SIGNATURE_RECOVERABLE_SIZE);

	if (error) {
		return error_tuple(env, error);
	}
	if (is_valid) {
		return ok_tuple2(env, enif_make_atom(env, "true"), pubkey_term);
	}
	return ok_tuple2(env, enif_make_atom(env, "false"), pubkey_term);
}

static ErlNifFunc nif_funcs[] = {
	{"sign_recoverable", 2, sign_recoverable},
	{"recover_pk_and_verify", 2, recover_pk_and_verify}
};

ERL_NIF_INIT(secp256k1_nif, nif_funcs, secp256k1_load, NULL, NULL, NULL)