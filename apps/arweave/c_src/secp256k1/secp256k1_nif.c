#include <secp256k1.h>

#include "../ar_nif.h"
#include "./fill_random.h"

#define SECP256K1_PUBKEY_UNCOMPRESSED_SIZE 65
#define SECP256K1_PUBKEY_COMPRESSED_SIZE 33
#define SECP256K1_PRIVKEY_SIZE 32
#define SECP256K1_CONTEXT_SEED_SIZE 32

static int secp256k1_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info) {
    return 0;
}

static ERL_NIF_TERM generate_key(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    unsigned char seed[SECP256K1_CONTEXT_SEED_SIZE];
    if (!fill_random(seed, sizeof(seed))) {
        return error_tuple(env, "Failed to generate random seed for context.");
    }
    secp256k1_context* ctx = secp256k1_context_create(SECP256K1_CONTEXT_NONE);
    if (!secp256k1_context_randomize(ctx, seed)) {
        return error_tuple(env, "Failed to randomize context.");
    }

    unsigned char privbytes[SECP256K1_PRIVKEY_SIZE];
    if (!fill_random(privbytes, sizeof(privbytes))) {
        return error_tuple(env, "Failed to generate random key.");
    }
    if (!secp256k1_ec_seckey_verify(ctx, privbytes)) {
        return error_tuple(env, "Generated secret secp256k1 key is invalid.");
    }

    secp256k1_pubkey pubkey;
    if(!secp256k1_ec_pubkey_create(ctx, &pubkey, privbytes)) {
        return error_tuple(env, "Failed to populate public key.");
    }

    unsigned char pubbytes[SECP256K1_PUBKEY_UNCOMPRESSED_SIZE];
    size_t l = sizeof(pubbytes);
    if (!secp256k1_ec_pubkey_serialize(ctx, pubbytes, &l, &pubkey, SECP256K1_EC_UNCOMPRESSED)) {
        return error_tuple(env, "Failed to serialize public key.");
    }

    ERL_NIF_TERM privkey_bin = make_output_binary(env, privbytes, SECP256K1_PRIVKEY_SIZE);
    ERL_NIF_TERM pubkey_bin = make_output_binary(env, pubbytes, SECP256K1_PUBKEY_UNCOMPRESSED_SIZE);
    return ok_tuple2(env, privkey_bin, pubkey_bin);
}
