#include <openssl/sha.h>

#include "feistel_msgsize_key_cipher.h"

// NOTE feistel_encrypt_block/feistel_decrypt_block with less than 2 blocks have no sense

void feistel_hash(const unsigned char *in_r, const unsigned char *in_k, unsigned char *out) {
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, in_r, 32);
	SHA256_Update(&sha256, in_k, 32);
	SHA256_Final(out, &sha256);
}

// size_t key_len, 
void feistel_encrypt_block(const unsigned char *in_left, const unsigned char *in_right, const unsigned char *in_key, unsigned char *out_left, unsigned char *out_right) {
	// size_t round_count = key_len / FEISTEL_BLOCK_LENGTH;

	// unsigned char temp;
	unsigned char key_hash[FEISTEL_BLOCK_LENGTH];
	unsigned char left[FEISTEL_BLOCK_LENGTH];
	unsigned char right[FEISTEL_BLOCK_LENGTH];
	const unsigned char *key = in_key;

	feistel_hash(in_right, key, key_hash);
	key += FEISTEL_BLOCK_LENGTH;
	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
		// temp = in_left[j] ^ key_hash[j];
		right[j] = in_left[j] ^ key_hash[j];
		left[j] = in_right[j];
		// right[j] = temp;
	}

	// NOTE will be unused by arweave
	// for (size_t i = 1; i < round_count - 1; i++) {
	// 	feistel_hash(right, key, key_hash);
	// 	key += FEISTEL_BLOCK_LENGTH;
	// 	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
	// 		temp = left[j] ^ key_hash[j];
	// 		left[j] = right[j];
	// 		right[j] = temp;
	// 	}
	// }

	feistel_hash(right, key, key_hash);
	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
		// temp = left[j] ^ key_hash[j];
		out_right[j] = left[j] ^ key_hash[j];
		out_left[j] = right[j];
		// out_right[j] = temp;
	}
}

void feistel_decrypt_block(const unsigned char *in_left, const unsigned char *in_right, const unsigned char *in_key, unsigned char *out_left, unsigned char *out_right) {
	// size_t round_count = key_len / FEISTEL_BLOCK_LENGTH;

	// unsigned char temp;
	unsigned char key_hash[FEISTEL_BLOCK_LENGTH];
	unsigned char left[FEISTEL_BLOCK_LENGTH];
	unsigned char right[FEISTEL_BLOCK_LENGTH];
	// const unsigned char *key = in_key + FEISTEL_BLOCK_LENGTH + 2*FEISTEL_BLOCK_LENGTH*(round_count - 1);
	const unsigned char *key = in_key + FEISTEL_BLOCK_LENGTH;

	feistel_hash(in_left, key, key_hash);
	key -= FEISTEL_BLOCK_LENGTH;
	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
		// temp = in_right[j] ^ key_hash[j];
		left[j] = in_right[j] ^ key_hash[j];
		right[j] = in_left[j];
		// left[j] = temp;
	}

	// NOTE will be unused by arweave
	// for (size_t i = 1; i < round_count - 1; i++) {
	// 	feistel_hash(left, key, key_hash);
	// 	key -= FEISTEL_BLOCK_LENGTH;
	// 	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
	// 		temp = right[j] ^ key_hash[j];
	// 		right[j] = left[j];
	// 		left[j] = temp;
	// 	}
	// }

	feistel_hash(left, key, key_hash);
	for(int j = 0; j < FEISTEL_BLOCK_LENGTH; j++) {
		// temp = right[j] ^ key_hash[j];
		out_left[j] = right[j] ^ key_hash[j];
		out_right[j] = left[j];
		// out_left[j] = temp;
	}
}

// feistel_encrypt accepts padded message with 2*FEISTEL_BLOCK_LENGTH = 64 bytes
// in_key_length == plaintext_len
// CBC
void feistel_encrypt(const unsigned char *plaintext, const size_t plaintext_len, const unsigned char *in_key, unsigned char *ciphertext) {
	size_t block_count = plaintext_len / (2*FEISTEL_BLOCK_LENGTH);
	unsigned char feed_key[2*FEISTEL_BLOCK_LENGTH] = {0};

	const unsigned char *in = plaintext;
	unsigned char *out = ciphertext;
	const unsigned char *key = in_key;

	feistel_encrypt_block(in, in + FEISTEL_BLOCK_LENGTH, key, out, out + FEISTEL_BLOCK_LENGTH);
	in  += 2*FEISTEL_BLOCK_LENGTH;
	key += 2*FEISTEL_BLOCK_LENGTH;

	for(size_t i = 1; i < block_count; i++) {
		for(int j = 0; j < 2*FEISTEL_BLOCK_LENGTH; j++) {
			feed_key[j] = key[j] ^ out[j];
		}
		out += 2*FEISTEL_BLOCK_LENGTH;

		feistel_encrypt_block(in, in + FEISTEL_BLOCK_LENGTH, feed_key, out, out + FEISTEL_BLOCK_LENGTH);
		in  += 2*FEISTEL_BLOCK_LENGTH;
		key += 2*FEISTEL_BLOCK_LENGTH;
	}
}

void feistel_decrypt(const unsigned char *ciphertext, const size_t ciphertext_len, const unsigned char *in_key, unsigned char *plaintext) {
	size_t block_count = ciphertext_len / (2*FEISTEL_BLOCK_LENGTH);
	unsigned char feed_key[2*FEISTEL_BLOCK_LENGTH] = {0};

	const unsigned char *in = ciphertext + ciphertext_len - 2*FEISTEL_BLOCK_LENGTH;
	unsigned char *out = plaintext + ciphertext_len - 2*FEISTEL_BLOCK_LENGTH;
	const unsigned char *key = in_key + ciphertext_len - 2*FEISTEL_BLOCK_LENGTH;

	for(size_t i = 0; i < block_count-1; i++) {
		for(int j = 0; j < 2*FEISTEL_BLOCK_LENGTH; j++) {
			feed_key[j] = key[j] ^ in[j - 2*FEISTEL_BLOCK_LENGTH];
		}

		feistel_decrypt_block(in, in + FEISTEL_BLOCK_LENGTH, feed_key, out, out + FEISTEL_BLOCK_LENGTH);
		in  -= 2*FEISTEL_BLOCK_LENGTH;
		key -= 2*FEISTEL_BLOCK_LENGTH;
		out -= 2*FEISTEL_BLOCK_LENGTH;
	}

	feistel_decrypt_block(in, in + FEISTEL_BLOCK_LENGTH, key, out, out + FEISTEL_BLOCK_LENGTH);
}

