#ifndef FEISTEL_MSGSIZE_KEY_CIPHER_H
#define FEISTEL_MSGSIZE_KEY_CIPHER_H


#define FEISTEL_BLOCK_LENGTH 32

#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif

bool feistel_encrypt(const unsigned char *plaintext, const size_t plaintext_len, const unsigned char *key, unsigned char *ciphertext);
bool feistel_decrypt(const unsigned char *ciphertext, const size_t ciphertext_len, const unsigned char *key, unsigned char *plaintext);

#if defined(__cplusplus)
}
#endif

#endif // FEISTEL_MSGSIZE_KEY_CIPHER_H
