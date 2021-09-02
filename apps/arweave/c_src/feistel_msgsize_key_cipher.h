#define FEISTEL_BLOCK_LENGTH 32

#if defined(__cplusplus)
extern "C" {
#endif

void feistel_encrypt(const unsigned char *plaintext, const size_t plaintext_len, const unsigned char *key, unsigned char *ciphertext);
void feistel_decrypt(const unsigned char *ciphertext, const size_t ciphertext_len, const unsigned char *key, unsigned char *plaintext);

#if defined(__cplusplus)
}
#endif
