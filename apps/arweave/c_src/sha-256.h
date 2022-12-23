#include <openssl/sha.h>

// -1 warning
void calc_sha_256(uint8_t hash[32], const void *input, size_t len);

void calc_sha_256(uint8_t hash[32], const void *input, size_t len) {
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, input, len);
  SHA256_Final(hash, &sha256);
}
