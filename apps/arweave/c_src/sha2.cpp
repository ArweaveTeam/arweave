#include <cryptopp/sha.h>
#include <cryptopp/hex.h>
#include <cryptopp/filters.h>
#include <cryptopp/cryptlib.h>

#include "sha2.h"

void sha2_p1(unsigned char* output, const unsigned char* input1, const size_t input1_size) {
	CryptoPP::SHA256 sha256;
	sha256.Update(input1, input1_size);
	sha256.Final(output);
}
void sha2_p2(unsigned char* output, const unsigned char* input1, const size_t input1_size, const unsigned char* input2, const size_t input2_size) {
	CryptoPP::SHA256 sha256;
	sha256.Update(input1, input1_size);
	sha256.Update(input2, input2_size);
	sha256.Final(output);
}
