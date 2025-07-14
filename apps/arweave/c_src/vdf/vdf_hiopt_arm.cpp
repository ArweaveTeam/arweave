#include <cstdint>
#include <cstring>
#include "vdf.h"

#if defined(__aarch64__) || defined(__arm__)

extern "C" {
	void sha256_block_vdf_order(unsigned int* cth, const void* in, size_t diff_num);
	void reverse_endianness_asm(const uint32_t h[8], uint8_t* md) {
		uint64_t h1, h2;
		__asm__ volatile (
			"LDP %[t1], %[t2], [%[in]], #16\n\t"
			"REV32 %[t1], %[t1]\n\t"
			"REV32 %[t2], %[t2]\n\t"
			"STP %[t1], %[t2], [%[out]], #16\n\t"

			"LDP %[t1], %[t2], [%[in]]\n\t"
			"REV32 %[t1], %[t1]\n\t"
			"REV32 %[t2], %[t2]\n\t"
			"STP %[t1], %[t2], [%[out]]\n\t"

			//
			: [t1] "+r" (h1),
			[t2] "+r" (h2),
			[in] "+r" (h),	
			[out] "+r" (md)	
			: 
			: "memory"
			);
	}
}

//sha256 h0-h7
unsigned int H07[8] = { 0x6a09e667U,0xbb67ae85U,0x3c6ef372U,0xa54ff53aU,
	0x510e527fU,0x9b05688cU,0x1f83d9abU,0x5be0cd19U };

////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
// NOTE saltBuffer is mutable in progress
void _vdf_sha2_hiopt_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	//unsigned char tempOut[VDF_SHA_HASH_SIZE];
	// 2 different branches for different optimisation cases

	unsigned int sha256[8];
	//one sha256 block
	unsigned char inBuffer[64];

	if (skipCheckpointCount == 0) {
		for (int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn = checkpointIdx == 0 ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE * (checkpointIdx - 1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out : (outCheckpoint + VDF_SHA_HASH_SIZE * checkpointIdx);

			memcpy(sha256, H07, 32);
			reverse_endianness_asm((uint32_t*)saltBuffer, inBuffer);
			reverse_endianness_asm((uint32_t*)locIn, &inBuffer[32]);
			
			sha256_block_vdf_order(sha256, inBuffer, hashingIterations);
			reverse_endianness_asm(sha256, locOut);
			long_add(saltBuffer, 1);
		}
	}
	else {
		for (int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn = checkpointIdx == 0 ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE * (checkpointIdx - 1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out : (outCheckpoint + VDF_SHA_HASH_SIZE * checkpointIdx);
			
			// 1 skip on start
			memcpy(sha256, H07, 32);
			reverse_endianness_asm((uint32_t*)saltBuffer, inBuffer);
			reverse_endianness_asm((uint32_t*)locIn, &inBuffer[32]);
			
			sha256_block_vdf_order(sha256, inBuffer, hashingIterations);
			memcpy(&inBuffer[32], sha256, 32);
			long_add(saltBuffer, 1);
			reverse_endianness_asm((uint32_t*)saltBuffer, inBuffer);
			for (int j = 1; j < skipCheckpointCount; j++) {
				// no skips
				memcpy(sha256, H07, 32);
				sha256_block_vdf_order(sha256, inBuffer, hashingIterations);
				memcpy(&inBuffer[32], sha256, 32);
				long_add(saltBuffer, 1);
				reverse_endianness_asm((uint32_t*)saltBuffer, inBuffer);
			}
			// 1 skip on end
			memcpy(sha256, H07, 32);
			sha256_block_vdf_order(sha256, inBuffer, hashingIterations);
			reverse_endianness_asm(sha256, locOut);
			long_add(saltBuffer, 1);
		}
	}
}

// use
//   unsigned char out[VDF_SHA_HASH_SIZE];
//   unsigned char* outCheckpoint = (unsigned char*)malloc(checkpointCount*VDF_SHA_HASH_SIZE);
//   free(outCheckpoint);
// for call
void vdf_sha2_hiopt_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char saltBufferStack[SALT_SIZE];
	// ensure 1 L1 cache page used
	// no access to heap, except of 0-iteration
	memcpy(saltBufferStack, saltBuffer, SALT_SIZE);

	_vdf_sha2_hiopt_arm(saltBufferStack, seed, out, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);
}


#endif