#include <cassert>
#include <openssl/sha.h>
#include "crc32.h"
#include "randomx_squared.h"
#include "feistel_msgsize_key_cipher.h"

// imports from randomx
#include "vm_compiled.hpp"
#include "blake2/blake2.h"

extern "C" {

	void _rsp_mix_entropy_near(
		const unsigned char *inEntropy,
		unsigned char *outEntropy,
		const size_t entropySize
	) {
		// NOTE we can't use _mm_crc32_u64, because it output only final 32-bit result
		// NOTE commented variant is more readable but unoptimized
		unsigned int state = ~0;
		// unsigned int state = 0;
		const unsigned int *inEntropyPtr = (const unsigned int*)inEntropy;
		unsigned int *outEntropyPtr = (unsigned int*)outEntropy;
		for(size_t i=0;i<entropySize;i+=8) {
			//state = crc32(~state, *inEntropyPtr);
			//*outEntropyPtr = *inEntropyPtr ^ ~state;

			state = ~crc32(state, *inEntropyPtr);
			*outEntropyPtr = *inEntropyPtr ^ state;
			inEntropyPtr++;
			outEntropyPtr++;
			*outEntropyPtr = *inEntropyPtr;
			inEntropyPtr++;
			outEntropyPtr++;
		}

		// keep state
		// reset output entropy from start
		outEntropyPtr = (unsigned int*)outEntropy;
		outEntropyPtr++;
		// take input from output now
		inEntropyPtr = outEntropyPtr;
		// Note it's optimizeable now with only 1 pointer, but we will reduce readability later
		for(size_t i=0;i<entropySize;i+=8) {
			//state = crc32(~state, *inEntropyPtr);
			//*outEntropyPtr = *inEntropyPtr ^ ~state;

			state = ~crc32(state, *inEntropyPtr);
			*outEntropyPtr = *inEntropyPtr ^ state;
			inEntropyPtr += 2;
			outEntropyPtr += 2;
		}
	}

	// Runs 1 RX2 round of programCount RandomX execs + 1 CRC mix on a single lane.
	// VM scratchpad is updated in place.
	void _rsp_exec_inplace(
		randomx_vm* machine,
		uint64_t* tempHash,
		int programCount,
		size_t scratchpadSize
	) {
		machine->resetRoundingMode();
		for (int chain = 0; chain < programCount-1; chain++) {
			machine->run(tempHash);
			int blakeResult = randomx_blake2b(
				tempHash, 64,
				machine->getRegisterFile(),
				sizeof(randomx::RegisterFile),
				nullptr, 0
			);
			assert(blakeResult == 0);
		}
		machine->run(tempHash);
		int blakeResult = randomx_blake2b(
			tempHash, 64,
			machine->getRegisterFile(),
			sizeof(randomx::RegisterFile),
			nullptr, 0
		);
		assert(blakeResult == 0);
		_rsp_mix_entropy_near(
			(const unsigned char*)machine->getScratchpad(),
			(unsigned char*)(void*)machine->getScratchpad(),
			scratchpadSize);
	}

	void _copy_chunk_cross_lane(
		randomx_vm** inSet,
		randomx_vm** outSet,
		size_t srcPos,
		size_t dstPos,
		size_t length,
		size_t scratchpadSize
	) {
		while (length > 0) {
			int srcLane = (int)(srcPos / scratchpadSize);
			size_t offsetInSrcLane = srcPos % scratchpadSize;

			int dstLane = (int)(dstPos / scratchpadSize);
			size_t offsetInDstLane = dstPos % scratchpadSize;

			size_t srcLaneRemain = scratchpadSize - offsetInSrcLane;
			size_t dstLaneRemain = scratchpadSize - offsetInDstLane;

			size_t chunkSize = length;
			if (chunkSize > srcLaneRemain) {
				chunkSize = srcLaneRemain;
			}
			if (chunkSize > dstLaneRemain) {
				chunkSize = dstLaneRemain;
			}

			unsigned char* srcSp = (unsigned char*)(void*) inSet[srcLane]->getScratchpad();
			unsigned char* dstSp = (unsigned char*)(void*) outSet[dstLane]->getScratchpad();
			memcpy(dstSp + offsetInDstLane, srcSp + offsetInSrcLane, chunkSize);

			srcPos += chunkSize;
			dstPos += chunkSize;
			length -= chunkSize;
		}
	}

	void _rsp_mix_entropy_far(
		randomx_vm** inSet,
		randomx_vm** outSet,
		int count,
		size_t scratchpadSize,
		size_t jumpSize,
		size_t blockSize)
	{
		size_t totalSize = (size_t)count * scratchpadSize;

		size_t entropySize = totalSize;
		size_t numJumps = entropySize / jumpSize;
		size_t numBlocksPerJump = jumpSize / blockSize;
		size_t leftover = jumpSize % blockSize;

		size_t outOffset = 0;
		for (size_t offset = 0; offset < numBlocksPerJump; ++offset) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + offset * blockSize;
				_copy_chunk_cross_lane(inSet, outSet, srcPos, outOffset, blockSize, scratchpadSize);
				outOffset += blockSize;
			}
		}

		if (leftover > 0) {
			for (size_t i = 0; i < numJumps; ++i) {
				size_t srcPos = i * jumpSize + numBlocksPerJump * blockSize;
				_copy_chunk_cross_lane(inSet, outSet, srcPos, outOffset, leftover, scratchpadSize);
				outOffset += leftover;
			}
		}
	}

	int rsp_fused_entropy(
		randomx_vm** vmList,
		size_t scratchpadSize,
		int subChunkCount,
		int subChunkSize,
		int laneCount,
		int rxDepth,
		int randomxProgramCount,
		int blockSize,
		const unsigned char* keyData,
		size_t keySize,
		unsigned char* outEntropy
	) {
		struct vm_hash_t {
			alignas(16) uint64_t tempHash[8]; // 64 bytes
		};

		vm_hash_t* vmHashes = new (std::nothrow) vm_hash_t[laneCount];
		if (!vmHashes) {
			return 0;
		}

		// Initialize the scratchaps for each lane
		for (int i = 0; i < laneCount; i++) {
			// laneSeed = sha256(<<keyData, i>>)
			// laneSeed should be unique - i.e. now two lanes across all entropies and all
			// replicas should have the same seed. Current key (as off 2025-01-01) is
			// <<Partition, EntropyIndex, RewardAddr>> where entropy index is unique within
			// a given partition.
			unsigned char laneSeed[32];
			{
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, keyData, keySize);
				unsigned char laneIndex = (unsigned char)i + 1;
				SHA256_Update(&sha256, &laneIndex, 1);
				SHA256_Final(laneSeed, &sha256);
			}
			int blakeResult = randomx_blake2b(
				vmHashes[i].tempHash, sizeof(vmHashes[i].tempHash),
				laneSeed, 32,
				nullptr, 0
			);
			if (blakeResult != 0) {
				delete[] vmHashes;
				return 0;
			}
			vmList[i]->initScratchpad(&vmHashes[i].tempHash);
		}

		for (int d = 0; d < rxDepth; d++) {
			for (int lane = 0; lane < laneCount; lane++) {
				_rsp_exec_inplace(
					vmList[lane],
					vmHashes[lane].tempHash,
					randomxProgramCount, scratchpadSize);
			}
			_rsp_mix_entropy_far(&vmList[0], &vmList[laneCount],
										 laneCount, scratchpadSize, scratchpadSize,
										 blockSize);

			if (d + 1 < rxDepth) {
				d++;
				for (int lane = 0; lane < laneCount; lane++) {
					_rsp_exec_inplace(
						vmList[lane+laneCount],
						vmHashes[lane].tempHash,
						randomxProgramCount, scratchpadSize);
				}
				_rsp_mix_entropy_far(&vmList[laneCount], &vmList[0],
											 laneCount, scratchpadSize, scratchpadSize,
											 blockSize);
			}
		}
		// NOTE still unoptimal. Last copy can be performed from scratchpad to output.
		// But requires +1 variation (set to buffer)

		if ((rxDepth % 2) == 0) {
			unsigned char* outEntropyPtr = outEntropy;
			for (int i = 0; i < laneCount; i++) {
				void* sp = (void*)vmList[i]->getScratchpad();
				memcpy(outEntropyPtr, sp, scratchpadSize);
				outEntropyPtr += scratchpadSize;
			}
		} else {
			unsigned char* outEntropyPtr = outEntropy;
			for (int i = laneCount; i < 2*laneCount; i++) {
				void* sp = (void*)vmList[i]->getScratchpad();
				memcpy(outEntropyPtr, sp, scratchpadSize);
				outEntropyPtr += scratchpadSize;
			}
		}

		delete[] vmHashes;

		return 1;
	}

	// TODO optimized packing_apply_to_subchunk (NIF only uses slice)
}
