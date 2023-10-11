#include <thread>
#include <cstring>
#include <vector>
#include <mutex>
#include <openssl/sha.h>
#include <gmp.h>
#include "vdf.h"

struct vdf_sha_thread_arg {
	unsigned char* saltBuffer;
	unsigned char* seed;
	unsigned char* outCheckpoint;
	int checkpointCount;
	int skipCheckpointCount;
	int hashingIterations;
	unsigned char* out;
};

struct vdf_sha_verify_thread_arg;

class vdf_verify_job {
public:
	unsigned char* startSaltBuffer;
	unsigned char* seed;
	unsigned char* inCheckpointSha;
	unsigned char* outCheckpointSha;
	unsigned char* inCheckpointRandomx;
	unsigned char* resetStepNumberBin256;
	unsigned char* resetSeed;
	int checkpointCount;
	int skipCheckpointCount;
	int hashingIterationsSha;
	int hashingIterationsRandomx;

	std::vector<vdf_sha_verify_thread_arg    > _vdf_sha_verify_thread_arg_list;
	volatile bool verifyRes;
	std::mutex lock;
};

struct vdf_sha_verify_thread_arg {
	std::thread* thread;
	volatile bool in_progress;

	vdf_verify_job* job;
	int checkpointIdx;
};

void long_add(unsigned char* saltBuffer, int checkpointIdx) {
	unsigned int acc = checkpointIdx;
	// big endian from erlang
	for(int i=SALT_SIZE-1;i>=0;i--) {
		unsigned int value = saltBuffer[i];
		value += acc;
		saltBuffer[i] = value & 0xFF;
		acc = value >> 8;
		if (acc == 0) break;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//    SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
// NOTE saltBuffer is mutable in progress
void _vdf_sha2(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char tempOut[VDF_SHA_HASH_SIZE];
	// 2 different branches for different optimisation cases
	if (skipCheckpointCount == 0) {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);
			
			if (hashingIterations > 1) {
				{
					SHA256_CTX sha256;
					SHA256_Init(&sha256);
					SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
					SHA256_Update(&sha256, locIn, VDF_SHA_HASH_SIZE); // -1 memcpy
					SHA256_Final(tempOut, &sha256);
				}
				for(int i = 2; i < hashingIterations; i++) {
					SHA256_CTX sha256;
					SHA256_Init(&sha256);
					SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
					SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
					SHA256_Final(tempOut, &sha256);
				}
				{
					SHA256_CTX sha256;
					SHA256_Init(&sha256);
					SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
					SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
					SHA256_Final(locOut, &sha256);
				}
			} else {
				{
					SHA256_CTX sha256;
					SHA256_Init(&sha256);
					SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
					SHA256_Update(&sha256, locIn, VDF_SHA_HASH_SIZE); // -1 memcpy
					SHA256_Final(locOut, &sha256);
				}
			}
			long_add(saltBuffer, 1);
		}
	} else {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);
			
			{
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
				SHA256_Update(&sha256, locIn, VDF_SHA_HASH_SIZE); // -1 memcpy
				SHA256_Final(tempOut, &sha256);
			}
			// 1 skip on start
			for(int i = 1; i < hashingIterations; i++) {
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
				SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
				SHA256_Final(tempOut, &sha256);
			}
			long_add(saltBuffer, 1);
			for(int j = 1; j < skipCheckpointCount; j++) {
				// no skips
				for(int i = 0; i < hashingIterations; i++) {
					SHA256_CTX sha256;
					SHA256_Init(&sha256);
					SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
					SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
					SHA256_Final(tempOut, &sha256);
				}
				long_add(saltBuffer, 1);
			}
			// 1 skip on end
			for(int i = 1; i < hashingIterations; i++) {
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
				SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
				SHA256_Final(tempOut, &sha256);
			}
			{
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, saltBuffer, SALT_SIZE);
				SHA256_Update(&sha256, tempOut, VDF_SHA_HASH_SIZE);
				SHA256_Final(locOut, &sha256);
			}
			long_add(saltBuffer, 1);
		}
	}
}

// use
//   unsigned char out[VDF_SHA_HASH_SIZE];
//   unsigned char* outCheckpoint = (unsigned char*)malloc(checkpointCount*VDF_SHA_HASH_SIZE);
//   free(outCheckpoint);
// for call
void vdf_sha2(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char saltBufferStack[SALT_SIZE];
	// ensure 1 L1 cache page used
	// no access to heap, except of 0-iteration
	memcpy(saltBufferStack, saltBuffer, SALT_SIZE);

	_vdf_sha2(saltBufferStack, seed, out, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//    Verify SHA
////////////////////////////////////////////////////////////////////////////////////////////////////
void _vdf_sha_verify_thread(vdf_sha_verify_thread_arg* _arg) {
	vdf_sha_verify_thread_arg* arg = _arg;
	while(true) {
		if (!arg->job->verifyRes) {
			return;
		}

		unsigned char expdOut[VDF_SHA_HASH_SIZE];
		unsigned char* in = arg->checkpointIdx == 0 ? arg->job->seed : (arg->job->inCheckpointSha + (arg->checkpointIdx-1)*VDF_SHA_HASH_SIZE);
		unsigned char* out = arg->job->inCheckpointSha + arg->checkpointIdx*VDF_SHA_HASH_SIZE;
		unsigned char* outFullCheckpoint = arg->job->outCheckpointSha + arg->checkpointIdx*(1+arg->job->skipCheckpointCount)*VDF_SHA_HASH_SIZE;
		unsigned char  saltBuffer[SALT_SIZE];
		memcpy(saltBuffer, arg->job->startSaltBuffer, SALT_SIZE);
		long_add(saltBuffer, arg->checkpointIdx*(1+arg->job->skipCheckpointCount));
		
		// unrolled for memcpy inject
		// _vdf_sha2(saltBuffer, in, expdOut, NULL, 0, arg->job->skipCheckpointCount, arg->job->hashingIterationsSha);
		
		// do not rewrite in
		unsigned char inCopy[VDF_SHA_HASH_SIZE];
		memcpy(inCopy, in, VDF_SHA_HASH_SIZE);
		for(int i=0;i<=arg->job->skipCheckpointCount;i++) {
			_vdf_sha2(saltBuffer, inCopy, expdOut, NULL, 0, 0, arg->job->hashingIterationsSha);
			memcpy(outFullCheckpoint, expdOut, VDF_SHA_HASH_SIZE);
			outFullCheckpoint += VDF_SHA_HASH_SIZE;
			memcpy(inCopy, expdOut, VDF_SHA_HASH_SIZE);
			// NOTE long_add included
		}
		// 0 == equal
		if (0 != memcmp(expdOut, out, VDF_SHA_HASH_SIZE)) {
			arg->job->verifyRes = false;
			return;
		}

		{
			const std::lock_guard<std::mutex> lock(arg->job->lock);

			bool found = false;
			for(int i=arg->checkpointIdx+1;i<arg->job->checkpointCount;i++) {
				vdf_sha_verify_thread_arg* new_arg = &arg->job->_vdf_sha_verify_thread_arg_list[i];
				if (!new_arg->in_progress) {
					new_arg->in_progress = true;
					arg = new_arg;
					found = true;
					break;
				}
			}
			if (!found) break;
		}
	}

	// TODO steal job from other hash function
}

bool vdf_parallel_sha_verify(unsigned char* startSaltBuffer, unsigned char* seed, int checkpointCount, int skipCheckpointCount, int hashingIterations, unsigned char* inRes, unsigned char* inCheckpoint, unsigned char* outCheckpoint, int maxThreadCount) {
	int freeThreadCount = maxThreadCount;

	vdf_verify_job job;
	job.startSaltBuffer = startSaltBuffer;
	job.seed = seed;
	job.inCheckpointSha = inCheckpoint;
	job.outCheckpointSha = outCheckpoint;
	job.checkpointCount = checkpointCount;
	job.skipCheckpointCount = skipCheckpointCount;
	job.hashingIterationsSha = hashingIterations;
	job.verifyRes = true;

	job._vdf_sha_verify_thread_arg_list    .resize(checkpointCount);

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];
		_vdf_sha_verify_thread_arg    ->checkpointIdx = checkpointIdx;
		_vdf_sha_verify_thread_arg    ->thread = NULL;
		_vdf_sha_verify_thread_arg    ->in_progress = false;
		_vdf_sha_verify_thread_arg    ->job = &job;
	}

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];
		if (freeThreadCount > 0) {
			freeThreadCount--;
			const std::lock_guard<std::mutex> lock(job.lock);
			_vdf_sha_verify_thread_arg->in_progress = true;
			_vdf_sha_verify_thread_arg->thread = new std::thread(_vdf_sha_verify_thread, _vdf_sha_verify_thread_arg);
		}
		if (freeThreadCount == 0) break;
	}

	if (job.verifyRes) {
		unsigned char expdOut[VDF_SHA_HASH_SIZE];
		unsigned char* sha_temp_result;
		if (checkpointCount == 0) {
			sha_temp_result = seed;
		} else {
			sha_temp_result = inCheckpoint + (checkpointCount-1)*VDF_SHA_HASH_SIZE;
		}
		
		unsigned char finalSaltBuffer[SALT_SIZE];
		memcpy(finalSaltBuffer, startSaltBuffer, SALT_SIZE);
		long_add(finalSaltBuffer, checkpointCount*(1+skipCheckpointCount));
		
		unsigned char* outFullCheckpoint = outCheckpoint + checkpointCount*(1+skipCheckpointCount)*VDF_SHA_HASH_SIZE;
		
		// unrolled for memcpy inject
		// _vdf_sha2(finalSaltBuffer, sha_temp_result, expdOut, NULL, 0, skipCheckpointCount, hashingIterations);
		
		// do not rewrite in
		unsigned char inCopy[VDF_SHA_HASH_SIZE];
		memcpy(inCopy, sha_temp_result, VDF_SHA_HASH_SIZE);
		for(int i=0;i<=skipCheckpointCount;i++) {
			_vdf_sha2(finalSaltBuffer, inCopy, expdOut, NULL, 0, 0, hashingIterations);
			memcpy(outFullCheckpoint, expdOut, VDF_SHA_HASH_SIZE);
			outFullCheckpoint += VDF_SHA_HASH_SIZE;
			memcpy(inCopy, expdOut, VDF_SHA_HASH_SIZE);
			// NOTE long_add included
		}
		if (0 != memcmp(expdOut, inRes, VDF_SHA_HASH_SIZE)) {
			job.verifyRes = false;
		}
	}

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];

		if (_vdf_sha_verify_thread_arg->thread) {
			_vdf_sha_verify_thread_arg->thread->join();
			free(_vdf_sha_verify_thread_arg->thread);
		}
	}

	return job.verifyRes;
}

void reset_mix(unsigned char* res, unsigned char* prevOutput, unsigned char* resetSeed) {
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, prevOutput, VDF_SHA_HASH_SIZE);
	SHA256_Update(&sha256, resetSeed, VDF_SHA_HASH_SIZE);
	SHA256_Final(res, &sha256);
}

bool fast_rev_cmp256(unsigned char* a, unsigned char* b) {
	for(int i=31; i>=0; i--) {
		if (a[i] != b[i])
			return false;
	}
	return true;
}

void _vdf_sha_verify_with_reset_thread(vdf_sha_verify_thread_arg* _arg) {
	vdf_sha_verify_thread_arg* arg = _arg;
	while(true) {
		if (!arg->job->verifyRes) {
			return;
		}

		unsigned char expdOut[VDF_SHA_HASH_SIZE];
		unsigned char* in = arg->checkpointIdx == 0 ? arg->job->seed : (arg->job->inCheckpointSha + (arg->checkpointIdx-1)*VDF_SHA_HASH_SIZE);
		unsigned char* out = arg->job->inCheckpointSha + arg->checkpointIdx*VDF_SHA_HASH_SIZE;
		unsigned char* outFullCheckpoint = arg->job->outCheckpointSha + arg->checkpointIdx*(1+arg->job->skipCheckpointCount)*VDF_SHA_HASH_SIZE;
		unsigned char  saltBuffer[SALT_SIZE];
		memcpy(saltBuffer, arg->job->startSaltBuffer, SALT_SIZE);
		long_add(saltBuffer, arg->checkpointIdx*(1+arg->job->skipCheckpointCount));
		
		// unrolled for memcpy inject and reset_mix
		// _vdf_sha2(saltBuffer, in, expdOut, NULL, 0, arg->job->skipCheckpointCount, arg->job->hashingIterationsSha);
		
		// do not rewrite in
		unsigned char inCopy[VDF_SHA_HASH_SIZE];
		memcpy(inCopy, in, VDF_SHA_HASH_SIZE);
		
		if (fast_rev_cmp256(saltBuffer, arg->job->resetStepNumberBin256)) {
			reset_mix(inCopy, inCopy, arg->job->resetSeed);
		}
		
		for(int i=0;i<=arg->job->skipCheckpointCount;i++) {
			_vdf_sha2(saltBuffer, inCopy, expdOut, NULL, 0, 0, arg->job->hashingIterationsSha);
			memcpy(outFullCheckpoint, expdOut, VDF_SHA_HASH_SIZE);
			outFullCheckpoint += VDF_SHA_HASH_SIZE;
			memcpy(inCopy, expdOut, VDF_SHA_HASH_SIZE);
			
			if (fast_rev_cmp256(saltBuffer, arg->job->resetStepNumberBin256)) {
				reset_mix(inCopy, inCopy, arg->job->resetSeed);
			}
			// NOTE long_add included
		}
		// 0 == equal
		if (0 != memcmp(expdOut, out, VDF_SHA_HASH_SIZE)) {
			arg->job->verifyRes = false;
			return;
		}

		{
			const std::lock_guard<std::mutex> lock(arg->job->lock);

			bool found = false;
			for(int i=arg->checkpointIdx+1;i<arg->job->checkpointCount;i++) {
				vdf_sha_verify_thread_arg* new_arg = &arg->job->_vdf_sha_verify_thread_arg_list[i];
				if (!new_arg->in_progress) {
					new_arg->in_progress = true;
					arg = new_arg;
					found = true;
					break;
				}
			}
			if (!found) break;
		}
	}

	// TODO steal job from other hash function
}

bool vdf_parallel_sha_verify_with_reset(unsigned char* startSaltBuffer, unsigned char* seed, int checkpointCount, int skipCheckpointCount, int hashingIterations, unsigned char* inRes, unsigned char* inCheckpoint, unsigned char* outCheckpoint, unsigned char* resetStepNumberBin256, unsigned char* resetSeed, int maxThreadCount) {
	int freeThreadCount = maxThreadCount;

	vdf_verify_job job;
	job.startSaltBuffer = startSaltBuffer;
	job.seed = seed;
	job.inCheckpointSha = inCheckpoint;
	job.outCheckpointSha = outCheckpoint;
	job.checkpointCount = checkpointCount;
	job.skipCheckpointCount = skipCheckpointCount;
	job.hashingIterationsSha = hashingIterations;
	job.resetStepNumberBin256 = resetStepNumberBin256;
	job.resetSeed = resetSeed;
	job.verifyRes = true;

	job._vdf_sha_verify_thread_arg_list    .resize(checkpointCount);

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];
		_vdf_sha_verify_thread_arg    ->checkpointIdx = checkpointIdx;
		_vdf_sha_verify_thread_arg    ->thread = NULL;
		_vdf_sha_verify_thread_arg    ->in_progress = false;
		_vdf_sha_verify_thread_arg    ->job = &job;
	}

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];
		if (freeThreadCount > 0) {
			freeThreadCount--;
			const std::lock_guard<std::mutex> lock(job.lock);
			_vdf_sha_verify_thread_arg->in_progress = true;
			_vdf_sha_verify_thread_arg->thread = new std::thread(_vdf_sha_verify_with_reset_thread, _vdf_sha_verify_thread_arg);
		}
		if (freeThreadCount == 0) break;
	}

	if (job.verifyRes) {
		unsigned char expdOut[VDF_SHA_HASH_SIZE];
		unsigned char* sha_temp_result;
		if (checkpointCount == 0) {
			sha_temp_result = seed;
		} else {
			sha_temp_result = inCheckpoint + (checkpointCount-1)*VDF_SHA_HASH_SIZE;
		}
		
		unsigned char finalSaltBuffer[SALT_SIZE];
		memcpy(finalSaltBuffer, startSaltBuffer, SALT_SIZE);
		long_add(finalSaltBuffer, checkpointCount*(1+skipCheckpointCount));
		
		unsigned char* outFullCheckpoint = outCheckpoint + checkpointCount*(1+skipCheckpointCount)*VDF_SHA_HASH_SIZE;
		
		// unrolled for memcpy inject
		// _vdf_sha2(finalSaltBuffer, sha_temp_result, expdOut, NULL, 0, skipCheckpointCount, hashingIterations);
		
		// do not rewrite in
		unsigned char inCopy[VDF_SHA_HASH_SIZE];
		memcpy(inCopy, sha_temp_result, VDF_SHA_HASH_SIZE);
		
		if (fast_rev_cmp256(finalSaltBuffer, resetStepNumberBin256)) {
			reset_mix(inCopy, inCopy, resetSeed);
		}
		
		for(int i=0;i<=skipCheckpointCount;i++) {
			_vdf_sha2(finalSaltBuffer, inCopy, expdOut, NULL, 0, 0, hashingIterations);
			memcpy(outFullCheckpoint, expdOut, VDF_SHA_HASH_SIZE);
			outFullCheckpoint += VDF_SHA_HASH_SIZE;
			memcpy(inCopy, expdOut, VDF_SHA_HASH_SIZE);
			
			if (fast_rev_cmp256(finalSaltBuffer, resetStepNumberBin256)) {
				reset_mix(inCopy, inCopy, resetSeed);
			}
			// NOTE long_add included
		}
		if (0 != memcmp(expdOut, inRes, VDF_SHA_HASH_SIZE)) {
			job.verifyRes = false;
		}
	}

	for (int checkpointIdx=0;checkpointIdx<checkpointCount;checkpointIdx++) {
		struct vdf_sha_verify_thread_arg*     _vdf_sha_verify_thread_arg     = &job._vdf_sha_verify_thread_arg_list[checkpointIdx];

		if (_vdf_sha_verify_thread_arg->thread) {
			_vdf_sha_verify_thread_arg->thread->join();
			free(_vdf_sha_verify_thread_arg->thread);
		}
	}

	return job.verifyRes;
}
