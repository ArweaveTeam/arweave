#ifndef SHA2_H
#define SHA2_H
#ifdef __cplusplus
#include <cstddef>
#else
#include <stddef.h>
#endif

#if defined(__cplusplus)
extern "C" {
#endif

void sha2_p1(unsigned char* output, const unsigned char* input1, const size_t input1_size);
void sha2_p2(unsigned char* output, const unsigned char* input1, const size_t input1_size, const unsigned char* input2, const size_t input2_size);

#if defined(__cplusplus)
}
#endif

#endif // SHA2_H