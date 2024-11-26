#ifndef CRC32_H
#define CRC32_H

#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
  #include <immintrin.h>
  #define crc32(a, b) _mm_crc32_u32(a, b)
#elif defined(__aarch64__) || defined(__arm__) || defined(_M_ARM64) || defined(_M_ARM)
  #include <arm_acle.h>
  #define crc32(a, b) __crc32cw(a, b)
#else
  // TODO make support for soft crc32
  #error "Unsupported architecture for CRC32 operations."
#endif

#endif // CRC32_H