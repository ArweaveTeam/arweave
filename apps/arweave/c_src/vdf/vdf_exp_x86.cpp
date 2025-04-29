#include <cstdint>
#include <cstring>
#include "vdf.h"

#if defined(__x86_64__) || defined(__i386__)

#include <immintrin.h>

// NOTE spaces here, because tabs are much more difficult for backslash alignment
#define sha256_compress1(state, data) {                                                             \
    __m128i STATE0, STATE1, ABEF_SAVE, CDGH_SAVE;                                                   \
    __m128i MSG, TMP, MASK;                                                                         \
    __m128i TMSG0, TMSG1, TMSG2, TMSG3;                                                             \
                                                                                                    \
                                                                                                    \
    TMP   = _mm_loadu_si128((const __m128i*) &state[0]);                                            \
    STATE1= _mm_loadu_si128((const __m128i*) &state[4]);                                            \
                                                                                                    \
    MASK  = _mm_set_epi64x(0x0c0d0e0f08090a0bULL, 0x0405060700010203ULL);                           \
                                                                                                    \
                                                                                                    \
    TMP   = _mm_shuffle_epi32(TMP, 0xB1);                                                           \
    STATE1= _mm_shuffle_epi32(STATE1, 0x1B);                                                        \
    STATE0= _mm_alignr_epi8(TMP, STATE1, 8);                                                        \
    STATE1= _mm_blend_epi16(STATE1, TMP, 0xF0);                                                     \
                                                                                                    \
                                                                                                    \
    {                                                                                               \
                                                                                                    \
        ABEF_SAVE = STATE0;                                                                         \
        CDGH_SAVE = STATE1;                                                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_loadu_si128((const __m128i*) (data + 0));                                       \
        TMSG0 = _mm_shuffle_epi8(MSG, MASK);                                                        \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0xE9B5DBA5B5C0FBCFULL,                          \
                                                   0x71374491428A2F98ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        TMSG1 = _mm_loadu_si128((const __m128i*) (data + 16));                                      \
        TMSG1 = _mm_shuffle_epi8(TMSG1, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0xAB1C5ED5923F82A4ULL,                          \
                                                   0x59F111F13956C25BULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        TMSG2 = _mm_loadu_si128((const __m128i*) (data + 32));                                      \
        TMSG2 = _mm_shuffle_epi8(TMSG2, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0x550C7DC3243185BEULL,                          \
                                                   0x12835B01D807AA98ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        TMSG3 = _mm_loadu_si128((const __m128i*) (data + 48));                                      \
        TMSG3 = _mm_shuffle_epi8(TMSG3, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0xC19BF1749BDC06A7ULL,                          \
                                                   0x80DEB1FE72BE5D74ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x240CA1CC0FC19DC6ULL,                          \
                                                   0xEFBE4786E49B69C1ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x76F988DA5CB0A9DCULL,                          \
                                                   0x4A7484AA2DE92C6FULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0xBF597FC7B00327C8ULL,                          \
                                                   0xA831C66D983E5152ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0x1429296706CA6351ULL,                          \
                                                   0xD5A79147C6E00BF3ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x53380D134D2C6DFCULL,                          \
                                                   0x2E1B213827B70A85ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x92722C8581C2C92EULL,                          \
                                                   0x766A0ABB650A7354ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0xC76C51A3C24B8B70ULL,                          \
                                                   0xA81A664BA2BFE8A1ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0x106AA070F40E3585ULL,                          \
                                                   0xD6990624D192E819ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x34B0BCB52748774CULL,                          \
                                                   0x1E376C0819A4C116ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x682E6FF35B9CCA4FULL,                          \
                                                   0x4ED8AA4A391C0CB3ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0x8CC7020884C87814ULL,                          \
                                                   0x78A5636F748F82EEULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0xC67178F2BEF9A3F7ULL,                          \
                                                   0xA4506CEB90BEFFFAULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        STATE0 = _mm_add_epi32(STATE0, ABEF_SAVE);                                                  \
        STATE1 = _mm_add_epi32(STATE1, CDGH_SAVE);                                                  \
    }                                                                                               \
                                                                                                    \
                                                                                                    \
    TMP    = _mm_shuffle_epi32(STATE0, 0x1B);                                                       \
    STATE1 = _mm_shuffle_epi32(STATE1, 0xB1);                                                       \
    STATE0 = _mm_blend_epi16(TMP, STATE1, 0xF0);                                                    \
    STATE1 = _mm_alignr_epi8(STATE1, TMP, 8);                                                       \
                                                                                                    \
                                                                                                    \
    _mm_storeu_si128((__m128i*) &state[0], STATE0);                                                 \
    _mm_storeu_si128((__m128i*) &state[4], STATE1);                                                 \
}

#define sha256_compress1_2(state, data1, data2) {                                                   \
    __m128i STATE0, STATE1, ABEF_SAVE, CDGH_SAVE;                                                   \
    __m128i MSG, TMP, MASK;                                                                         \
    __m128i TMSG0, TMSG1, TMSG2, TMSG3;                                                             \
                                                                                                    \
                                                                                                    \
    TMP   = _mm_loadu_si128((const __m128i*) &state[0]);                                            \
    STATE1= _mm_loadu_si128((const __m128i*) &state[4]);                                            \
                                                                                                    \
    MASK  = _mm_set_epi64x(0x0c0d0e0f08090a0bULL, 0x0405060700010203ULL);                           \
                                                                                                    \
                                                                                                    \
    TMP   = _mm_shuffle_epi32(TMP, 0xB1);                                                           \
    STATE1= _mm_shuffle_epi32(STATE1, 0x1B);                                                        \
    STATE0= _mm_alignr_epi8(TMP, STATE1, 8);                                                        \
    STATE1= _mm_blend_epi16(STATE1, TMP, 0xF0);                                                     \
                                                                                                    \
                                                                                                    \
    {                                                                                               \
                                                                                                    \
        ABEF_SAVE = STATE0;                                                                         \
        CDGH_SAVE = STATE1;                                                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_loadu_si128((const __m128i*) (data1 + 0));                                      \
        TMSG0 = _mm_shuffle_epi8(MSG, MASK);                                                        \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0xE9B5DBA5B5C0FBCFULL,                          \
                                                   0x71374491428A2F98ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        TMSG1 = _mm_loadu_si128((const __m128i*) (data1 + 16));                                     \
        TMSG1 = _mm_shuffle_epi8(TMSG1, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0xAB1C5ED5923F82A4ULL,                          \
                                                   0x59F111F13956C25BULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        TMSG2 = _mm_loadu_si128((const __m128i*) (data2 + 0));                                      \
        TMSG2 = _mm_shuffle_epi8(TMSG2, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0x550C7DC3243185BEULL,                          \
                                                   0x12835B01D807AA98ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        TMSG3 = _mm_loadu_si128((const __m128i*) (data2 + 16));                                     \
        TMSG3 = _mm_shuffle_epi8(TMSG3, MASK);                                                      \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0xC19BF1749BDC06A7ULL,                          \
                                                   0x80DEB1FE72BE5D74ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x240CA1CC0FC19DC6ULL,                          \
                                                   0xEFBE4786E49B69C1ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x76F988DA5CB0A9DCULL,                          \
                                                   0x4A7484AA2DE92C6FULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0xBF597FC7B00327C8ULL,                          \
                                                   0xA831C66D983E5152ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0x1429296706CA6351ULL,                          \
                                                   0xD5A79147C6E00BF3ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x53380D134D2C6DFCULL,                          \
                                                   0x2E1B213827B70A85ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x92722C8581C2C92EULL,                          \
                                                   0x766A0ABB650A7354ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG0= _mm_sha256msg1_epu32(TMSG0, TMSG1);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0xC76C51A3C24B8B70ULL,                          \
                                                   0xA81A664BA2BFE8A1ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG1= _mm_sha256msg1_epu32(TMSG1, TMSG2);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0x106AA070F40E3585ULL,                          \
                                                   0xD6990624D192E819ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG3, TMSG2, 4);                                                   \
        TMSG0= _mm_add_epi32(TMSG0, TMP);                                                           \
        TMSG0= _mm_sha256msg2_epu32(TMSG0, TMSG3);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG2= _mm_sha256msg1_epu32(TMSG2, TMSG3);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG0, _mm_set_epi64x(0x34B0BCB52748774CULL,                          \
                                                   0x1E376C0819A4C116ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG0, TMSG3, 4);                                                   \
        TMSG1= _mm_add_epi32(TMSG1, TMP);                                                           \
        TMSG1= _mm_sha256msg2_epu32(TMSG1, TMSG0);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
        TMSG3= _mm_sha256msg1_epu32(TMSG3, TMSG0);                                                  \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG1, _mm_set_epi64x(0x682E6FF35B9CCA4FULL,                          \
                                                   0x4ED8AA4A391C0CB3ULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG1, TMSG0, 4);                                                   \
        TMSG2= _mm_add_epi32(TMSG2, TMP);                                                           \
        TMSG2= _mm_sha256msg2_epu32(TMSG2, TMSG1);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG2, _mm_set_epi64x(0x8CC7020884C87814ULL,                          \
                                                   0x78A5636F748F82EEULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        TMP   = _mm_alignr_epi8(TMSG2, TMSG1, 4);                                                   \
        TMSG3= _mm_add_epi32(TMSG3, TMP);                                                           \
        TMSG3= _mm_sha256msg2_epu32(TMSG3, TMSG2);                                                  \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        MSG   = _mm_add_epi32(TMSG3, _mm_set_epi64x(0xC67178F2BEF9A3F7ULL,                          \
                                                   0xA4506CEB90BEFFFAULL));                         \
        STATE1= _mm_sha256rnds2_epu32(STATE1, STATE0, MSG);                                         \
        MSG   = _mm_shuffle_epi32(MSG, 0x0E);                                                       \
        STATE0= _mm_sha256rnds2_epu32(STATE0, STATE1, MSG);                                         \
                                                                                                    \
                                                                                                    \
        STATE0 = _mm_add_epi32(STATE0, ABEF_SAVE);                                                  \
        STATE1 = _mm_add_epi32(STATE1, CDGH_SAVE);                                                  \
    }                                                                                               \
                                                                                                    \
                                                                                                    \
    TMP    = _mm_shuffle_epi32(STATE0, 0x1B);                                                       \
    STATE1 = _mm_shuffle_epi32(STATE1, 0xB1);                                                       \
    STATE0 = _mm_blend_epi16(TMP, STATE1, 0xF0);                                                    \
    STATE1 = _mm_alignr_epi8(STATE1, TMP, 8);                                                       \
                                                                                                    \
                                                                                                    \
    _mm_storeu_si128((__m128i*) &state[0], STATE0);                                                 \
    _mm_storeu_si128((__m128i*) &state[4], STATE1);                                                 \
}



// Optimized sha2_p2_32_32: Computes SHA-256 on the concatenation of two 32-byte inputs.
#define sha2_p2_32_32(output, input1, input2) {                                                     \
                                                                                                    \
    uint32_t state[8];                                                                              \
                                                                                                    \
    state[0] = 0x6a09e667U;                                                                         \
    state[1] = 0xbb67ae85U;                                                                         \
    state[2] = 0x3c6ef372U;                                                                         \
    state[3] = 0xa54ff53aU;                                                                         \
    state[4] = 0x510e527fU;                                                                         \
    state[5] = 0x9b05688cU;                                                                         \
    state[6] = 0x1f83d9abU;                                                                         \
    state[7] = 0x5be0cd19U;                                                                         \
                                                                                                    \
    sha256_compress1_2(state, input1, input2);                                                      \
                                                                                                    \
    uint8_t block2[64] = { 0 };                                                                     \
    block2[0] = 0x80;                                                                               \
                                                                                                    \
    uint64_t bit_len = 64ULL * 8ULL;                                                                \
    block2[56] = (uint8_t)(bit_len >> 56);                                                          \
    block2[57] = (uint8_t)(bit_len >> 48);                                                          \
    block2[58] = (uint8_t)(bit_len >> 40);                                                          \
    block2[59] = (uint8_t)(bit_len >> 32);                                                          \
    block2[60] = (uint8_t)(bit_len >> 24);                                                          \
    block2[61] = (uint8_t)(bit_len >> 16);                                                          \
    block2[62] = (uint8_t)(bit_len >> 8);                                                           \
    block2[63] = (uint8_t)(bit_len);                                                                \
                                                                                                    \
                                                                                                    \
    sha256_compress1(state, block2);                                                                \
                                                                                                    \
                                                                                                    \
    output[ 0] = (uint8_t)(state[0] >> 24);                                                         \
    output[ 1] = (uint8_t)(state[0] >> 16);                                                         \
    output[ 2] = (uint8_t)(state[0] >> 8);                                                          \
    output[ 3] = (uint8_t)(state[0]);                                                               \
                                                                                                    \
    output[ 4] = (uint8_t)(state[1] >> 24);                                                         \
    output[ 5] = (uint8_t)(state[1] >> 16);                                                         \
    output[ 6] = (uint8_t)(state[1] >> 8);                                                          \
    output[ 7] = (uint8_t)(state[1]);                                                               \
                                                                                                    \
    output[ 8]  = (uint8_t)(state[2] >> 24);                                                        \
    output[ 9]  = (uint8_t)(state[2] >> 16);                                                        \
    output[10] = (uint8_t)(state[2] >> 8);                                                          \
    output[11] = (uint8_t)(state[2]);                                                               \
                                                                                                    \
    output[12] = (uint8_t)(state[3] >> 24);                                                         \
    output[13] = (uint8_t)(state[3] >> 16);                                                         \
    output[14] = (uint8_t)(state[3] >> 8);                                                          \
    output[15] = (uint8_t)(state[3]);                                                               \
                                                                                                    \
    output[16] = (uint8_t)(state[4] >> 24);                                                         \
    output[17] = (uint8_t)(state[4] >> 16);                                                         \
    output[18] = (uint8_t)(state[4] >> 8);                                                          \
    output[19] = (uint8_t)(state[4]);                                                               \
                                                                                                    \
    output[20] = (uint8_t)(state[5] >> 24);                                                         \
    output[21] = (uint8_t)(state[5] >> 16);                                                         \
    output[22] = (uint8_t)(state[5] >> 8);                                                          \
    output[23] = (uint8_t)(state[5]);                                                               \
                                                                                                    \
    output[24] = (uint8_t)(state[6] >> 24);                                                         \
    output[25] = (uint8_t)(state[6] >> 16);                                                         \
    output[26] = (uint8_t)(state[6] >> 8);                                                          \
    output[27] = (uint8_t)(state[6]);                                                               \
                                                                                                    \
    output[28] = (uint8_t)(state[7] >> 24);                                                         \
    output[29] = (uint8_t)(state[7] >> 16);                                                         \
    output[30] = (uint8_t)(state[7] >> 8);                                                          \
    output[31] = (uint8_t)(state[7]);                                                               \
                                                                                                    \
}


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
// TODO make even better impl with ideas from ARM impl
void _vdf_sha2_exp_x86(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	// 2 different branches for different optimisation cases
	if (skipCheckpointCount == 0) {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);
			memcpy(locOut, locIn, VDF_SHA_HASH_SIZE);

			for(int i = 0; i < hashingIterations; i++) {
				sha2_p2_32_32(locOut, saltBuffer, locOut);
			}
			long_add(saltBuffer, 1);
		}
	} else {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);
			memcpy(locOut, locIn, VDF_SHA_HASH_SIZE);

			// 1 skip on start
			for(int i = 0; i < hashingIterations; i++) {
				sha2_p2_32_32(locOut, saltBuffer, locOut);
			}
			long_add(saltBuffer, 1);
			for(int j = 1; j < skipCheckpointCount; j++) {
				// no skips
				for(int i = 0; i < hashingIterations; i++) {
					sha2_p2_32_32(locOut, saltBuffer, locOut);
				}
				long_add(saltBuffer, 1);
			}
			// 1 skip on end
			for(int i = 0; i < hashingIterations; i++) {
				sha2_p2_32_32(locOut, saltBuffer, locOut);
			}
			long_add(saltBuffer, 1);
		}
	}
}

void vdf_sha2_exp_x86(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char saltBufferStack[SALT_SIZE];
	// ensure 1 L1 cache page used
	// no access to heap, except of 0-iteration
	memcpy(saltBufferStack, saltBuffer, SALT_SIZE);

	_vdf_sha2_exp_x86(saltBufferStack, seed, out, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);
}

#endif
