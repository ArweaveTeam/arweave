#include <cstdint>
#include <cstring>
#include "vdf.h"

#if defined(__aarch64__) || defined(__arm__)

#include <arm_neon.h>
#include <arm_acle.h>

static const uint32_t K[] = {
	0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5,
	0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
	0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
	0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
	0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC,
	0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
	0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7,
	0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
	0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
	0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
	0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3,
	0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
	0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5,
	0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
	0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
	0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2
};

void sha2_p2_32_32_rev_norm (unsigned char *output,
                             const unsigned char *input1,
                             const unsigned char *input2)
{
	uint32_t state[8] = {
		0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
		0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
	};

	uint32x4_t STATE0, STATE1, ABEF_SAVE, CDGH_SAVE;
	uint32x4_t MSG0, MSG1, MSG2, MSG3;
	uint32x4_t TMP0, TMP2;

	// Load state
	STATE0 = vld1q_u32(&state[0]);
	STATE1 = vld1q_u32(&state[4]);

	// Save current state
	ABEF_SAVE = STATE0;
	CDGH_SAVE = STATE1;

	// Load input1 (32 bytes) and input2 (32 bytes) into two message blocks
	// These constitute our 64-byte block
	MSG0 = vld1q_u32((const uint32_t *)(input1 + 0));
	MSG1 = vld1q_u32((const uint32_t *)(input1 + 16));
	MSG2 = vld1q_u32((const uint32_t *)(input2 + 0));
	MSG3 = vld1q_u32((const uint32_t *)(input2 + 16));

	// Adjust endianness
	MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
	MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));
	MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG2)));
	MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG3)));

	// Rounds 1-4
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 5-8
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 9-12
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 13-16
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 17-20
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 21-24
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 25-28
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 29-32
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 33-36
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 37-40
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 41-44
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 45-48
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 49-52
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 53-56
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 57-60
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 61-64
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Update state
	STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
	STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

	// Now we need to process the padding block
	// For a 64-byte input (32+32), the padding block consists of 0x80 followed by zeros
	// and the 64-bit length (512 bits)

	// TODO merge with endian fixes
	uint8_t padding[64] = {0};
	padding[0] = 0x80;  // Padding start marker

	// Set the 64-bit length value (512 bits = 0x0200)
	padding[56] = 0x00;
	padding[57] = 0x00;
	padding[58] = 0x00;
	padding[59] = 0x00;
	padding[60] = 0x00;
	padding[61] = 0x00;
	padding[62] = 0x02;
	padding[63] = 0x00;

	// Save current state
	ABEF_SAVE = STATE0;
	CDGH_SAVE = STATE1;

	// Load padding block
	MSG0 = vld1q_u32((const uint32_t *)(padding + 0));
	MSG1 = vld1q_u32((const uint32_t *)(padding + 16));
	MSG2 = vld1q_u32((const uint32_t *)(padding + 32));
	MSG3 = vld1q_u32((const uint32_t *)(padding + 48));

	MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
	MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));
	MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG2)));
	MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG3)));

	// Rounds 1-4
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 5-8
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 9-12
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 13-16
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 17-20
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 21-24
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 25-28
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 29-32
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 33-36
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 37-40
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 41-44
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 45-48
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 49-52
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 53-56
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 57-60
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 61-64
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Update state
	STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
	STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

	/* write 32-bit words little-endian */
	vst1q_u32((uint32_t *)output,      STATE0);
	vst1q_u32((uint32_t *)(output+16), STATE1);
}

void sha2_p2_32_32_norm_loop(unsigned char  *tempOut,
                        const unsigned char *saltBuffer,
                        int                 iterations)
{
	uint32_t state[8] = {
		0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
		0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
	};

	uint32x4_t STATE0, STATE1, ABEF_SAVE, CDGH_SAVE;
	uint32x4_t MSG0, MSG1, MSG2, MSG3;
	uint32x4_t TMP0, TMP2;

	MSG2 = vld1q_u32((const uint32_t *)(tempOut + 0));
	MSG3 = vld1q_u32((const uint32_t *)(tempOut + 16));

	for (int i = 0; i < iterations; ++i) {
		STATE0 = vld1q_u32(&state[0]);
		STATE1 = vld1q_u32(&state[4]);

		// Save current state
		ABEF_SAVE = STATE0;
		CDGH_SAVE = STATE1;

		// Load input1 (32 bytes) and input2 (32 bytes) into two message blocks
		// These constitute our 64-byte block
		MSG0 = vld1q_u32((const uint32_t *)(saltBuffer + 0));
		MSG1 = vld1q_u32((const uint32_t *)(saltBuffer + 16));

		// Adjust endianness
		MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
		MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));

		// Rounds 1-4
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 5-8
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 9-12
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 13-16
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 17-20
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 21-24
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 25-28
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 29-32
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 33-36
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 37-40
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 41-44
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 45-48
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 49-52
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 53-56
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 57-60
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 61-64
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Update state
		STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
		STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

		// Now we need to process the padding block
		// For a 64-byte input (32+32), the padding block consists of 0x80 followed by zeros
		// and the 64-bit length (512 bits)

		// TODO merge with endian fixes
		uint8_t padding[64] = {0};
		padding[0] = 0x80;  // Padding start marker

		// Set the 64-bit length value (512 bits = 0x0200)
		padding[56] = 0x00;
		padding[57] = 0x00;
		padding[58] = 0x00;
		padding[59] = 0x00;
		padding[60] = 0x00;
		padding[61] = 0x00;
		padding[62] = 0x02;
		padding[63] = 0x00;

		// Save current state
		ABEF_SAVE = STATE0;
		CDGH_SAVE = STATE1;

		// Load padding block
		MSG0 = vld1q_u32((const uint32_t *)(padding + 0));
		MSG1 = vld1q_u32((const uint32_t *)(padding + 16));
		MSG2 = vld1q_u32((const uint32_t *)(padding + 32));
		MSG3 = vld1q_u32((const uint32_t *)(padding + 48));

		MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
		MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));
		MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG2)));
		MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG3)));

		// Rounds 1-4
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 5-8
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 9-12
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 13-16
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 17-20
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 21-24
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 25-28
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 29-32
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 33-36
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
		TMP2 = STATE0;
		MSG0 = vsha256su0q_u32(MSG0, MSG1);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

		// Rounds 37-40
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
		TMP2 = STATE0;
		MSG1 = vsha256su0q_u32(MSG1, MSG2);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

		// Rounds 41-44
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
		TMP2 = STATE0;
		MSG2 = vsha256su0q_u32(MSG2, MSG3);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

		// Rounds 45-48
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
		TMP2 = STATE0;
		MSG3 = vsha256su0q_u32(MSG3, MSG0);
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
		MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

		// Rounds 49-52
		TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 53-56
		TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 57-60
		TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Rounds 61-64
		TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
		TMP2 = STATE0;
		STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
		STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

		// Update state
		MSG2 = vaddq_u32(STATE0, ABEF_SAVE);
		MSG3 = vaddq_u32(STATE1, CDGH_SAVE);
	}

	vst1q_u32((uint32_t *)tempOut,      MSG2);
	vst1q_u32((uint32_t *)(tempOut+16), MSG3);
}

void sha2_p2_32_32_norm_rev (unsigned char *output,
                             const unsigned char *input1,
                             const unsigned char *input2)
{
	uint32_t state[8] = {
		0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
		0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
	};

	uint32x4_t STATE0, STATE1, ABEF_SAVE, CDGH_SAVE;
	uint32x4_t MSG0, MSG1, MSG2, MSG3;
	uint32x4_t TMP0, TMP2;

	// Load state
	STATE0 = vld1q_u32(&state[0]);
	STATE1 = vld1q_u32(&state[4]);

	// Save current state
	ABEF_SAVE = STATE0;
	CDGH_SAVE = STATE1;

	// Load input1 (32 bytes) and input2 (32 bytes) into two message blocks
	// These constitute our 64-byte block
	MSG0 = vld1q_u32((const uint32_t *)(input1 + 0));
	MSG1 = vld1q_u32((const uint32_t *)(input1 + 16));
	MSG2 = vld1q_u32((const uint32_t *)(input2 + 0));
	MSG3 = vld1q_u32((const uint32_t *)(input2 + 16));

	// Adjust endianness
	MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
	MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));

	// Rounds 1-4
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 5-8
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 9-12
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 13-16
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 17-20
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 21-24
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 25-28
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 29-32
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 33-36
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 37-40
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 41-44
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 45-48
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 49-52
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 53-56
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 57-60
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 61-64
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Update state
	STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
	STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

	// Now we need to process the padding block
	// For a 64-byte input (32+32), the padding block consists of 0x80 followed by zeros
	// and the 64-bit length (512 bits)

	// TODO merge with endian fixes
	uint8_t padding[64] = {0};
	padding[0] = 0x80;  // Padding start marker

	// Set the 64-bit length value (512 bits = 0x0200)
	padding[56] = 0x00;
	padding[57] = 0x00;
	padding[58] = 0x00;
	padding[59] = 0x00;
	padding[60] = 0x00;
	padding[61] = 0x00;
	padding[62] = 0x02;
	padding[63] = 0x00;

	// Save current state
	ABEF_SAVE = STATE0;
	CDGH_SAVE = STATE1;

	// Load padding block
	MSG0 = vld1q_u32((const uint32_t *)(padding + 0));
	MSG1 = vld1q_u32((const uint32_t *)(padding + 16));
	MSG2 = vld1q_u32((const uint32_t *)(padding + 32));
	MSG3 = vld1q_u32((const uint32_t *)(padding + 48));

	MSG0 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG0)));
	MSG1 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG1)));
	MSG2 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG2)));
	MSG3 = vreinterpretq_u32_u8(vrev32q_u8(vreinterpretq_u8_u32(MSG3)));

	// Rounds 1-4
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[0]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 5-8
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[4]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 9-12
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[8]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 13-16
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[12]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 17-20
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[16]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 21-24
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[20]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 25-28
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[24]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 29-32
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[28]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 33-36
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[32]));
	TMP2 = STATE0;
	MSG0 = vsha256su0q_u32(MSG0, MSG1);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG0 = vsha256su1q_u32(MSG0, MSG2, MSG3);

	// Rounds 37-40
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[36]));
	TMP2 = STATE0;
	MSG1 = vsha256su0q_u32(MSG1, MSG2);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG1 = vsha256su1q_u32(MSG1, MSG3, MSG0);

	// Rounds 41-44
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[40]));
	TMP2 = STATE0;
	MSG2 = vsha256su0q_u32(MSG2, MSG3);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG2 = vsha256su1q_u32(MSG2, MSG0, MSG1);

	// Rounds 45-48
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[44]));
	TMP2 = STATE0;
	MSG3 = vsha256su0q_u32(MSG3, MSG0);
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);
	MSG3 = vsha256su1q_u32(MSG3, MSG1, MSG2);

	// Rounds 49-52
	TMP0 = vaddq_u32(MSG0, vld1q_u32(&K[48]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 53-56
	TMP0 = vaddq_u32(MSG1, vld1q_u32(&K[52]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 57-60
	TMP0 = vaddq_u32(MSG2, vld1q_u32(&K[56]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Rounds 61-64
	TMP0 = vaddq_u32(MSG3, vld1q_u32(&K[60]));
	TMP2 = STATE0;
	STATE0 = vsha256hq_u32(STATE0, STATE1, TMP0);
	STATE1 = vsha256h2q_u32(STATE1, TMP2, TMP0);

	// Update state
	STATE0 = vaddq_u32(STATE0, ABEF_SAVE);
	STATE1 = vaddq_u32(STATE1, CDGH_SAVE);

	/* big-endian, canonical SHA-256 layout */
	uint32_t tmp[8];
	vst1q_u32(&tmp[0], STATE0);
	vst1q_u32(&tmp[4], STATE1);
	for (int i = 0; i < 8; i++) {
		output[(i<<2)+0] = (uint8_t)(tmp[i] >> 24);
		output[(i<<2)+1] = (uint8_t)(tmp[i] >> 16);
		output[(i<<2)+2] = (uint8_t)(tmp[i] >>  8);
		output[(i<<2)+3] = (uint8_t)(tmp[i]      );
	}
}

void _vdf_sha2_fused_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char tempOut[VDF_SHA_HASH_SIZE];
	// 2 different branches for different optimisation cases
	if (skipCheckpointCount == 0) {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);

			sha2_p2_32_32_rev_norm(tempOut, saltBuffer, locIn);
			sha2_p2_32_32_norm_loop(tempOut, saltBuffer, hashingIterations-2);
			sha2_p2_32_32_norm_rev(locOut, saltBuffer, tempOut);
			long_add(saltBuffer, 1);
		}
	} else {
		for(int checkpointIdx = 0; checkpointIdx <= checkpointCount; checkpointIdx++) {
			unsigned char* locIn  = checkpointIdx == 0               ? seed : (outCheckpoint + VDF_SHA_HASH_SIZE*(checkpointIdx-1));
			unsigned char* locOut = checkpointIdx == checkpointCount ? out  : (outCheckpoint + VDF_SHA_HASH_SIZE*checkpointIdx);

			sha2_p2_32_32_rev_norm(tempOut, saltBuffer, locIn);
			// 1 skip on start
			sha2_p2_32_32_norm_loop(tempOut, saltBuffer, hashingIterations-1);
			long_add(saltBuffer, 1);
			for(int j = 1; j < skipCheckpointCount; j++) {
				// no skips
				sha2_p2_32_32_norm_loop(tempOut, saltBuffer, hashingIterations);
				long_add(saltBuffer, 1);
			}
			// 1 skip on end
			sha2_p2_32_32_norm_loop(tempOut, saltBuffer, hashingIterations-1);
			sha2_p2_32_32_norm_rev(locOut, saltBuffer, tempOut);
			long_add(saltBuffer, 1);
		}
	}
}

void vdf_sha2_fused_arm(unsigned char* saltBuffer, unsigned char* seed, unsigned char* out, unsigned char* outCheckpoint, int checkpointCount, int skipCheckpointCount, int hashingIterations) {
	unsigned char saltBufferStack[SALT_SIZE];
	// ensure 1 L1 cache page used
	// no access to heap, except of 0-iteration
	memcpy(saltBufferStack, saltBuffer, SALT_SIZE);

	_vdf_sha2_fused_arm(saltBufferStack, seed, out, outCheckpoint, checkpointCount, skipCheckpointCount, hashingIterations);
}

#endif