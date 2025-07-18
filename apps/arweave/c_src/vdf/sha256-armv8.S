.text
.globl	_sha256_block_vdf_order
.align	6
_sha256_block_vdf_order:
	stp	x29,x30,[sp,#-16]!
	add	x29,sp,#0
    adr    x3,K2564

    ld1    {v4.16b,v5.16b,v6.16b,v7.16b},[x1]
	ld1	   {v0.4s,v1.4s},[x0]
    
    ld1    {v16.4s},[x3],#16
    add    v16.4s,v16.4s,v4.4s
    orr    v2.16b,v0.16b,v0.16b
.long    0x5e104020    //sha256h v0.16b,v1.16b,v16.4s
.long    0x5e105041    //sha256h2 v1.16b,v2.16b,v16.4s

    ld1    {v17.4s},[x3],#16
    add    v17.4s,v17.4s,v5.4s
    orr    v2.16b,v0.16b,v0.16b
.long    0x5e114020    //sha256h v0.16b,v1.16b,v17.4s
.long    0x5e115041    //sha256h2 v1.16b,v2.16b,v17.4s
    orr    v20.16b,v0.16b,v0.16b
    orr    v21.16b,v1.16b,v1.16b
    ld1    {v18.4s,v19.4s},[x0]

Loop_hw:
	sub	x2,x2,#1	
//	rev32	v4.16b,v4.16b
//	rev32	v5.16b,v5.16b
//	rev32	v6.16b,v6.16b
//	rev32	v7.16b,v7.16b

    ld1    {v4.16b,v5.16b},[x1]
    orr    v0.16b,v20.16b,v20.16b
    orr    v1.16b,v21.16b,v21.16b
	
.long	0x5e2828a4	//sha256su0 v4.16b,v5.16b
.long	0x5e0760c4	//sha256su1 v4.16b,v6.16b,v7.16b
	ld1	{v16.4s},[x3],#16
.long	0x5e2828c5	//sha256su0 v5.16b,v6.16b
.long	0x5e0460e5	//sha256su1 v5.16b,v7.16b,v4.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v6.4s
.long	0x5e2828e6	//sha256su0 v6.16b,v7.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
.long	0x5e056086	//sha256su1 v6.16b,v4.16b,v5.16b
	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v7.4s
.long	0x5e282887	//sha256su0 v7.16b,v4.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s
.long	0x5e0660a7	//sha256su1 v7.16b,v5.16b,v6.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v4.4s
.long	0x5e2828a4	//sha256su0 v4.16b,v5.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
.long	0x5e0760c4	//sha256su1 v4.16b,v6.16b,v7.16b
	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v5.4s
.long	0x5e2828c5	//sha256su0 v5.16b,v6.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s
.long	0x5e0460e5	//sha256su1 v5.16b,v7.16b,v4.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v6.4s
.long	0x5e2828e6	//sha256su0 v6.16b,v7.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
.long	0x5e056086	//sha256su1 v6.16b,v4.16b,v5.16b
	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v7.4s
.long	0x5e282887	//sha256su0 v7.16b,v4.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s
.long	0x5e0660a7	//sha256su1 v7.16b,v5.16b,v6.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v4.4s
.long	0x5e2828a4	//sha256su0 v4.16b,v5.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
.long	0x5e0760c4	//sha256su1 v4.16b,v6.16b,v7.16b
	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v5.4s
.long	0x5e2828c5	//sha256su0 v5.16b,v6.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s
.long	0x5e0460e5	//sha256su1 v5.16b,v7.16b,v4.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v6.4s
.long	0x5e2828e6	//sha256su0 v6.16b,v7.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
.long	0x5e056086	//sha256su1 v6.16b,v4.16b,v5.16b
	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v7.4s
.long	0x5e282887	//sha256su0 v7.16b,v4.16b
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s
.long	0x5e0660a7	//sha256su1 v7.16b,v5.16b,v6.16b
	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v4.4s
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	add	v17.4s,v17.4s,v5.4s
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s

	ld1	{v17.4s},[x3],#16
	add	v16.4s,v16.4s,v6.4s
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s
	
	add	v17.4s,v17.4s,v7.4s
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e114020	//sha256h v0.16b,v1.16b,v17.4s
.long	0x5e115041	//sha256h2 v1.16b,v2.16b,v17.4s

	add	v6.4s,v0.4s,v18.4s
	add	v7.4s,v1.4s,v19.4s

//64B

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v6.16b,v6.16b
    orr    v0.16b,v6.16b,v6.16b
    orr    v1.16b,v7.16b,v7.16b
//.long   0x5e1040e0  //sha256h v0.16b,v7.16b,v16.4s
.long   0x5e104020  //sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3],#16
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	ld1	{v16.4s},[x3]
	sub	x3,x3,#128*4-48	// rewind
	orr	v2.16b,v0.16b,v0.16b
.long	0x5e104020	//sha256h v0.16b,v1.16b,v16.4s
.long	0x5e105041	//sha256h2 v1.16b,v2.16b,v16.4s

	add	v6.4s,v0.4s,v6.4s
	add	v7.4s,v1.4s,v7.4s
	
	cbnz	x2,Loop_hw

	st1	{v6.4s,v7.4s},[x0]

	ldr	x29,[sp],#16
	ret
	
K2564:
.long	0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5
.long	0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5
.long	0xd807aa98,0x12835b01,0x243185be,0x550c7dc3
.long	0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174
.long	0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc
.long	0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da
.long	0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7
.long	0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967
.long	0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13
.long	0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85
.long	0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3
.long	0xd192e819,0xd6990624,0xf40e3585,0x106aa070
.long	0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5
.long	0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3
.long	0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208
.long	0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
.long	0xC28A2F98,0x71374491,0xB5C0FBCF,0xE9B5DBA5		//64B
.long	0x3956C25B,0x59F111F1,0x923F82A4,0xAB1C5ED5
.long	0xD807AA98,0x12835B01,0x243185BE,0x550C7DC3
.long	0x72BE5D74,0x80DEB1FE,0x9BDC06A7,0xC19BF374
.long	0x649B69C1,0xF0FE4786,0x0FE1EDC6,0x240CF254
.long	0x4FE9346F,0x6CC984BE,0x61B9411E,0x16F988FA
.long	0xF2C65152,0xA88E5A6D,0xB019FC65,0xB9D99EC7
.long	0x9A1231C3,0xE70EEAA0,0xFDB1232B,0xC7353EB0
.long	0x3069BAD5,0xCB976D5F,0x5A0F118F,0xDC1EEEFD
.long	0x0A35B689,0xDE0B7A04,0x58F4CA9D,0xE15D5B16
.long	0x007F3E86,0x37088980,0xA507EA32,0x6FAB9537
.long	0x17406110,0x0D8CD6F1,0xCDAA3B6D,0xC0BBBE37
.long	0x83613BDA,0xDB48A363,0x0B02E931,0x6FD15CA7
.long	0x521AFACA,0x31338431,0x6ED41A95,0x6D437890
.long	0xC39C91F2,0x9ECCABBD,0xB5C9A0E6,0x532FB63C
.long	0xD2C741C6,0x07237EA3,0xA4954B68,0x4C191D76
.long	0	//terminator
