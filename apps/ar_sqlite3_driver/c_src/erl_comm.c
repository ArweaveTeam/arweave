#include <unistd.h>
#include "erl_comm.h"
#define PACKET_SIZE 2

int read_exact(byte *buf, int len);
int write_exact(byte *buf, int len);

int read_cmd(byte *buf)
{
	int len;

	if (read_exact(buf, PACKET_SIZE) != PACKET_SIZE)
	{
		return 0;
	}

	len = (buf[0] << 8) | buf[1];
	return read_exact(buf, len);
}

int write_cmd(byte *buf, int len)
{
	byte li;

	li = (len >> 8) & 0xff;
	write_exact(&li, 1);

	li = len & 0xff;
	write_exact(&li, 1);

	return write_exact(buf, len);
}

int read_exact(byte *buf, int len)
{
	ssize_t i;
	int got = 0;

	while (got < len)
	{
		i = read(0, buf + got, len - got);
		if (i <= 0) return 0;
		got += i;
	}

	return len;
}

int write_exact(byte *buf, int len)
{
	ssize_t i;
	int wrote = 0;

	while (wrote < len)
	{
		i = write(1, buf + wrote, len - wrote);
		if (i <= 0) return 0;
		wrote += i;
	}

	return len;
}
