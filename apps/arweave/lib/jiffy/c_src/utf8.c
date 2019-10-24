// This file is part of Jiffy released under the MIT license.
// See the LICENSE file for more information.
#include "jiffy.h"
#include <stdio.h>

static const unsigned char hexvals[256] = {
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
      0,   1,   2,   3,   4,   5,   6,   7,
      8,   9, 255, 255, 255, 255, 255, 255,
    255,  10,  11,  12,  13,  14,  15, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255,  10,  11,  12,  13,  14,  15, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255
};

static const char hexdigits[16] = {
    '0', '1', '2', '3',
    '4', '5', '6', '7',
    '8', '9', 'A', 'B',
    'C', 'D', 'E', 'F'
};

int
int_from_hex(const unsigned char* p)
{
    unsigned char* h = (unsigned char*) p;
    int ret;

    if(hexvals[*(h+0)] == 255) return -1;
    if(hexvals[*(h+1)] == 255) return -1;
    if(hexvals[*(h+2)] == 255) return -1;
    if(hexvals[*(h+3)] == 255) return -1;

    ret = (hexvals[*(h+0)] << 12)
        + (hexvals[*(h+1)] << 8)
        + (hexvals[*(h+2)] << 4)
        + (hexvals[*(h+3)] << 0);

    return ret;
}

int
int_to_hex(int val, char* p)
{
    if(val < 0 || val > 65535)
        return -1;

    p[0] = hexdigits[(val >> 12) & 0xF];
    p[1] = hexdigits[(val >> 8) & 0xF];
    p[2] = hexdigits[(val >> 4) & 0xF];
    p[3] = hexdigits[val & 0xF];

    return 1;
}

int
utf8_len(int c)
{
    if(c < 128) {
        return 1;
    } else if(c < 0x800) {
        return 2;
    } else if(c < 0x10000) {
        if(c < 0xD800 || (c > 0xDFFF)) {
            return 3;
        } else {
            return -1;
        }
    } else if(c <= 0x10FFFF) {
        return 4;
    } else {
        return -1;
    }
}

int
utf8_esc_len(int c)
{
    if(c < 0x10000) {
        return 6;
    } else if(c <= 0x10FFFF) {
        return 12;
    } else {
        return -1;
    }
}

int
utf8_validate(unsigned char* data, size_t size)
{
    int ulen = -1;
    int ui;

    if((data[0] & 0x80) == 0x00) {
        ulen = 1;
    } if((data[0] & 0xE0) == 0xC0) {
        ulen = 2;
    } else if((data[0] & 0xF0) == 0xE0) {
        ulen = 3;
    } else if((data[0] & 0xF8) == 0xF0) {
        ulen = 4;
    }
    if(ulen < 0 || ulen > size) {
        return -1;
    }

    // Check each continuation byte.
    for(ui = 1; ui < ulen; ui++) {
        if((data[ui] & 0xC0) != 0x80) return -1;
    }

    // Wikipedia says I have to check that a UTF-8 encoding
    // uses as few bits as possible. This means that we
    // can't do things like encode 't' in three bytes.
    // To check this all we need to ensure is that for each
    // of the following bit patterns that there is at least
    // one 1 bit in any of the x's
    //  1: 0yyyyyyy
    //  2: 110xxxxy 10yyyyyy
    //  3: 1110xxxx 10xyyyyy 10yyyyyy
    //  4: 11110xxx 10xxyyyy 10yyyyyy 10yyyyyy

    // ulen == 1 passes by definition
    if(ulen == 2) {
        if((data[0] & 0x1E) == 0)
            return -1;
    } else if(ulen == 3) {
        if((data[0] & 0x0F) + (data[1] & 0x20) == 0)
            return -1;
    } else if(ulen == 4) {
        if((data[0] & 0x07) + (data[1] & 0x30) == 0)
            return -1;
    }

    // Lastly we need to check some miscellaneous ranges for
    // some of the larger code point values.
    if(ulen >= 3) {
        ui = utf8_to_unicode(data, ulen);
        if(ui < 0) {
            return -1;
        } else if(ui >= 0xD800 && ui <= 0xDFFF) {
            return -1;
        } else if(ui > 0x10FFFF) {
            return -1;
        }
    }

    return ulen;
}

int
utf8_to_unicode(unsigned char* buf, size_t size)
{
    int ret;
    if((buf[0] & 0x80) == 0x00) {
        // 0xxxxxxx
        ret = (int) buf[0];
    } else if((buf[0] & 0xE0) == 0xC0 && size >= 2) {
        // 110xxxxy 10yyyyyy
        ret = ((buf[0] & 0x1F) << 6)
            | ((buf[1] & 0x3F));
    } else if((buf[0] & 0xF0) == 0xE0 && size >= 3) {
        // 1110xxxx 10xyyyyy 10yyyyyy
        ret = ((buf[0] & 0x0F) << 12)
            | ((buf[1] & 0x3F) << 6)
            | ((buf[2] & 0x3F));
        if(ret >= 0xD800 && ret <= 0xDFFF) {
            ret = -1;
        }
    } else if((buf[0] & 0xF8) == 0xF0 && size >= 4) {
        // 11110xxx 10xxyyyy 10yyyyyy 10yyyyyy
        ret = ((buf[0] & 0x07) << 18)
            | ((buf[1] & 0x3F) << 12)
            | ((buf[2] & 0x3F) << 6)
            | ((buf[3] & 0x3F));
    } else {
        ret = -1;
    }
    return ret;
}

int
unicode_to_utf8(int c, unsigned char* buf)
{
    if(c < 0x80) {
        buf[0] = (unsigned char) c;
        return 1;
    } else if(c < 0x800) {
        buf[0] = (unsigned char) 0xC0 + (c >> 6);
        buf[1] = (unsigned char) 0x80 + (c & 0x3F);
        return 2;
    } else if(c < 0x10000) {
        if(c < 0xD800 || (c > 0xDFFF)) {
            buf[0] = (unsigned char) 0xE0 + (c >> 12);
            buf[1] = (unsigned char) 0x80 + ((c >> 6) & 0x3F);
            buf[2] = (unsigned char) 0x80 + (c & 0x3F);
            return 3;
        } else {
            return -1;
        }
    } else if(c < 0x10FFFF) {
        buf[0] = (unsigned char) 0xF0 + (c >> 18);
        buf[1] = (unsigned char) 0x80 + ((c >> 12) & 0x3F);
        buf[2] = (unsigned char) 0x80 + ((c >> 6) & 0x3F);
        buf[3] = (unsigned char) 0x80 + (c & 0x3F);
        return 4;
    }
    return -1;
}

int
unicode_from_pair(int hi, int lo)
{
    if(hi < 0xD800 || hi >= 0xDC00) return -1;
    if(lo < 0xDC00 || lo > 0xDFFF) return -1;
    return ((hi & 0x3FF) << 10) + (lo & 0x3FF) + 0x10000;
}

int
unicode_uescape(int val, char* p)
{
    int n;
    if(val < 0x10000) {
        p[0] = '\\';
        p[1] = 'u';
        if(int_to_hex(val, p+2) < 0) {
            return -1;
        }
        return 6;
    } else if (val <= 0x10FFFF) {
        n = val - 0x10000;
        p[0] = '\\';
        p[1] = 'u';
        if(int_to_hex((0xD800 | ((n >> 10) & 0x03FF)), p+2) < 0) {
            return -1;
        }
        p[6] = '\\';
        p[7] = 'u';
        if(int_to_hex((0xDC00 | (n & 0x03FF)), p+8) < 0) {
            return -1;
        }
        return 12;
    }
    return -1;
}

