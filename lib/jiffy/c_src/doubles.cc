#include "double-conversion/double-conversion.h"

#define BEGIN_C extern "C" {
#define END_C }


namespace dc = double_conversion;


BEGIN_C

int
double_to_shortest(char* buf, size_t size, size_t* len, double val)
{
    int flags = dc::DoubleToStringConverter::UNIQUE_ZERO |
                dc::DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN |
                dc::DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT |
                dc::DoubleToStringConverter::EMIT_TRAILING_ZERO_AFTER_POINT;

    dc::StringBuilder builder(buf, size);
    dc::DoubleToStringConverter conv(flags, NULL, NULL, 'e', -6, 21, 6, 0);

    if(!conv.ToShortest(val, &builder)) {
        return 0;
    }

    *len = (size_t) builder.position();
    builder.Finalize();

    return 1;
}

END_C
