#include "Util.h"

std::string Util::randomStr(const int len) {
        static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        std::string s;
        for (int i = 0; i < len; ++i) {
            s += alphanum[rand() % (sizeof(alphanum) - 1)];
        }
        return s;
}