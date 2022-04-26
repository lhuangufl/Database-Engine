#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <sstream>
#include <stdlib.h>

namespace Util {
    
    std::string randomStr(const int len);
    
    template<typename T>
    std::string toString(T& a) {
        std::stringstream ss;
        ss << a;
        return ss.str();
    } 

    template<typename T>
    T fromString(std::string& s) {
        T res;
        std::istringstream(s) >> res;
        return res; 
    } 

    template<typename T>
    int catArrays(T* merged, T* left, T* right, int leftLength, int rightLength) {
        for (int i = 0; i < leftLength; i++) {
            merged[i] = left[i];
        }
        for (int j = 0; j < rightLength; j++) {
            merged[leftLength+j] = right[j];
        }
        return leftLength + rightLength;
    }
}

#endif