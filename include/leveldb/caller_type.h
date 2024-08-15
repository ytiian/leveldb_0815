// common.h
#ifndef COMMON_H
#define COMMON_H

namespace leveldb {

enum class CallerType {
    kGet,
    kCompaction,
    kCallerTypeUnknown
};

}  // namespace leveldb

#endif  // COMMON_H