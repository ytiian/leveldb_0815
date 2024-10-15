#ifndef STORAGE_LEVELDB_DB_SAVER_H_
#define STORAGE_LEVELDB_DB_SAVER_H_

#include <atomic>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "dbformat.h"

namespace leveldb {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
  std::atomic<int> status[config::kNumLevels];
};
} // namespace

#endif 