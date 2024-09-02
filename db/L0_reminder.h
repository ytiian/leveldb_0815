#ifndef STORAGE_LEVELDB_DB_L0_REMINDER_H_
#define STORAGE_LEVELDB_DB_L0_REMINDER_H_

#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <iostream>
#include <unordered_map>
#include <condition_variable>

#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "util/coding.h"
#include "util/hash.h"

namespace leveldb {

struct L0ReminderEntry {
  Slice key;
  Slice value;
  L0ReminderEntry(const Slice& k, const Slice& v) : key(k), value(v) {}
};


struct TableHandle { 
  //key是user_key
  //value是 <fileID + offset>
  void (*deleter)(const Slice&, void* value);
  TableHandle* next_hash;
  uint64_t charge;  
  size_t key_length;
  size_t value_length;
  bool in_cache;     // Whether entry is in the cache.
  uint32_t refs;     // References, including cache reference, if present.
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  char key_data[1];  // Beginning of key

  Slice key() const {
    return Slice(key_data, key_length);
  }

  Slice value() const {
    return Slice(key_data + key_length, value_length);
  }
};

class L0_Reminder_HashTable {
 public:
  L0_Reminder_HashTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~L0_Reminder_HashTable() { delete[] list_; }

  TableHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  TableHandle* Insert(TableHandle* h) {
    TableHandle** ptr = FindPointer(h->key(), h->hash);
    TableHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  TableHandle* Remove(const Slice& key, uint32_t hash) {
    TableHandle** ptr = FindPointer(key, hash);
    TableHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  TableHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  TableHandle** FindPointer(const Slice& key, uint32_t hash) {
    TableHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    TableHandle** new_list = new TableHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      TableHandle* h = list_[i];
      while (h != nullptr) {
        TableHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        TableHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

class L0_Reminder{
public:
  L0_Reminder(){};
  void WriteToReminder(const Slice& user_key, const Slice& value);
  //void RemoveFromReminder();
  TableHandle* ReadFromReminder(const Slice& user_key); //返回这个key所在的位置
  void Release(TableHandle* handle);

private:
  L0_Reminder_HashTable hash_table;
  void Ref(TableHandle* handle);
  void Unref(TableHandle* handle);
};

static inline uint32_t HashSlice(const Slice& s) {
  return Hash(s.data(), s.size(), 0);
}


} // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_L0_REMINDER_H_