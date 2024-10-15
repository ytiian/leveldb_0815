// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>
#include "caller_type.h"

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <mutex>

namespace leveldb {

class LEVELDB_EXPORT Cache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(uint64_t capacity, bool is_monitor = false);

LEVELDB_EXPORT Cache* NewSkipListLRUCache(uint64_t capacity, const Comparator* internal_cmp, bool is_monitor);

class LEVELDB_EXPORT Cache {
 public:
  Cache(bool is_monitor) : is_monitor_(is_monitor), cache_misses_(0), cache_insert_(0), stop_thread_(false) {
    if (is_monitor_) {
      StartCacheHitRateCalculator();
      std::cout << "Cache Hit Rate Calculator Started" << std::endl;
    }
  }

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache(){
    if(is_monitor_){
      stop_thread_.store(true);
      cv_.notify_all();
      if (hit_rate_thread_.joinable()) {
        hit_rate_thread_.join();
      }
    }
  }

  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, uint64_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  virtual Handle* Insert(const Slice& key, void* value, uint64_t charge,
                         void (*deleter)(const Slice& key, void* value), const Slice& min_key, const uint64_t& file_number) {return nullptr;}

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  virtual uint64_t TotalCharge() const = 0;

  /*virtual Slice BlockMinKey(Handle* handle) const = 0;

  virtual Slice BlockMaxKey(Handle* handle) const = 0;*/

  virtual Slice Key(Handle* handle) const = 0;

  virtual Iterator* NewIterator(const Slice& left_bound, const uint64_t& file_number, const int& which, bool* compaction_state) {
    return nullptr;
  }

  virtual Iterator* NewIterator(const Slice& left_bound, const Slice& right_bound) {
    return nullptr;
  }

  void IncrementCacheHits(CallerType caller) {
    if( is_monitor_ && caller == CallerType::kGet){
      cache_hits_.fetch_add(1, std::memory_order_relaxed);
    } 
  }

  // µÝÔöcache_misses_
  void IncrementCacheMisses(CallerType caller) {
    if( is_monitor_ && caller == CallerType::kGet){
      cache_misses_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void IncrementCacheInsert(CallerType caller) {
    if( is_monitor_ && caller == CallerType::kGet){
      cache_insert_.fetch_add(1, std::memory_order_relaxed);
    }
  }

 private:
  bool is_monitor_;
  std::atomic<uint64_t> cache_hits_ = 0;
  std::atomic<uint64_t> cache_misses_ = 0;
  std::atomic<uint64_t> cache_insert_ = 0;
  std::thread hit_rate_thread_;
  std::mutex mtx_;
  std::condition_variable cv_;
  std::atomic<bool> stop_thread_;

  void CalculateCacheHitRatePerSecond() {
    uint64_t previous_hits = 0;
    uint64_t previous_misses = 0;

    while (!stop_thread_) {
      std::unique_lock<std::mutex> lock(mtx_);
      cv_.wait_for(lock, std::chrono::seconds(1), [this]() { return stop_thread_.load(); });
      if (stop_thread_) break;

      uint64_t current_hits = cache_hits_.load(std::memory_order_relaxed);
      uint64_t current_misses = cache_misses_.load(std::memory_order_relaxed);
      uint64_t current_insert = cache_insert_.load(std::memory_order_relaxed);
      uint64_t hits = current_hits - previous_hits;
      uint64_t misses = current_misses - previous_misses;
      uint64_t total = hits + misses;

      double hit_rate = total > 0 ? static_cast<double>(hits) / total : 0.0;

      std::time_t nowTime = std::time(nullptr);

      std::cout << nowTime << "; Cache Hit Rate (Only Get): " << hit_rate * 100 
      << "%; total_access: " << current_hits + current_misses 
      <<"; total_insert: "<< current_insert 
      << "; total_miss:" << current_misses << std::endl;

      previous_hits = current_hits;
      previous_misses = current_misses;
    }
  }

  void StartCacheHitRateCalculator() {
    hit_rate_thread_ = std::thread([this]() { CalculateCacheHitRatePerSecond(); });
  }

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
