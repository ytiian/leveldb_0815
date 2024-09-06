// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include<iostream>

#include "leveldb/iterator.h"
#include "leveldb/caller_type.h"
#include "leveldb/slice.h"
#include "leveldb/options.h"
#include "table/iterator_wrapper.h"
#include "util/coding.h"
#include <cstdint>

namespace leveldb {

struct ReadOptions;

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&, const CallerType&);

typedef Iterator* (*FileFunction)(void*, const ReadOptions&, const Slice&, const Slice&, const Slice&, const CallerType&);

typedef Iterator* (*CacheFunction)(void*, const ReadOptions&, const Slice&,const Slice&, Iterator*, const Slice&, const int&);

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  TwoLevelIterator(Iterator* index_iter, FileFunction block_function,
                   void* arg, const ReadOptions& options);

  TwoLevelIterator(Iterator* index_iter, Iterator* cache_iter,
                    const uint32_t& level,
                   CacheFunction cache_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

  bool IfCache() override{
    return data_iter_.IfCache();
  }

  Slice keyAndHandle(uint64_t* offset, uint64_t* size) {
    assert(Valid());
    Slice block_handle(data_block_handle_.data(), data_block_handle_.size());
    GetVarint64(&block_handle, offset);
    GetVarint64(&block_handle , size);
    return key();
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  FileFunction file_function_;
  CacheFunction cache_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  IteratorWrapper cache_iter_;
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
  const uint32_t level_;
};
// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg, const ReadOptions& options,
                                const Slice& index_value, const CallerType& caller_type),
    void* arg, const ReadOptions& options);

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*file_function)(void* arg, const ReadOptions& options,
                                const Slice& index_value, const Slice& left_bound,
                                const Slice& right_bound, 
                                const CallerType& caller_type),
    void* arg, const ReadOptions& options);

Iterator* NewTwoLevelIterator(
    Iterator* index_iter, Iterator* cache_iter, const uint32_t& level,
    Iterator* (*cache_function)(void* arg, const ReadOptions& options,
        const Slice& index_key ,const Slice& index_value, Iterator* cache_iter, 
        const Slice& right_bound, const int& level),
    void* arg, const ReadOptions& options);


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
