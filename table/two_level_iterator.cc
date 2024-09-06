// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/caller_type.h"
#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      file_function_(nullptr),
      cache_function_(nullptr),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      cache_iter_(nullptr),
      data_iter_(nullptr),
      level_(0) {}

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   FileFunction file_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(nullptr),
      file_function_(file_function),
      cache_function_(nullptr),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      cache_iter_(nullptr),
      data_iter_(nullptr),
      level_(0) {}

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter, Iterator* cache_iter, const uint32_t& level,
                                   CacheFunction cache_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(nullptr),
      file_function_(nullptr),
      cache_function_(cache_function),
      arg_(arg),
      level_(level),
      options_(options),
      index_iter_(index_iter),
      cache_iter_(cache_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Slice largest = index_iter_.key();
      Iterator* iter;
      if(block_function_ != nullptr){
        iter = (*block_function_)(arg_, options_, handle, CallerType::kCallerTypeUnknown);
      }
      if(file_function_ != nullptr){
        const Slice& left_bound = index_iter_.left_bound();
        const Slice& right_bound = index_iter_.right_bound();
        iter = (*file_function_)(arg_, options_, handle, left_bound, right_bound, CallerType::kCallerTypeUnknown);
      }
      if(cache_function_!= nullptr){
        const Slice& right_bound = index_iter_.right_bound();
        const Slice& index_key = index_iter_.key();
        iter = (*cache_function_)(arg_, options_, index_key, handle, cache_iter_.iter(), right_bound, level_);
      }
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

//}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              FileFunction file_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, file_function, arg, options);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter, Iterator* cache_iter,
                              const uint32_t& level,
                              CacheFunction cache_function, void* arg, 
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, cache_iter, level, cache_function, arg, options);
}

}  // namespace leveldb
