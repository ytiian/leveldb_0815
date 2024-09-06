// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "util/coding.h"

namespace leveldb {

namespace {
  
class PassCacheIterator : public Iterator {
 public:
  PassCacheIterator(const Comparator* comparator, Iterator* need_iter, 
                              Iterator* cache_iter, const Slice largest, const int& level)
      : comparator_(comparator),
        current_(nullptr),
        direction_(kForward),
        largest_(largest),
        level_(level) {
          need_iter_->Set(need_iter);
          cache_iter_->Set(cache_iter);
        }

  ~PassCacheIterator() override {
    delete need_iter_;
    delete cache_iter_;
  } //delete[] children_;?

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    need_iter_->SeekToFirst();
    cache_iter_->SeekToFirst();
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override{

  }

  //only for scan, not need to Seek?
  void Seek(const Slice& target) override {
  /*  need_iter_->Seek(target);
    cache_iter_->Seek(target);
    FindSmallest();
    direction_ = kForward;*/
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    assert(direction_ = kForward);

    current_->Next();
    FindSmallest();
  }

  void Prev() override{
    
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    status = need_iter_->status();
    if (!status.ok()) {
        return status;
    }
    status = cache_iter_->status();
    if (!status.ok()) {
        return status;
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* need_iter_;
  IteratorWrapper* cache_iter_;
  IteratorWrapper* current_;
  Direction direction_;
  const Slice largest_;
  const int& level_;
};

void PassCacheIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  int cache_level;
  if (need_iter_->Valid() && cache_iter_->Valid()) {
    if (comparator_->Compare(need_iter_->key(), cache_iter_->key()) >= 0 &&
           comparator_->Compare(largest_, cache_iter_->key())) { //[todo] level judge
      current_ = cache_iter_;
    } else {
      current_ = need_iter_;
    }
  } else if (need_iter_->Valid()) {
    current_ = need_iter_;
  } else if (cache_iter_->Valid()) {
    current_ = cache_iter_;
  } else {
    current_ = nullptr;
  }
}

}  // namespace

Iterator* NewPassCacheIterator(const Comparator* comparator, Iterator* need_iter, 
                              Iterator* cache_iter, const Slice largest, const int& level) {
    return new PassCacheIterator(comparator, need_iter, cache_iter, largest, level);
}
//Iterator* NewPassCacheIterator(const Comparator* comparator, Iterator* need_iter, 
//                              Iterator* cache_iter, const Slice largest);
}  // namespace leveldb
