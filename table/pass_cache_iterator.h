// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_PASS_CACHE_H_
#define STORAGE_LEVELDB_PASS_CACHE_H_

#include "leveldb/slice.h"

namespace leveldb {

class Comparator;
class Iterator;


Iterator* NewPassCacheIterator(const Comparator* comparator, Iterator* need_iter, 
                              Iterator* cache_iter, const Slice largest, const int& level);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_MERGER_H_
