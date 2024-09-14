// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/caller_type.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "table/pass_cache_iterator.h"
#include "util/coding.h"
#include "leveldb/comparator.h"
#include "db/dbformat.h"
#include <iostream>

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t file_number;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table, uint64_t file_number) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->file_number = file_number;
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

//only for L0
Iterator* Table::BlockReaderWithoutCache(void* arg, const ReadOptions& options,
                             const Slice& index_value, const CallerType& caller_type){
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Iterator* iter;
  if (s.ok()) {
    BlockContents contents;
    assert(block_cache != nullptr);
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
        if (block != nullptr) {
          iter = block->NewIterator(table->rep_->options.comparator);
          if (cache_handle == nullptr) {
            iter->RegisterCleanup(&DeleteBlock, block, nullptr);
          } else {
            iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
          }
        } else {
          iter = NewErrorIterator(s);
        }
      }
    } 
  return iter;
}

//only for >L0
Iterator* Table::BlockReaderWithoutCache(void* arg, const ReadOptions& options, 
                             const Slice& index_value, const int& level,
                             const Comparator* ucmp, const Slice& k,
                             const CallerType& caller_type){
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Iterator* iter;
  if (s.ok()) {
    BlockContents contents;
    assert(block_cache != nullptr);
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      block_cache -> IncrementCacheMisses(caller_type);
      if (s.ok()) {
        block = new Block(contents);
        if (block != nullptr) {
          iter = block->NewIterator(table->rep_->options.comparator);
        } else {
          iter = NewErrorIterator(s);
        }
        if (contents.cachable && options.fill_cache) {
          iter->SeekToFirst();
          assert(iter->Valid());
          std::string min = iter->key().ToString();

          iter->SeekToLast();
          assert(iter->Valid());
          std::string max = iter->key().ToString();

          Slice min_key(min);
          Slice max_key(max); 
          char cache_key_buffer[sizeof(uint32_t) + max_key.size()];
          EncodeFixed32(cache_key_buffer, level);
          memcpy(cache_key_buffer + sizeof(uint32_t), max_key.data(), max_key.size());
          Slice cache_key(cache_key_buffer, sizeof(cache_key_buffer));

          //[todo] test compaction, need to delete
          /*if(ucmp == nullptr){
            cache_handle = block_cache->Insert(cache_key, block, block->size(),
                                    &DeleteCachedBlock, min_key);
          }*/

          if(ucmp != nullptr && ucmp->Compare(Slice(min_key.data(), min_key.size() - 8), Slice(k.data(), k.size() - 8)) <= 0 
              && ucmp->Compare(Slice(max_key.data(), max_key.size() - 8), Slice(k.data(), k.size() - 8)) >= 0){
            cache_handle = block_cache->Insert(cache_key, block, block->size(),
                                                &DeleteCachedBlock, min_key, table->rep_->file_number);
          }
          if(cache_handle != nullptr){
            iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
          }else{
            iter->RegisterCleanup(&DeleteBlock, block, nullptr);
          }
          //block_cache -> IncrementCacheInsert(caller_type);
        }
      }
    } 
  return iter;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
/*Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value, const CallerType& caller_type) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
        block_cache -> IncrementCacheHits(caller_type);
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        block_cache -> IncrementCacheMisses(caller_type);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
            block_cache -> IncrementCacheInsert(caller_type);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}*/

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReaderWithoutCache, const_cast<Table*>(this), options);
}

Iterator* Table::NewIterator(const ReadOptions& options, 
    const Slice& left_bound, const Slice& right_bound, const int& level, const uint64_t& file_number) const {
  Cache* block_cache = rep_->options.block_cache;
  char cache_key_buffer[sizeof(uint32_t) + left_bound.size()];
  EncodeFixed32(cache_key_buffer, level); 
  memcpy(cache_key_buffer + sizeof(uint32_t), left_bound.data(), left_bound.size());  
  Slice key(cache_key_buffer, sizeof(cache_key_buffer));
  Iterator* cache_iter = block_cache->NewIterator(key, file_number);
  cache_iter->Seek(key);
  //std::cout<<"cache_iter valid:"<<cache_iter->Valid()<<std::endl;
  //std::cout<<"cache_iter seek:"<< key.ToString() <<std::endl;
  //[todo] Cleanup?
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator, false, right_bound), cache_iter, level,
       &Table::BlockReadFromStoreOrCache, const_cast<Table*>(this), options);
}

/*Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      Iterator* block_iter = BlockReader(this, options, iiter->value(), CallerType::kGet);
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}*/

namespace {
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
}  // namespace

Status Table::InternalGetByIO(const ReadOptions& options, const Slice& k, void* arg, const int& level, const Comparator* ucmp,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)){
  Status s;
  Saver* saver = reinterpret_cast<Saver*>(arg);
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      while(saver->status[level].load(std::memory_order_seq_cst) == SEARCH_BEGIN_CACHE_SEARCH){}
      if(saver->status[level].load(std::memory_order_seq_cst) == SEARCH_NEED_IO){
        //std::cout<<"start read block"<<std::endl;
        Iterator* block_iter = BlockReaderWithoutCache(this, options, iiter->value(), level, ucmp, k, CallerType::kGet);
        block_iter->Seek(k);
        if (block_iter->Valid()) {
          (*handle_result)(arg, block_iter->key(), block_iter->value());
        }
        s = block_iter->status();
        delete block_iter;      
      }
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

Status Table::GetWithOffset(const ReadOptions& options, const Slice& k, void* arg, const Slice& block_handle, const Slice& cache_key,
                            void (*handle_result)(void*, const Slice&, const Slice&)) {
  //It must be the block from IO
  Status s;
  BlockHandle handle;
  Cache* block_cache = rep_->options.block_cache;
  Block* block = nullptr;
  BlockContents contents;
  Slice input = block_handle;
  Cache::Handle* cache_handle = nullptr;
  handle.DecodeFrom(&input);
  s = ReadBlock(rep_->file, options, handle, &contents);
  if (s.ok()) {
    block = new Block(contents);
    block_cache -> IncrementCacheMisses(CallerType::kGet);
    if (contents.cachable && options.fill_cache) {//contents.cachable && options.fill_cache
      // std::cout<<"insertL0"<<std::endl;
      cache_handle = block_cache->Insert(cache_key, block, block->size(),
                                          &DeleteCachedBlock);
    }    
  }
  Iterator* block_iter;
  if (block != nullptr) {
    block_iter = block->NewIterator(rep_->options.comparator);
    if (cache_handle == nullptr) {
      block_iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      block_iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    block_iter = NewErrorIterator(s);
  }
  block_iter->Seek(k);
  if (block_iter->Valid()) {
    (*handle_result)(arg, block_iter->key(), block_iter->value());
  }
  s = block_iter->status();
  delete block_iter;  
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

Iterator* Table::BlockReadFromStoreOrCache(void* arg, const ReadOptions& options,
        const Slice& index_key ,const Slice& index_value, Iterator* cache_iter, 
        const Slice& right_bound, const int& level){
  Table* table = reinterpret_cast<Table*>(arg);
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;
  Cache* block_cache = table->rep_->options.block_cache;
  const Comparator* cmp = table->rep_->options.comparator;

          //std::cout << "level: " << level << std::endl;
          //std::cout << "index_key and right boudn:" << index_key.ToString() << " " << right_bound.ToString() << std::endl;
  Slice largest_key = (cmp->Compare(index_key, right_bound) < 0) ? index_key : right_bound;
          //std::cout << "largest result:" << largest_key.ToString()<< std::endl;
          //std::cout<<"cache_iter valid:"<<cache_iter->Valid()<<std::endl;
  char cache_key_buffer[sizeof(uint32_t) + largest_key.size()];
  EncodeFixed32(cache_key_buffer, level); 
  memcpy(cache_key_buffer + sizeof(uint32_t), largest_key.data(), largest_key.size());  
  Slice largest(cache_key_buffer, sizeof(cache_key_buffer)); 
  
  Iterator* iter;
  if(cache_iter->Valid()){
    cache_handle = reinterpret_cast<Cache::Handle*>(cache_iter->TestAndReturn(largest));
  }
  if(cache_handle != nullptr){
    block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
    iter = block->NewIterator(cmp, true);
    iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    assert(iter->IfCache());
    //iter->SetIfCache();
  } else{
    //[todo] no need to insert
    iter = table->BlockReaderWithoutCache(table, options, index_value, level, nullptr, Slice(), CallerType::kCompaction);
  }
  return iter;
}

}  // namespace leveldb
