#ifndef STORAGE_LEVELDB_DB_MEMORY_STRUCTURE_H_
#define STORAGE_LEVELDB_DB_MEMORY_STRUCTURE_H_

#include "leveldb/slice.h"
#include "leveldb/cache.h"
#include "util/cache.cc"
#include "util/coding.h"
#include "table/format.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "db/dbformat.h"
#include "util/hash.h"
#include "db/version_edit.h"
#include "leveldb/table.h"
#include "db/saver.h"

namespace leveldb {

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

void ReadBlockFromCache(int level, const Slice& k, void* arg, Cache* block_cache,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)){
  Status s;
  Saver* saver = reinterpret_cast<Saver*>(arg);
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  if (block_cache != nullptr) {
    if(saver->status[level].load(std::memory_order_seq_cst) == SEARCH_NOT_FOUND 
      || saver->status[level].load(std::memory_order_seq_cst) == SEARCH_FOUND_BLOCK){
      return ;
    }
    size_t key_len = k.size();
    char cache_key_buffer[sizeof(uint32_t) + key_len];
    EncodeFixed32(cache_key_buffer, level); 
    memcpy(cache_key_buffer + sizeof(uint32_t), k.data(), key_len);
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
    cache_handle = block_cache->Lookup(key, true); // return LRUHandle*
    if (cache_handle != nullptr) {
      saver->cache_handle = cache_handle;
      //std::cout<<"Lookup1"<<std::endl;
      //block_cache -> IncrementCacheHits(CallerType::kGet);
      //block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); //Value(cache_handle) : return reinterpret_cast<LRUHandle*>(handle)->value; (void*)
      saver->status[level].store(SEARCH_FOUND_BLOCK, std::memory_order_seq_cst);
    } else{
      saver->status[level].store(SEARCH_NEED_IO, std::memory_order_seq_cst);
    }
  }
}

}  // namespace leveldb

#endif