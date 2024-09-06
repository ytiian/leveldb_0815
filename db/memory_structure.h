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

namespace leveldb {

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

//Only from cache
Status NotL0Get(int level, const Slice& k, void* arg, Cache* block_cache, const Comparator* cmp, bool* if_search,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)){
  Status s;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  if (block_cache != nullptr) {
    size_t key_len = k.size();
    char cache_key_buffer[sizeof(uint32_t) + key_len];
    EncodeFixed32(cache_key_buffer, level); 
    memcpy(cache_key_buffer + sizeof(uint32_t), k.data(), key_len);
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
    
    cache_handle = block_cache->Lookup(key); // return LRUHandle*
    if (cache_handle != nullptr) {
      //std::cout<<"Lookup1"<<std::endl;
      block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); //Value(cache_handle) : return reinterpret_cast<LRUHandle*>(handle)->value; (void*)
    }
  } 
  
  Iterator* block_iter;
  if(block != nullptr){
    *if_search = true;
    block_iter = block->NewIterator(cmp);
    block_iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle); 
    block_iter->Seek(k);
    if (block_iter->Valid()) {
      (*handle_result)(arg, block_iter->key(), block_iter->value()); 
    }
    s = block_iter->status();
    delete block_iter;
  }
  return s;
}

//only from cache
Status L0Get(const ReadOptions& options, const Slice& k, void* arg, const Slice& reminder_result, Cache* block_cache, 
        TableCache* table_cache, const Comparator* cmp, const std::vector<FileMetaData*>* files,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)){
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  Status s;
  if (block_cache != nullptr) {
    size_t handle_len = reminder_result.size();
    Slice input = reminder_result;
    uint64_t number, offset, size;
    GetVarint64(&input, &number);
    GetVarint64(&input, &offset);
    GetVarint64(&input, &size);
    std::string cache_key;
    PutFixed32(&cache_key, 0);
    PutFixed64(&cache_key, number);
    PutFixed64(&cache_key, offset);
    PutFixed64(&cache_key, size);
    Slice key(cache_key);

    bool io_flag = true; //need to io:1
    cache_handle = block_cache->Lookup(key); 
    if (cache_handle != nullptr) {  //Lookup has already performed an equality check internally
      //std::cout<<"Lookup2"<<std::endl;
      block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); //Value(cache_handle) : return reinterpret_cast<LRUHandle*>(handle)->value; (void*)
      io_flag = false;
      
      Iterator* iter;
      if (block != nullptr) {
        //std::cout<<"L0 cache match"<<std::endl;
        iter = block->NewIterator(cmp);
        iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
      } else {
        iter = NewErrorIterator(s);
        block_cache->Release(cache_handle);
      }
      iter->Seek(k);
      if (iter->Valid()) {
        (*handle_result)(arg, iter->key(), iter->value()); 
      }
      s = iter->status();
      delete iter;
    }
    if(io_flag){
      Slice input = reminder_result;
      uint64_t number ;
      GetVarint64(&input, &number);
      for(uint32_t i = 0; i < files[0].size(); i++){
        FileMetaData* f = files[0][i];
        if(f->number == number){
          s = table_cache->L0Get(options, number, f->file_size, k, arg, input, key, handle_result);      
        }
      }
    }
  }     
  return s;  
}

}  // namespace leveldb

#endif