// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>

#include "folly/ConcurrentSkipList.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/coding.h"
#include "db/dbformat.h"

namespace leveldb {

//Cache::~Cache() {}

struct KvWrapper {
  void* value;
  size_t cache_key_length; // cache_key_length
  size_t key_data_length; // min_key_length = key_data_length - cache_key_length
  uint64_t file_number;
  bool if_level0;    // Whether entry is in level0.
  char data[1];  //Passed in by the caller
  // format: L0: cache_key
  //         >L0: cache_key + min_key

  Slice key() const { //return cache_key
    return Slice(data, cache_key_length);
  }

  Slice MaxKey() const{
    //only level>0
    assert(!if_level0);
    return Slice(data + 4, cache_key_length - 4); //level: uint32_t
  }

  Slice MinKey() const{
    //only level>0
    assert(!if_level0);
    return Slice(data + cache_key_length, key_data_length - cache_key_length);
  }

  uint32_t level() const{
    Slice level(data, 4);
    return DecodeFixed32(level.data());
  }

  uint64_t FileNumber() const{
    return file_number;
  }
};

struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  uint64_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;     // Whether entry is in the cache.
  bool in_skiplist;
  uint32_t refs;     // References, including cache reference, if present.
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  KvWrapper* kv;      //for skiplist cache
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }

  LRUHandle(Slice key): deleter(nullptr), next(nullptr), prev(nullptr), charge(0), in_cache(false), in_skiplist(false), refs(0) {
    kv = reinterpret_cast<KvWrapper*>(malloc(sizeof(KvWrapper) - 1 + key.size()));
    std::memcpy(kv->data, key.data(), key.size());
    kv->cache_key_length = key.size();
  }

  LRUHandle() : deleter(nullptr), next(nullptr), prev(nullptr), charge(0), in_cache(false), in_skiplist(false), refs(0), kv(nullptr) {}

  LRUHandle(KvWrapper* h):kv(h) { }

  KvWrapper* GetKV(){
    return kv;
  }

  KvWrapper* GetKV() const{
    return kv;
  }

  void free_kv(){
    if (kv != nullptr) {
      free(kv);
      kv = nullptr;
    }
  }

  ~LRUHandle() {
    free_kv();
  }

};

class CacheComparator : public Comparator {

 public:
  const InternalKeyComparator internal_comparator_;
  //[todo] is correct?
  explicit CacheComparator() : internal_comparator_(BytewiseComparator()) {};
  explicit CacheComparator(const Comparator* c) : internal_comparator_(c) {}
  const char* Name() const override;
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override; //to do
  void FindShortSuccessor(std::string* key) const override;//to do
  int UserCompare(const Slice& akey, const Slice& bkey) const;
  const Comparator* user_comparator() const { return internal_comparator_.user_comparator(); }
  bool operator()(LRUHandle* const& lhs, LRUHandle* const& rhs) const;
};

const char* CacheComparator::Name() const { 
  return "leveldb.SkipListCacheComparator";
}

int CacheComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing level (according to user-supplied comparator) //4bit
  // == L0: 
  //    increasing sst id //8bit
  //  increasing block offset //8bit
  // > L0:
  //    increasing max_key
  int a_level = DecodeFixed32(akey.data());
  int b_level = DecodeFixed32(bkey.data());
  int r = (a_level < b_level) ? -1 : (a_level > b_level) ? 1 : 0;
  if(r == 0){
    if(a_level == 0){
      uint64_t a_sst_id = DecodeFixed32(akey.data() + 4);
      uint64_t b_sst_id = DecodeFixed32(bkey.data() + 4);
      r = (a_sst_id < b_sst_id) ? -1 : (a_sst_id > b_sst_id) ? 1 : 0;
      if(r == 0){
        uint64_t a_offset = DecodeFixed32(akey.data() + 12);
        uint64_t b_offset = DecodeFixed32(bkey.data() + 12);
        r = (a_offset < b_offset) ? -1 : (a_offset > b_offset) ? 1 : 0;
      }
    }
    else{
      r = internal_comparator_.Compare(Slice(akey.data() + 4, akey.size() - 4), Slice(bkey.data() + 4, bkey.size() - 4));
    }
  }
  return r;
}

int CacheComparator::UserCompare(const Slice& akey, const Slice& bkey) const {
  const Comparator* user_cmp = internal_comparator_.user_comparator();
  return user_cmp->Compare(Slice(akey.data(), akey.size() - 8), Slice(bkey.data(), bkey.size() - 8));
}

void CacheComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {}

void CacheComparator::FindShortSuccessor(std::string* key) const {}

bool CacheComparator::operator()(LRUHandle* const& lhs, LRUHandle* const& rhs) const{
  if(lhs->GetKV() == nullptr){
    return true;
  }
  if(rhs->GetKV() == nullptr){
    return false;
  }
  return Compare(lhs->GetKV()->key(), rhs->GetKV()->key()) < 0;
}


using SkipListT = folly::ConcurrentSkipList<LRUHandle*, CacheComparator>;
using SkipListAccessor = SkipListT::Accessor;
using SkipListSkipper = SkipListT::Skipper;

/************************************************ */
//wrapper for SkipListT
class SkipListBase { //Skiplist
 public:
  explicit SkipListBase(CacheComparator cmp, Arena* arena);

  SkipListBase(const SkipListBase&) = delete;
  SkipListBase& operator=(const SkipListBase&) = delete;

  LRUHandle* Lookup(const Slice& key) { 
    LRUHandle* x = nullptr;
    int key_level = DecodeFixed32(key.data());

    {
      SkipListAccessor accessor(sl_);
      LRUHandle tmp(key);
      SkipListT::iterator iter = accessor.lower_bound(&tmp);
      if(iter != accessor.end()){
        x = *iter;
      }
    }

    if(x != nullptr && key_level == 0){
      if(Equal(key, x->GetKV()->key())){//Accurate search in L0
        return x;
      }
      x = nullptr;
    }    

    if(x != nullptr && key_level != 0){
      Slice key_target = Slice(key.data() + 4, key.size() - 4);
      Slice cache_key = x->GetKV()->key();
      int cache_level = DecodeFixed32(cache_key.data());

      /*std::cout << "target level vs find level:"<<cache_level<<","<<key_level<<std::endl;
      std::cout<<"target key:"<<key_target.ToString()<<std::endl;
      std::cout<<"min_key:"<<x->GetKV()->MinKey().ToString()<<std::endl;
      std::cout<<"max_key:"<<x->GetKV()->MaxKey().ToString()<<std::endl;
      std::cout<<"******************************"<<std::endl<<std::endl;*/
      if(cache_level == key_level 
              && compare_.UserCompare(key_target, x->GetKV()->MaxKey()) <= 0 
              && compare_.UserCompare(key_target, x->GetKV()->MinKey()) >= 0){
        return x;
      }
      x = nullptr;
    }

    return x; 
  }

  //does not allow duplicate insertion
  bool Insert(LRUHandle* e) { 
    SkipListAccessor accessor(sl_);
    auto ret = accessor.insert(e);
    //0: not insert; 1: insert success
    return ret.second;
  }

  LRUHandle* Remove(LRUHandle* e){
    bool ret;
    {
      SkipListAccessor accessor(sl_);
      ret = accessor.erase(e);
    }
    if(ret){
      return e;
    }else{
      return nullptr;
    }
  }

  //[todo]
  LRUHandle* NewNode(KvWrapper* h) {
    return nullptr;
  }


 private:

  bool Equal(const Slice& a, const Slice& b) const { return (compare_.Compare(a, b) == 0); }

  std::shared_ptr<SkipListT> sl_;

  CacheComparator const compare_;

  Arena* const arena_;  

};

SkipListBase::SkipListBase(CacheComparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      sl_(SkipListT::createInstance()) {}

class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  bool Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    if(old != nullptr){
      return false;
    }
    h->next_hash = nullptr;
    *ptr = h;
    ++elems_;
    if (elems_ > length_) {
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
    return true;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
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
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
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
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
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

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(uint64_t capacity) { capacity_ = capacity; }

  void SetSkiplist(SkipListBase* skiplist_table) { skiplist_table_ = skiplist_table; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        uint64_t charge,
                        void (*deleter)(const Slice& key, void* value), const bool& dual_insert,
                        const Slice& sl_key,
                        const Slice& min_key, const uint64_t& file_number);
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void AddRef(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  uint64_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  uint64_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  uint64_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);

  SkipListBase* skiplist_table_;

};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert((!e->in_skiplist && e->refs == 1) || (e->in_skiplist && e->refs == 2) );  // Invariant of lru_ list.
    Unref(e);
    if(e->in_skiplist){
      e->in_skiplist = false;
      Unref(e);
    }
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if(e->in_cache && !e->in_skiplist){
    if(e->refs == 1){
      LRU_Remove(e);
      LRU_Append(&in_use_, e);      
    }
  }else if(e->in_cache && e->in_skiplist){
    if(e->refs == 2){
      LRU_Remove(e);
      LRU_Append(&in_use_, e);         
    }
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && !e->in_skiplist && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  } else if(e->in_cache && e->in_skiplist && e->refs == 2){
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

void LRUCache::AddRef(Cache::Handle* handle){
  MutexLock l(&mutex_);
  Ref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                uint64_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value),
                                const bool& dual_insert, 
                                const Slice& sl_key,
                                const Slice& min_key, const uint64_t& file_number) {
  MutexLock l(&mutex_);

  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->in_skiplist = false;
  e->refs = 1;  // for the returned handle.
  e->next = nullptr;
  std::memcpy(e->key_data, key.data(), key.size());

  if(dual_insert){
    KvWrapper* kv = 
          reinterpret_cast<KvWrapper*>(malloc(sizeof(KvWrapper) - 1 + sl_key.size() + min_key.size()));
    kv->value = value;
    kv->cache_key_length = sl_key.size();
    kv->key_data_length = sl_key.size() + min_key.size();
    kv->if_level0 = min_key.size() == 0 ? 1 : 0; //min_key.size()=0 -> if_level0 = true
    kv->file_number = file_number;  
    std::memcpy(kv->data, sl_key.data(), sl_key.size());
    if(min_key.size() > 0){
      std::memcpy(kv->data + sl_key.size(), min_key.data(), min_key.size());
    }
    e->kv = kv;
  }

  if (capacity_ > 0) {
    bool if_insert = table_.Insert(e);
    if(if_insert){
      e->refs++;  // for the cache's reference.
      e->in_cache = true;
      LRU_Append(&in_use_, e);
      usage_ += charge;
      if(dual_insert){
        bool skipkist_insert = skiplist_table_->Insert(e);
        assert(skipkist_insert);
        if(skipkist_insert){
          e->refs++;
          e->in_skiplist = true;
        }
      }
    }
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert((!old->in_skiplist && old->refs == 1) || (old->in_skiplist && old->refs == 2));
    if(old->in_skiplist){
      LRUHandle* result = skiplist_table_->Remove(old);
      result->in_skiplist = false;
      assert(result != nullptr);
      Unref(result);
    }
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if(e != nullptr && e -> in_skiplist){
    LRUHandle* result = skiplist_table_ -> Remove(e);
    if(result != nullptr){
      e->in_skiplist = false;
      Unref(e);
    }
  }  
  FinishErase(e);
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedDualCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
  SkipListBase skiplist_table_;
  Arena arena_ GUARDED_BY(mutex_);


  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedDualCache(uint64_t capacity, const Comparator* user_cmp, bool is_monitor = false) : 
    last_id_(0), Cache(is_monitor), skiplist_table_(CacheComparator(user_cmp), &arena_)  {
    const uint64_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
      shard_[s].SetSkiplist(&skiplist_table_);
    }
  }
  ~ShardedDualCache() override {}
  Handle* Insert(const Slice& key, void* value, uint64_t charge,
                 void (*deleter)(const Slice& key, void* value), 
                 const bool& dual_insert, const Slice& skiplist_key, const Slice& min_key, const uint64_t& file_number) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter, dual_insert, skiplist_key, min_key, file_number);
  }
  Handle* Lookup(const Slice& key, const bool& is_skiplist) override {
    if(is_skiplist){
      LRUHandle* e = skiplist_table_.Lookup(key);
      if(e != nullptr){
        const uint32_t hash = e->hash;
        Cache::Handle* result = reinterpret_cast<Cache::Handle*>(e);
        //shard_[Shard(hash)].AddRef(result);//外部决定是否引用
        return result;
      }
      return nullptr;
    }
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void AddRef(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].AddRef(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void Erase(Handle* handle, const bool& clean_all) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    if(h->in_skiplist){
      LRUHandle* result = skiplist_table_.Remove(h);
      if(result != nullptr){
        result->in_skiplist = false;
        Cache::Handle* r = reinterpret_cast<Cache::Handle*>(result);
        Release(r);
      }
    }
    if(clean_all){
      shard_[Shard(h->hash)].Erase(h->key(), h->hash);
    }
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  uint64_t TotalCharge() const override {
    uint64_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

Cache* NewDualCache(uint64_t capacity, const Comparator* user_cmp, bool is_monitor) {
   return new ShardedDualCache(capacity, user_cmp, is_monitor); 
}

}  // namespace leveldb
