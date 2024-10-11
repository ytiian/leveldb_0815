// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

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

//cache_key: L0 <levelid + sst id + block offset>
//>L0: <levelid + max_key> 

//value: data block
//namespace {
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

struct Node {
  void (*deleter)(const Slice&, void* value);
  Node* cache_next;
  Node* cache_prev;
  uint64_t charge;  
  bool in_cache;     // Whether entry is in the cache.
  uint32_t refs;     // References, including cache reference, if present.

//  KvWrapper* kv = 
//        reinterpret_cast<KvWrapper*>(malloc(sizeof(KvWrapper) - 1 + key.size() + min_key.size()));
  Node(Slice key): deleter(nullptr), cache_next(nullptr), cache_prev(nullptr), charge(0), in_cache(false), refs(0) {
    kv = reinterpret_cast<KvWrapper*>(malloc(sizeof(KvWrapper) - 1 + key.size()));
    std::memcpy(kv->data, key.data(), key.size());
    kv->cache_key_length = key.size();
  }

  Node() : deleter(nullptr), cache_next(nullptr), cache_prev(nullptr), charge(0), in_cache(false), refs(0), kv(nullptr) {}

  explicit Node(KvWrapper* h):kv(h) { }

  KvWrapper* GetKV(){
    return kv;
  }

  KvWrapper* GetKV() const{
    return kv;
  }

  ~Node() {
    if (kv != nullptr) {
        free(kv);  // 释放通过 malloc 动态分配的 kv 空间
        kv = nullptr;
    }
  }

 private:
  KvWrapper* kv; //Actual stored objects
};

/************************************************ */
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
  bool operator()(Node* const& lhs, Node* const& rhs) const;
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

bool CacheComparator::operator()(Node* const& lhs, Node* const& rhs) const{
  return Compare(lhs->GetKV()->key(), rhs->GetKV()->key()) < 0;
}

using SkipListT = folly::ConcurrentSkipList<Node*, CacheComparator>;
using SkipListAccessor = SkipListT::Accessor;
using SkipListSkipper = SkipListT::Skipper;

/************************************************ */
//wrapper for SkipListT
class SkipListBase { //Skiplist
 public:
  explicit SkipListBase(CacheComparator cmp, Arena* arena);

  SkipListBase(const SkipListBase&) = delete;
  SkipListBase& operator=(const SkipListBase&) = delete;


  Node* Lookup(const Slice& key) { 
    Node* x = nullptr;
    int key_level = DecodeFixed32(key.data());

    SkipListAccessor accessor(sl_);
    Node tmp(key);
    SkipListT::iterator iter = accessor.lower_bound(&tmp);
    if(iter != accessor.end()){
      x = *iter;
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
  bool Insert(Node* e) { 
    SkipListAccessor accessor(sl_);
    auto ret = accessor.insert(e);
    //0: not insert; 1: insert success
    return ret.second;
  }

  Node* Remove(Node* e){
    SkipListAccessor accessor(sl_);
    auto ret = accessor.erase(e);
    if(ret){
      return e;
    }else{
      return nullptr;
    }
  }

  Node* NewNode(KvWrapper* h) {
    void* memory = malloc(sizeof(Node));
    if (!memory) {
        // faild to allocate memory
        return nullptr;
    }
    return new (memory) Node(h);
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
/****************************************************************************************************** */

// A single shard of sharded cache.
class SkipListLRUCache {
 public:
  SkipListLRUCache(const Comparator* internal_cmp) ;
  ~SkipListLRUCache();

  // Separate from constructor so caller can easily make an array of SkipListLRUCache
  void SetCapacity(uint64_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, void* value,
                        uint64_t charge,
                        void (*deleter)(const Slice& key, void* value), const Slice& min_key, const uint64_t& file_number);
  Cache::Handle* Insert(const Slice& key, void* value,
                        uint64_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key);
  void Prune();
  uint64_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(Node* e);
  void LRU_Append(Node* list, Node* e);
  void Ref(Node* e);
  void Unref(Node* e);
  bool FinishErase(Node* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void InsertThread();
  // Initialized before use.
  uint64_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  uint64_t usage_ GUARDED_BY(mutex_);

  mutable port::Mutex list_mutex_;
  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  Node lru_ GUARDED_BY(list_mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  Node in_use_ GUARDED_BY(list_mutex_);

  Arena arena_ GUARDED_BY(mutex_);

  SkipListBase table_ GUARDED_BY(mutex_);

  std::thread insert_thread_;

  std::queue<Node*> insert_queue_;

  std::condition_variable queue_cv_;

  std::mutex queue_mutex_;

  std::atomic<bool> has_items{false};  

};

SkipListLRUCache::SkipListLRUCache(const Comparator* user_cmp) : 
              capacity_(0), usage_(0), table_(CacheComparator(user_cmp), &arena_) {
  // Make empty circular linked lists.
  lru_.cache_next = &lru_;
  lru_.cache_prev = &lru_;
  in_use_.cache_next = &in_use_;
  in_use_.cache_prev = &in_use_;
  insert_thread_ = std::thread(&SkipListLRUCache::InsertThread, this);
}

SkipListLRUCache::~SkipListLRUCache() {
  int num=0;
  for(Node* e = in_use_.cache_next; e != &in_use_;) {
    num++;
  }
  assert(in_use_.cache_next == &in_use_);  // Error if caller has an unreleased handle
  for (Node* e = lru_.cache_next; e != &lru_;) {
    Node* next = e->cache_next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

//[Lock when calling] -> list_mutex_
void SkipListLRUCache::Ref(Node* e) {
  //std::cout<<"Ref"<<std::endl;
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

//[Lock when calling] -> list_mutex_
void SkipListLRUCache::Unref(Node* e) {
  //std::cout<<"UnRef"<<std::endl;
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->GetKV()->key(), e->GetKV()->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
      LRU_Remove(e);
      LRU_Append(&lru_, e);
  }
}

void SkipListLRUCache::LRU_Remove(Node* e) {
  if(e->cache_next != nullptr){
    e->cache_next->cache_prev = e->cache_prev;
    e->cache_prev->cache_next = e->cache_next;
  }
}

void SkipListLRUCache::LRU_Append(Node* list, Node* e) {
  // Make "e" newest entry by inserting just before *list
  e->cache_next = list;
  e->cache_prev = list->cache_prev;
  e->cache_prev->cache_next = e;
  e->cache_next->cache_prev = e;
}

Cache::Handle* SkipListLRUCache::Lookup(const Slice& key) {
  Node* e = nullptr;
  e = table_.Lookup(key);
  if (e != nullptr) {
    {
      MutexLock l(&list_mutex_);
      Ref(e);
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void SkipListLRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&list_mutex_);
  //std::cout<<"Release"<<std::endl;
  Unref(reinterpret_cast<Node*>(handle));
}

Cache::Handle* SkipListLRUCache::Insert(const Slice& key, void* value,
                                uint64_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value), const Slice& min_key, 
                                                        const uint64_t& file_number) {
  //MutexLock l(&mutex_);
  KvWrapper* kv = 
        reinterpret_cast<KvWrapper*>(malloc(sizeof(KvWrapper) - 1 + key.size() + min_key.size()));
  kv->value = value;
  kv->cache_key_length = key.size();
  kv->key_data_length = key.size() + min_key.size();
  kv->if_level0 = min_key.size() == 0 ? 1 : 0; //min_key.size()=0 -> if_level0 = true
  kv->file_number = file_number;
  std::memcpy(kv->data, key.data(), key.size());
  if(min_key.size() > 0){
    std::memcpy(kv->data + key.size(), min_key.data(), min_key.size());
  }

  int height;
  Node* e = table_.NewNode(kv);
  e->deleter = deleter;
  e->charge = charge;
  e->refs = 1;  // for the returned handle. Unref by iter's RegisterCleanup
  e->in_cache = false;
  //std::cout<<"Insert"<<std::endl;
  if (capacity_ > 0) {
    {
    e->refs++;  // for the cache's reference.[LRU_Append]
      std::unique_lock<std::mutex> lock(queue_mutex_); 
      insert_queue_.push(e);
      has_items.store(true, std::memory_order_release);
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void SkipListLRUCache::InsertThread() {
    while (true) {
      if(has_items.load(std::memory_order_acquire)){
        std::unique_lock<std::mutex> lock(queue_mutex_);
        while (!insert_queue_.empty()) {
            Node* e = insert_queue_.front();
            insert_queue_.pop();
            lock.unlock(); // 解锁以允许其他线程访问队列

            /*{
              MutexLock l(&list_mutex_);
              e->in_cache = true;
              if(e->refs == 1){
                LRU_Append(&lru_, e);
              }else{
                LRU_Append(&in_use_, e);
              }
              usage_ += e->charge;
            }*/
            bool if_insert = table_.Insert(e);
            //does not allow duplicate insertion, does not need to return and erase
            //assert(if_insert);
            /*if(!if_insert) {
              std::cout<<"Duplicate insertion"<<std::endl;
              {
                MutexLock l(&list_mutex_);
                e->refs--;
                e->in_cache = false;
                usage_ -= e->charge;
              }
            }*/
            if(if_insert){
              {
                MutexLock l(&list_mutex_);
                e->in_cache = true;
                if(e->refs == 1){
                  LRU_Append(&lru_, e);
                }else{
                  LRU_Append(&in_use_, e);
                }
                usage_ += e->charge;
              }
            }else{
              std::cout<<"Duplicate insertion"<<std::endl;
            }

            while (usage_ > capacity_ && lru_.cache_next != &lru_) {
              Node* old = lru_.cache_next;
              assert(old->refs == 1);
              bool erased = FinishErase(table_.Remove(old));
              if (!erased) {  // to avoid unused variable when compiled NDEBUG
                assert(erased);
              }
            }        
            lock.lock(); // 重新加锁以检查队列
        }
        has_items.store(false, std::memory_order_release);
      }
    }
}

Cache::Handle* SkipListLRUCache::Insert(const Slice& key, void* value,
                                uint64_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {                                            
    return Insert(key, value, charge, deleter, Slice(), 0);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool SkipListLRUCache::FinishErase(Node* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void SkipListLRUCache::Erase(const Slice& key) {
  MutexLock l(&mutex_);
  //[todo] only read-write mix called
}

void SkipListLRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.cache_next != &lru_) {
    Node* e = lru_.cache_next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}


class LRUCacheWrapper : public Cache {
 private:
  SkipListLRUCache lru_cache_;

 public:
  explicit LRUCacheWrapper(uint64_t capacity, const Comparator* user_cmp, bool is_monitor = false) : Cache(is_monitor), lru_cache_(user_cmp) {
    lru_cache_.SetCapacity(capacity);
  }
  ~LRUCacheWrapper() override {}
  Handle* Insert(const Slice& key, void* value, uint64_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    return lru_cache_.Insert(key, value, charge, deleter);
  }
  Handle* Insert(const Slice& key, void* value, uint64_t charge,
                 void (*deleter)(const Slice& key, void* value), const Slice& min_key, const uint64_t& file_number) override{
    return lru_cache_.Insert(key, value, charge, deleter, min_key, file_number);
  }
  Handle* Lookup(const Slice& key) override {
    return lru_cache_.Lookup(key);
  }
  void Release(Handle* handle) override {
    lru_cache_.Release(handle);
  }
  void Erase(const Slice& key) override {
    lru_cache_.Erase(key);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<Node*>(handle)->GetKV()->value;
  }

  uint64_t NewId() override {// not used
    return 0;
  }

  void Prune() override {
      lru_cache_.Prune();
  }
  uint64_t TotalCharge() const override {
    uint64_t total = 0;
    total += lru_cache_.TotalCharge();
    return total;
  }

  Slice Key(Handle* handle) const override {
    return reinterpret_cast<Node*>(handle)->GetKV()->key();
  }

  //[todo]
  Iterator* NewIterator(const Slice& left_bound, const uint64_t& file_number) override {
    //return lru_cache_.NewCacheIterator(left_bound, file_number);
    return nullptr;
  }
};

//}  // end anonymous namespace

Cache* NewSkipListLRUCache(uint64_t capacity, const Comparator* user_cmp, bool is_monitor) { 
  return new LRUCacheWrapper(capacity, user_cmp, is_monitor); 
}

}  // namespace leveldb
