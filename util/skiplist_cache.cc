// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

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
  int height;
  bool in_cache;     // Whether entry is in the cache.
  uint32_t refs;     // References, including cache reference, if present.

  Node() : deleter(nullptr), cache_next(nullptr), cache_prev(nullptr), charge(0), in_cache(false), refs(0), kv(nullptr) {
    next_[0].store(nullptr, std::memory_order_relaxed);
  }

  explicit Node(KvWrapper* h):kv(h) { }

  Node* Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }

  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

  KvWrapper* GetKV(){
    return kv;
  }

 private:
  KvWrapper* kv; //Actual stored objects
  std::atomic<Node*> next_[1];
};


/************************************************ */
class CacheComparator : public Comparator {

 public:
  const InternalKeyComparator internal_comparator_;
  explicit CacheComparator(const Comparator* c) : internal_comparator_(c) {}
  const char* Name() const override;
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override; //to do
  void FindShortSuccessor(std::string* key) const override;//to do
  int UserCompare(const Slice& akey, const Slice& bkey) const;
  const Comparator* user_comparator() const { return internal_comparator_.user_comparator(); }

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

class SkipListBase { //Skiplist
 public:
  explicit SkipListBase(CacheComparator cmp, Arena* arena);

  SkipListBase(const SkipListBase&) = delete;
  SkipListBase& operator=(const SkipListBase&) = delete;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipListBase* list, const Slice& left_bound,const uint64_t& file_number);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    Node* node() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    //void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Slice& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //void SeekToLast();

    Slice key() const;

    Node* TestAndReturn(Slice right_bound);

   private:
    const SkipListBase* list_;
    Node* now_node_;
    Node* next_node_;
    const uint64_t expected_file_;
    Slice left_bound_;
    std::vector<char> left_bound_data_;

    void SetLeftBound(const Slice& left_bound){
      left_bound_data_.resize(left_bound.size());
      memcpy(left_bound_data_.data(), left_bound.data(), left_bound.size());
      left_bound_ = Slice(left_bound_data_.data(), left_bound.size());
    }
    // Intentionally copyable
  };

/*void PrintLast64Bits(const std::string& str) {
  if (str.size() < 8) {
          std::cerr << "The string is too short to have 64 bits." << std::endl;
          return;
      }

      uint64_t result = 0; // 用于存储56位的结果
      for (size_t i = str.size() - 8; i < str.size(); ++i) {
          unsigned char byte = str[i];
          unsigned char high_7_bits = byte >> 1; // 提取高7位
          result = (result << 7) | high_7_bits; // 将高7位拼接到结果中
      }

      std::cout << "The 56-bit number in decimal: " << result << std::endl;
}*/

  Node* Lookup(const Slice& key) { 
    Node* x = FindGreaterOrEqual(key, nullptr);
    int key_level = DecodeFixed32(key.data());
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
      
      /*if(cache_level == key_level){
        std::cout<<"Level MATCH!!!"<<std::endl;
      }
      if(compare_.internal_comparator_.Compare(key_target, x->GetKV()->MaxKey()) <= 0){
        std::cout<<"MaxKey MATCH!!!"<<std::endl;
      }
      if(compare_.internal_comparator_.Compare(key_target, x->GetKV()->MinKey()) >= 0){
        std::cout<<"MinKey MATCH!!!"<<std::endl;
      }
      std::cout<<"*************************"<<std::endl<<std::endl;*/
      /*std::cout<<"Lookup: cache_level "<<cache_level<<"; key_level "<<key_level<<std::endl;
      std::cout<<"Lookup: key_target "<<key_target.ToString()<<std::endl;
      PrintLast64Bits(key_target.ToString());
      std::cout<<"Lookup: MinKey "<<x->GetKV()->MinKey().ToString()<<std::endl;
      PrintLast64Bits(x->GetKV()->MinKey().ToString());
      std::cout<<"Lookup: MaxKey "<<x->GetKV()->MaxKey().ToString()<<std::endl;
      PrintLast64Bits(x->GetKV()->MaxKey().ToString());
      std::cout<<"key vs max"<<compare_.UserCompare(key_target, x->GetKV()->MaxKey())<<std::endl;
      std::cout<<"key vs min"<<compare_.UserCompare(key_target, x->GetKV()->MinKey())<<std::endl;*/
      if(cache_level == key_level 
              && compare_.UserCompare(key_target, x->GetKV()->MaxKey()) <= 0 
              && compare_.UserCompare(key_target, x->GetKV()->MinKey()) >= 0){
        return x;
      }
      x = nullptr;
    }

    return x; 
  }

  Node* CreateNode(KvWrapper* h, int* height_return){
    int height = RandomHeight();
    *height_return = height;
    return NewNode(h, height);
  }

  //does not allow duplicate insertion
  bool Insert(Node* e, const int& height) { 
    // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
    // here since Insert() is externally synchronized.
    Node* prev[kMaxHeight];
    KvWrapper* h = e -> GetKV();
    Node* x = FindGreaterOrEqual(h->key(), prev);
    uint32_t level;
    Slice tmp = h->key();
    GetVarint32(&tmp, &level);
    /*std::cout<<"Insert: level "<< level<<std::endl;
    std::cout<<"Insert: MinKey "<<h->MinKey().ToString()<<std::endl;
    std::cout<<"Insert: MaxKey "<<h->MaxKey().ToString()<<std::endl;*/
    if(x != nullptr && Equal(h->key(), x->GetKV()->key())){
      //std::cout<<"Duplicate Insert want: "<<h->MinKey().ToString()<<" "<<h->MaxKey().ToString()<<" "<<h->FileNumber() <<std::endl;
      //std::cout<<"Duplicate Insert exit: "<<x->GetKV()->MinKey().ToString()<<" "<<x->GetKV()->MaxKey().ToString()<<" "<<x->GetKV()->FileNumber() <<std::endl;
      return false;
    }
    // Our data structure does not allow duplicate insertion
    assert(x == nullptr || !Equal(h->key(), x->GetKV()->key()));

    //int height = RandomHeight();
    if (height > GetMaxHeight()) {
      for (int i = GetMaxHeight(); i < height; i++) {
        prev[i] = head_;
      }
      max_height_.store(height, std::memory_order_relaxed);
    }

    //x = NewNode(h, height);
    for (int i = 0; i < height; i++) {
      e->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
      prev[i]->SetNext(i, e);
    }  
    return true;
  }

  Node* Remove(const Slice& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);
    if (x != nullptr && Equal(key, x->GetKV()->key())) {
      for (int i = 0; i < GetMaxHeight(); i++) {
        if (prev[i]->Next(i) == x) {
          prev[i]->SetNext(i, x->Next(i));
        }
      }
      return x;
    } else {
      return nullptr;
    }
  } 
  //When to Release deleted Node?
  //SkipListLRUCache::FinishErase(Node* e); this e from Remove return

  Node* GetHead() const { return head_; }
 private:
  enum { kMaxHeight = 12 };
  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(KvWrapper* key, int height);
  Node* FindGreaterOrEqual(const Slice& key, Node** prev) const;
  Node* FindLessThan(const Slice& key) const;
  bool KeyIsAfterNode(const Slice& key, Node* n) const;
  int RandomHeight();
  bool Equal(const Slice& a, const Slice& b) const { return (compare_.Compare(a, b) == 0); }

  CacheComparator const compare_;

  Arena* const arena_;  

  Node* const head_;

  std::atomic<int> max_height_; 

  Random rnd_;

};


inline SkipListBase::Iterator::Iterator(const SkipListBase* list, const Slice& left_bound, const uint64_t& file_number):
    list_(list), now_node_(nullptr), next_node_(nullptr), expected_file_(file_number){
      SetLeftBound(left_bound);
}

inline bool SkipListBase::Iterator::Valid() const {
  return (now_node_ != nullptr && now_node_ != list_->GetHead());
}

inline Node* SkipListBase::Iterator::node() const {
  assert(Valid());
  return now_node_;
}

inline void SkipListBase::Iterator::Next() {

}

/*inline void SkipListBase::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}*/

inline void SkipListBase::Iterator::Seek(const Slice& target) {
  now_node_ = list_->FindLessThan(target);
  next_node_ = now_node_;
}

inline void SkipListBase::Iterator::SeekToFirst() {
  //node_ = list_->head_->Next(0);
}

inline Slice SkipListBase::Iterator::key() const{
  //return node_->GetKV()->key();
  return Slice();
}

/*template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}*/

inline Node* SkipListBase::Iterator::TestAndReturn(Slice right_bound){
  //std::cout<<"assert:" <<now_node_->GetKV()->MaxKey().ToString()<<" "<<right_bound.ToString()<<std::endl;
  //assert(list_->compare_.Compare(now_node_->GetKV()->key(), left_bound_) < 0);
  assert(now_node_ == next_node_);
  const Comparator* user_cmp = list_->compare_.user_comparator();
  while(1){
    next_node_ = now_node_->Next(0);
    uint32_t next_node_level = next_node_->GetKV()->level();
    uint32_t bound_level = DecodeFixed32(right_bound.data());

    Slice min_key, max_key;
    
    if(next_node_level != 0){
      min_key = next_node_->GetKV()->MinKey();
      max_key = next_node_->GetKV()->MaxKey();
    }

    uint64_t file_number = next_node_->GetKV()->FileNumber();
    //todo
    /*std::cout<<std::endl<<std::endl;
    std::cout<<"level"<<next_node_level<<" "<<bound_level<<std::endl;
    std::cout<<"bound:"<< Slice(left_bound_.data()+4, left_bound_.size()-4).ToString() <<" "<< 
                Slice(right_bound.data()+4, right_bound.size()-4).ToString() <<std::endl;
    std::cout<<"now_node:"<<now_node_->GetKV()->MinKey().ToString()<<" "<<now_node_->GetKV()->MaxKey().ToString()<<std::endl;            
    std::cout<<"next_node:"<<min_key.ToString()<<" "<<max_key.ToString()<<std::endl;*/
    
    if(next_node_level == bound_level && 
      user_cmp->Compare(min_key, Slice(left_bound_.data()+4, left_bound_.size()-4)) >= 0 && 
        user_cmp->Compare(max_key, Slice(right_bound.data()+4, right_bound.size()-4)) <= 0){
        //std::cout<<"compaction cache match"<<std::endl;
        if(file_number != expected_file_){
          now_node_ = next_node_;
          continue;
        }
        now_node_ = next_node_;
        SetLeftBound(right_bound);
        return now_node_;
    }else if( next_node_level < bound_level || (next_node_level == bound_level &&
      user_cmp->Compare(max_key, Slice(left_bound_.data()+4, left_bound_.size()-4)) <= 0)){
      now_node_ = next_node_;
      //std::cout<<"max_key < left_bound"<<std::endl;
      continue;
    }else if( next_node_level > bound_level || (next_node_level == bound_level &&
      user_cmp->Compare(min_key, Slice(right_bound.data()+4, right_bound.size()-4)) >= 0)){
      next_node_ = now_node_;
      SetLeftBound(right_bound);
      //std::cout<<"right_bound < min_key"<<std::endl;
      return nullptr;
    }else{//[todo] need to delete
      assert(file_number != expected_file_);
      //std::cout<<"another case"<<std::endl;
      now_node_ = next_node_;
      continue;
    }
  }
}

SkipListBase::SkipListBase(CacheComparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(nullptr, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

Node* SkipListBase::NewNode(KvWrapper* h, int height) {
  /*char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(h);*/
  void* memory = malloc(sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  if (!memory) {
      // 处理内存分配失败
      return nullptr;
  }

  // 在分配的内存上调用 Node 的构造函数
  return new (memory) Node(h);
}

Node* SkipListBase::FindGreaterOrEqual(const Slice& key, Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

Node* SkipListBase::FindLessThan(const Slice& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_.Compare(x->GetKV()->key(), key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_.Compare(next->GetKV()->key(), key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

bool SkipListBase::KeyIsAfterNode(const Slice& key, Node* n) const {
  // null n is considered infinite
  if(n == nullptr){
    return false;
  }
  if(n -> GetKV() == nullptr){
    return true;
  }
  return (compare_.Compare(n->GetKV()->key(), key) < 0);
}

int SkipListBase::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

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

  class CacheIterator : public Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit CacheIterator(const SkipListBase* list, SkipListLRUCache* cache, const Slice& left_bound, const uint64_t& file_number)
                                  : list_cache_(list, left_bound, file_number), cache_(cache), now_node_(nullptr){}

    //[todo] ~CacheIterator() {}?
    // Returns true iff the iterator is positioned at a valid node.
    ~CacheIterator(){
      if(now_node_ != nullptr){
        cache_->Unref(now_node_);
      }
    }

    bool Valid() const override{
      return list_cache_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override{
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& target) override{
      list_cache_.Seek(target);
      if(list_cache_.Valid()){
        now_node_ = list_cache_.node();
        cache_->Ref(now_node_);//one for iter, no return
      }
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override{
      /*list_cache_.SeekToFirst();
      if(list_cache_.Valid()){
        now_node_ = list_cache_.node();
        cache_->Ref(now_node_);
      }*/
    }

    void SeekToLast() override{}

    void Prev() override{}

    Slice key() const override{
      assert(Valid());
      return list_cache_.key();
    }

    Slice value() const override{}

    Status status() const override{
      return Status::OK();
    }

    /*void* node() override{
      assert(Valid());
      cache_->Ref(next_node_);
      return list_cache_.node();
    }*/

    void* TestAndReturn(Slice right_bound) override{
      assert(Valid());
      Node* last_node = now_node_;
      Node* node = list_cache_.TestAndReturn(right_bound);
      if(node != nullptr && node != now_node_){
        {
          MutexLock l(&cache_->mutex_);
          cache_->Ref(node); //one for iter
          cache_->Ref(node); //one for return
          now_node_ = node;
          cache_->Unref(last_node);//for iter          
        }
        //cache_->Erase(last_node->GetKV()->key());
        return node;
      } 
      return nullptr;
    }

   private:
    SkipListLRUCache* cache_;
    SkipListBase::Iterator list_cache_;
    Node* now_node_;
  };

  Iterator* NewCacheIterator(const Slice& left_bound, const uint64_t& file_number){
    return new CacheIterator(&table_, this, left_bound, file_number);
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

void SkipListLRUCache::Ref(Node* e) {
  //std::cout<<"Ref"<<std::endl;
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

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
  {
    MutexLock l(&mutex_);   
    e = table_.Lookup(key);
  }
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
  Node* e = table_.CreateNode(kv, &height);
  e->deleter = deleter;
  e->charge = charge;
  e->refs = 1;  // for the returned handle. Unref by iter's RegisterCleanup
  e->height = height;
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
            MutexLock l(&mutex_);
            bool if_insert = table_.Insert(e, e->height);
            //does not allow duplicate insertion, does not need to return and erase
            //assert(if_insert);
            if(!if_insert) {
              //std::cout<<"Duplicate insertion"<<std::endl;
              e->refs--;
              e->in_cache = false;
              usage_ -= e->charge;
            }

            while (usage_ > capacity_ && lru_.cache_next != &lru_) {
              Node* old = lru_.cache_next;
              assert(old->refs == 1);
              bool erased = FinishErase(table_.Remove(old->GetKV()->key()));
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
  FinishErase(table_.Remove(key));
}

void SkipListLRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.cache_next != &lru_) {
    Node* e = lru_.cache_next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->GetKV()->key()));
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

  Iterator* NewIterator(const Slice& left_bound, const uint64_t& file_number) override {
    return lru_cache_.NewCacheIterator(left_bound, file_number);
  }
};

//}  // end anonymous namespace

Cache* NewSkipListLRUCache(uint64_t capacity, const Comparator* user_cmp, bool is_monitor) { 
  return new LRUCacheWrapper(capacity, user_cmp, is_monitor); 
}

}  // namespace leveldb
