#include "db/L0_reminder.h"
#include "util/hash.h"
#include "leveldb/slice.h"
#include "util/mutexlock.h"

namespace leveldb {

TableHandle* L0_Reminder_Wrapper::ReadFromReminder(const Slice& user_key){
  MutexLock l(&mutex_);
  TableHandle* handle = hash_table.Lookup(user_key, HashSlice(user_key));
  if(handle != nullptr){
    Ref(handle);
    return handle;
  }
  return nullptr;
}

void L0_Reminder_Wrapper::WriteToReminder(const Slice& user_key, const Slice& value, uint32_t hash){ 
  MutexLock l(&mutex_);
  TableHandle* handle =
      reinterpret_cast<TableHandle*>(malloc(sizeof(TableHandle) - 1 + user_key.size() + value.size()));  
  handle->key_length = user_key.size();
  handle->value_length = value.size();
  handle->charge = handle->key_length + handle->value_length;
  handle->hash = hash;
  handle->refs = 1; //Also counted as one reference in Reminder
  memcpy(handle->key_data, user_key.data(), user_key.size());
  memcpy(handle->key_data + user_key.size(), value.data(), value.size());
  hash_table.Insert(handle);
}

void L0_Reminder_Wrapper::Release(TableHandle* handle){
  MutexLock l(&mutex_);
  assert(handle != nullptr);
  Unref(handle);
}

void L0_Reminder_Wrapper::Ref(TableHandle* handle){
  handle->refs++;
}

void L0_Reminder_Wrapper::Unref(TableHandle* handle){
  assert(handle->refs > 0);
  handle->refs--;
  if(handle->refs == 0){
    free(handle);
  }
}

void L0_Reminder_Wrapper::Erase(const Slice& key, const uint64_t& file_number){
  MutexLock l(&mutex_);
  TableHandle* result = hash_table.Remove(key, file_number, HashSlice(key));
  if(result != nullptr){
    Unref(result);
  } 
}

} // namespace leveldb