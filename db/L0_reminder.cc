#include "db/L0_reminder.h"
#include "util/hash.h"
#include "leveldb/slice.h"

namespace leveldb {

TableHandle* L0_Reminder::ReadFromReminder(const Slice& user_key){
  TableHandle* handle = hash_table.Lookup(user_key, HashSlice(user_key));
  if(handle != nullptr){
    Ref(handle);
    return handle;
  }
  return nullptr;
}

void L0_Reminder::WriteToReminder(const Slice& ikey, const Slice& value){ 
  //MutexLock l(&mutex_); don't need to lock, only one thread will write to reminder
  assert(ikey.size() > 8);
  Slice user_key = Slice(ikey.data(), ikey.size() - 8);
  TableHandle* handle =
      reinterpret_cast<TableHandle*>(malloc(sizeof(TableHandle) - 1 + user_key.size() + value.size()));  
  handle->key_length = user_key.size();
  handle->value_length = value.size();
  handle->charge = handle->key_length + handle->value_length;
  handle->hash = HashSlice(user_key);
  handle->refs = 1; //Also counted as one reference in Reminder
  memcpy(handle->key_data, user_key.data(), user_key.size());
  memcpy(handle->key_data + user_key.size(), value.data(), value.size());
  hash_table.Insert(handle);
}

void L0_Reminder::Release(TableHandle* handle){
  assert(handle != nullptr);
  Unref(handle);
}

void L0_Reminder::Ref(TableHandle* handle){
  handle->refs++;
}

void L0_Reminder::Unref(TableHandle* handle){
  assert(handle->refs > 0);
  handle->refs--;
  if(handle->refs == 0){
    free(handle);
  }
}

} // namespace leveldb