/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto search = itemMap.find(value);
  if(search!=itemMap.end())
  {//already exist  remove it
    itemList.erase(search->second);
    itemMap.erase(search->first);
  }
  itemList.push_front(value);
  itemMap.insert(std::make_pair(value,itemList.begin()));
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if(!itemList.empty())
  {
    value = itemList.back();      //remove the object that was least recently used
    itemMap.erase(value);
    itemList.pop_back();
    return true;
  }
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto search = itemMap.find(value);
  if(search!=itemMap.end())
  {//find
    itemList.erase(search->second);
    itemMap.erase(search->first);
    return true;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  std::lock_guard<std::mutex> lock(mutex_);
  return itemMap.size(); 
 }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
