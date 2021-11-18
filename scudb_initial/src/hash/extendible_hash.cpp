#include <list>
#include "hash/extendible_hash.h"
#include "page/page.h"
#include <bitset>
#include <assert.h>
#include "common/logger.h"
namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size):mDepth(1),mBucketDataSize(size)
{
  mDirectory.emplace_back(new Bucket(mDepth,0));
  mDirectory.emplace_back(new Bucket(mDepth,1));
  mBucketCount = 2;
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  std::hash<K> key_hash;
  return key_hash(key);
}

template <typename K, typename V>
size_t ExtendibleHash<K, V>::GetBucketIndexFromHash(size_t hash)
{
  int n = hash & ((1 << mDepth) - 1 );          //2^mDepth-1
  return n;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return mDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if(size_t(bucket_id) >= GetDirCapacity())
  {
    return -1;
  }
  if(mDirectory[bucket_id] != nullptr)
  {
    size_t NumOfItems = mDirectory[bucket_id]->dataMap.size();
    if(NumOfItems == 0)
    {//there is no items in the datamap
      return -1;
    }
    return mDirectory[bucket_id]->mLocalDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lock(mutex_);
  int count = 0;
  int i = 0;
  for(auto a:mDirectory)
  {
    size_t NumOfItems = a->dataMap.size();
    if(NumOfItems > 0 && a->mId == i)
    {
      count++;
    }
    i++;
  }
  return count;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = GetBucketIndexFromHash(HashKey(key));
  if(index >= GetDirCapacity())
  {
    return false;
  }
  auto search = mDirectory[index]->dataMap.find(key);
  if(search != mDirectory[index]->dataMap.end())       //successfully find
  {
    value = search->second;               //value
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = GetBucketIndexFromHash(HashKey(key));
  if(index >= GetDirCapacity())
  {
    return false;
  }
  auto search = mDirectory[index]->dataMap.find(key);
  if(search != mDirectory[index]->dataMap.end())       //successfully find
  {
    mDirectory[index]->dataMap.erase(key);
    return true;
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Split(size_t index, const K &key,const V &value)
{
  auto bucket = mDirectory[index];
  if(bucket->mLocalDepth == mDepth)
  {//only one item in the mDiretory point to the bucket
  //we need to resize the mDirectory
    size_t preSize = GetDirCapacity();
    mDirectory.resize(2*preSize);
    mDepth++;
    for(size_t i = preSize; i < mDirectory.capacity(); i++)
    {
      mDirectory[i] = mDirectory[i - preSize];
    }
  }
  bucket->mLocalDepth++;
  // index = splitBucketId
  int newBucketId = bucket->mId + (GetDirCapacity() / 2);
  int splitBucketId = bucket->mId;
  mDirectory[newBucketId] = std::make_shared<Bucket>(bucket->mLocalDepth,newBucketId);
  mBucketCount++;
  for(auto kv = mDirectory[splitBucketId]->dataMap.begin();kv!=mDirectory[splitBucketId]->dataMap.end();)
  {
    //std::cout<<kv->first<<std::endl;
    size_t newHash = GetBucketIndexFromHash(HashKey(kv->first));
    //std::cout<<"newHash"<<newHash<<std::endl;
    if(int(newHash) != mDirectory[splitBucketId]->mId)
    {
      //std::cout<<mDirectory[newHash]->mId<<std::endl;
      if(mDirectory[newHash]->mId != int(newHash))
      {
        mDirectory[newHash]->mLocalDepth++;
        size_t temDepth = mDirectory[newHash]->mLocalDepth;
        mDirectory[newHash] = std::make_shared<Bucket>(temDepth,newHash);
      }
      mDirectory[newHash]->dataMap[kv->first] = kv->second;
      mDirectory[splitBucketId]->dataMap.erase(kv++);
    }else
    {
      kv++;
    }
  }
  mDirectory[newBucketId]->mLocalDepth = bucket->mLocalDepth;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = GetBucketIndexFromHash(HashKey(key));
  //printf("index:%zu\n",index);
  assert(GetDirCapacity() - 1 >= index);
  auto bucket = mDirectory[index];
  while (bucket->dataMap.size() >= mBucketDataSize)
  {
    Split(index,key,value);
    index = GetBucketIndexFromHash(HashKey(key));
    bucket = mDirectory[index];
  }
  bucket->dataMap[key] = value;
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
