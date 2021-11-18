/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>
#include <map>
#include <math.h>
#include <memory>
#include "hash/hash_table.h"

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  struct Bucket
  {
    Bucket(int depth,int id)
    {
      mLocalDepth = depth;
      mId = id;
    }
    int mLocalDepth = 0;
    int mId = 0;
    std::map<K,V> dataMap;
  };
private:
  // add your own member variables here
  int mDepth;
  int mBucketCount;
  size_t GetBucketIndexFromHash(size_t hash);
  void Split(size_t index, const K &key,const V &value);
  size_t GetDirCapacity()const 
  {
    return pow(2,mDepth);        // 2^mDepth
  }
  size_t mBucketDataSize;
  std::vector<std::shared_ptr<Bucket>> mDirectory;
  mutable std::mutex mutex_;
};
} // namespace scudb
