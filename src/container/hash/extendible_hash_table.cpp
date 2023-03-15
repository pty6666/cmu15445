//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
  printf("1\n");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //  UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool flag = dir_[index]->Find(key, value);
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //  UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool flag = dir_[index]->Remove(key);
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth();
  int dep = bucket->GetDepth();
  std::shared_ptr<Bucket> buc(std::make_shared<Bucket>(bucket_size_, dep));
  size_t pre_index = std::hash<K>()((*bucket->GetItems().begin()).first) & ((1 << (dep - 1)) - 1);
  for (auto i = bucket->GetItems().begin(); i != bucket->GetItems().end(); i++) {
    size_t index = std::hash<K>()(i->first) & ((1 << dep) - 1);
    if (index != pre_index) {
      buc->Insert(i->first, i->second);
      bucket->Remove(i->first);
    }
  }
  dir_[pre_index | (1 << (dep - 1))] = buc;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //  UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    size_t index = IndexOf(key);
    bool flag = dir_[index]->Insert(key, value);
    if (flag) {
      break;
    }
    if (GetLocalDepthInternal(index) < GetGlobalDepthInternal()) {
      RedistributeBucket(dir_[index]);
    } else {
      global_depth_++;
      size_t siz = dir_.size();
      for (size_t i = 0; i < siz; i++) {
        dir_.emplace_back(dir_[i]);
      }
      RedistributeBucket(dir_[index]);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  //  UNREACHABLE("not implemented");
  for (const auto &i : list_) {
    if (i.first == key) {
      value = i.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  //  UNREACHABLE("not implemented");
  for (auto i = list_.begin(); i != list_.end(); i++) {
    if (i->first == key) {
      list_.erase(i);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //  UNREACHABLE("not implemented");
  for (auto &i : list_) {
    if (i.first == key) {
      i.second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
