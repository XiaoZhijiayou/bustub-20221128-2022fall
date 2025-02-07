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
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = std::make_shared<Bucket>(bucket_size);
  dir_.emplace_back(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  // 桶的个数就是2^global_depth_
  int mask = (1 << global_depth_) - 1;
  // 通过111掩码来获取key的索引
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
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}


template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {}

/**
  第 1 步 – 分析数据元素： 数据元素可能以多种形式存在，例如。Integer、String、Float 等。目前，我们来研究 integer 类型的数据元素。例如：49。
  第 2 步 – 转换为二进制格式： 将数据元素转换为二进制形式。对于字符串元素，请考虑起始字符的 ASCII 等效整数，然后将该整数转换为二进制形式。由于我们有 49 作为数据元素，因此其二进制形式为 110001。
  第 3 步 – 检查目录的 Global Depth。假设 Hash 目录的全局深度为 3。
  第 4 步 – 确定目录： 考虑二进制数中 LSB 的 'Global-Depth' 数量，并将其与目录 ID 匹配。
  例如。获得的二进制是： 110001 且全局深度为 3。因此，哈希函数将返回 3 个 110001 的 LSB，即 001。
  第 5 步 – 导航： 现在，导航到 directory-id 为 001 的目录指向的存储桶。
  第 6 步 – 插入和溢出检查： 插入元素并检查桶是否溢出。如果遇到溢出，请转到步骤 7，然后转到步骤 8，否则，请转到步骤 9。
  第 7 步 – 处理数据插入期间的超流情况：
  如果插入桶失败
     检查局部深度是否小于或等于全局深度：
      1.如果溢出 Bucket 的局部深度等于全局深度，则需要进行 Directory Expansion 和 Bucket Split。然后将全局深度和局部深度值递增 1。并且，分配适当的指针。
      目录扩展将使哈希结构中存在的目录数量增加一倍。
      2.如果局部深度小于全局深度，则仅发生 Bucket Split。然后仅将局部深度值递增 1。并且，分配适当的指针。
      Directory Expansion：就是将新加入的那一半按照顺序依次连接上之前的目录例如：dir_[i + capacity] = dir_[i];
  最后递归：被拆分的溢出存储桶中存在的 Elements 将根据目录的新全局深度进行重新哈希处理。
  */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value){
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  bool res = bucket->Insert(key, value);
  //  插入成功
  if (res) {
    return;
  }
  // Bucket is full, depth of bucket equals global depth, entend the dir and split the bucket
  if (GetGlobalDepthInternal() == bucket->GetDepth()) {
    global_depth_++;
    size_t capacity = dir_.size();
    dir_.resize(capacity  << 1);
    for (size_t i = 0; i < capacity; i++) {
      dir_[i + capacity] = dir_[i];
    }
  }

  // Bucket is full, but the depth of bucket is less than global depth, split the bucket
  int new_depth = bucket->GetDepth() + 1;
  size_t base_mask = (1 << bucket->GetDepth()) - 1;
  size_t split_mask = (1 << new_depth) - 1;
  auto first_bucket = std::make_shared<Bucket>(bucket_size_, new_depth);
  auto second_bucket = std::make_shared<Bucket>(bucket_size_, new_depth);

  size_t low_index = index & base_mask;
  // 这一部分其实就是将原来的桶中的元素重新分配到新的桶和原来的桶中去，如果元素的索引和原来的桶的索引相同，就放到原来的桶中，否则放到新的桶中
  for (const auto &ele : bucket->GetItems()) {
    size_t split_index = IndexOf(ele.first);
    if ((split_index & split_mask) == low_index) {
      // 这里就是将ele放入到老桶中去
      first_bucket->GetItems().emplace_back(ele);
      continue;
    }
    // 这里是将ele放入到新桶中去
    second_bucket->GetItems().emplace_back(ele);
  }

  for (size_t i = 0; i < dir_.size(); i++) {
    if ((i & base_mask) == low_index) {
      if ((i & split_mask) == low_index) {
        dir_[i] = first_bucket;
        continue;
      }
      dir_[i] = second_bucket;
    }
  }
  num_buckets_++;
  //在桶分裂并且更新目录之后，递归的再吃尝试插入键值对(这时桶的深度已经增加，新的目录已经扩展),这种递归保证了插入操作最终能够成功
  return InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto ele = list_.begin(); ele != list_.end();) {
    if (ele->first == key) {
      list_.erase(ele);
      return true;
    }
    ele++;
  }
  return false;
}


template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // if the key already exists, overwrite its value with new value
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  // Here the local depth of the bucket is less than global depth.
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
