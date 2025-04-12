//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * 在内部页面中存储 n 个索引键和 n+1 个子指针（page_id）。
 * 指针 PAGE_ID(i) 指向一个子树，其中所有键 K 满足：
 * K(i) <= K < K(i+1)。
 * 注意：由于键的数量与子指针数量不相等，第一个键始终是无效的。
 * 但是注意它的第一个value是有效的，k是无效的，它的第一个value指向第一个内部页面的id
 * 也就是说，任何搜索/查找操作都应忽略第一个键。
 *
 * 内部页面格式（键按递增顺序存储）：
 *  --------------------------------------------------------------------------
 * | 头部 | 键(1)+页面ID(1) | 键(2)+页面ID(2) | ... | 键(n)+页面ID(n) |
 *  --------------------------------------------------------------------------
 */

/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);
  // 基本的访问
  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;

  /**
    * @brief: 获取值的相邻（默认为下一个）节点
    * @param value 节点的父级值。
    * @return ValueType 如果node是最后一个，则它的前节点
    *将被退回。否则，下一个。
    */
  auto Adjacent(const ValueType &value) const -> ValueType;

  /**
    * @brief 获取 v 和 v_other 之间的关系。
    *
    * @param v 用于进行比较的值。
    * @param v_other 另一个值
    * @return 如果 v_other 是 v 的前序元素，则返回 true；否则返回 false。
    */
  auto IsPredecessor(const ValueType &v, const ValueType &v_other) -> bool;

  /**
    * @brief 获取键值介于 va 和 vb 之间的键的索引。
    *
    * @param va 第一个值
    * @param vb 第二个值
    * @return 键的索引。
    */
  auto BetweenKeyIndex(const ValueType &va, const ValueType &vb) const -> int;

  /**
    * @brief 在指定值之后插入键值对 (new_key, new_value)。
    *
    * @param value 指定的值，新键值对将插入到该值之后
    * @param new_key 要插入的新键
    * @param new_value 要插入的新值
    */
  void InsertAfter(const ValueType &value, const KeyType &new_key, const ValueType &new_value);

  // 在尾部插入键值对
  void PushBack(const KeyType &key, const ValueType &value);

  // 在头部插入键值对
  void PushFront(const ValueType &value);

  // 在指定位置插入键值对
  void Put(const ValueType &left, const KeyType &key, const ValueType &right);

  // 提取一半的键值对
  auto ExtractHalf() -> std::vector<MappingType>;

  /**
    * @brief 用于合并操作，并将大小置为 0
    * 
    * @return all pairs
    */
  auto ExtractAll() -> std::vector<MappingType>;

  // 插入多个键值对
  void EmplaceBack(const std::vector<MappingType> &pairs);

  // 删除指定键值对
  void Remove(const KeyType &key, const KeyComparator &comparator);

  /**
    * @brief 删除最后的pair并且减少大小
    * @return 最后一个pair
    */
  auto PopBack() -> MappingType;

  // 删除第一个pair
  auto PopFront() -> MappingType;

  // 获取数组
  inline auto Get() -> MappingType * { return array_; }

 private:
  // 将键转换为字符串
  auto KeyToString(const KeyType &key) const -> std::string;

  // Flexible array member for page data.
  MappingType array_[LEAF_PAGE_SIZE];
};

}  // namespace bustub
