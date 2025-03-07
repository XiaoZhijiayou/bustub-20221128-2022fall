//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * 在叶子页面中存储索引键和记录ID（record id = 页面ID与槽ID的组合，
 * 具体实现见 include/common/rid.h）在一起。只支持唯一键。
 *
 * 叶子页面格式（键按顺序存储）：
 *  ----------------------------------------------------------------------
 * | 头部 | 键(1) + RID(1) | 键(2) + RID(2) | ... | 键(n) + RID(n) |
 *  ----------------------------------------------------------------------
 *
 * 头部格式（字节大小，总共28字节）：
 *  ---------------------------------------------------------------------
 * | 页面类型 (4) | LSN (4) | 当前大小 (4) | 最大大小 (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | 父页面ID (4) | 页面ID (4) | 下一页面ID (4) |
 *  -----------------------------------------------
 */

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;

  auto Contain(const KeyType &key, const KeyComparator &comparator) -> bool;

  /**
   * @brife Insert the new kv pair in the proper position of the array and size will increase if size<=max_size .
   * Otherwise, the function just returns.
   *
   * @param key The key
   * @param value The value
   *
   * @note Check the size with the max size before invocation.
   */
  void Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);

  void Remove(const KeyType &key, const KeyComparator &comparator);


  /**
   * @brief ExtractHalf return half of the elements which is useful in splitting.
   *
   * @return Half of the elements.
   */
  auto ExtractHalf() -> std::vector<MappingType>;

  /**
   * @brife Used for coalescing and let size be 0.
   *
   * @return All pairs.
   */
  auto ExtractAll() -> std::vector<MappingType>;
  
  void EmplaceBack(const std::vector<MappingType> &paris);
  /**
   * @brife Pop the last pair and decrease the size.
   *
   * @return The back pair.
   */
  auto PopBack() -> MappingType;

  auto PopFront() -> MappingType;

  inline auto Get() -> MappingType * { return array_; }
  
 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[LEAF_PAGE_SIZE];
};

}  // namespace bustub
