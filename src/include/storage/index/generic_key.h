//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// generic_key.h
//
// Identification: src/include/storage/index/generic_key.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>

#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * Generic key is used for indexing with opaque data.
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instantiated
 * with a template argument.
 */

/** 
 * 通用键用于使用不透明数据进行索引。
 *
 * 该键类型使用固定长度的数组来存储数据，用于索引目的，
 * 实际的大小由模板参数指定和实例化。
 */
template <size_t KeySize>
class GenericKey {
 public:

  // 将一个 Tuple 对象中的数据复制到 GenericKey 中。
  // 首先会将 data_ 数组清空（初始化为0），然后将 tuple 中的数据复制到 data_ 数组中。
  inline void SetFromKey(const Tuple &tuple) {
    // intialize to 0
    memset(data_, 0, KeySize);
    memcpy(data_, tuple.GetData(), tuple.GetLength());
  }

  // 将一个 Tuple 对象中的数据复制到 GenericKey 中
  // NOTE: for test purpose only
  inline void SetFromInteger(int64_t key) {
    memset(data_, 0, KeySize);
    memcpy(data_, &key, sizeof(int64_t));
  }

  // 根据给定的 Schema 和列索引，将 GenericKey 中的数据转换为一个 Value 对象。
  // Value 对象代表数据的不同类型（比如整数、字符串等），并通过 DeserializeFrom 方法将字节数据转换成相应的 Value 类型。
  inline auto ToValue(Schema *schema, uint32_t column_idx) const -> Value {
    const char *data_ptr;
    const auto &col = schema->GetColumn(column_idx);
    const TypeId column_type = col.GetType();
    const bool is_inlined = col.IsInlined();
    if (is_inlined) {
      data_ptr = (data_ + col.GetOffset());
    } else {
      int32_t offset = *reinterpret_cast<int32_t *>(const_cast<char *>(data_ + col.GetOffset()));
      data_ptr = (data_ + offset);
    }
    return Value::DeserializeFrom(data_ptr, column_type);
  }

  // NOTE: for test purpose only
  // interpret the first 8 bytes as int64_t from data vector
  // 将 GenericKey 中的前 8 个字节解释为一个 int64_t 整数并返回
  inline auto ToString() const -> int64_t { return *reinterpret_cast<int64_t *>(const_cast<char *>(data_)); }

  // NOTE: for test purpose only
  // interpret the first 8 bytes as int64_t from data vector
  // 重载了 << 运算符，用于将 GenericKey 转换为字符串输出
  friend auto operator<<(std::ostream &os, const GenericKey &key) -> std::ostream & {
    os << key.ToString();
    return os;
  }

  // 固定长度的字符数组，用于存储键的原始字节数据
  // actual location of data, extends past the end.
  char data_[KeySize];
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
 // 用于比较两个 GenericKey 对象的函数对象。它比较的是键的每一列，通常用于 B+ 树中的排序操作。
template <size_t KeySize>
class GenericComparator {
 public:

  // 比较两个 GenericKey 对象 lhs 和 rhs 中的每一列，直到找到不相等的列。
  inline auto operator()(const GenericKey<KeySize> &lhs, const GenericKey<KeySize> &rhs) const -> int {
    uint32_t column_count = key_schema_->GetColumnCount();

    for (uint32_t i = 0; i < column_count; i++) {
      Value lhs_value = (lhs.ToValue(key_schema_, i));
      Value rhs_value = (rhs.ToValue(key_schema_, i));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue) {
        return -1;
      }
      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue) {
        return 1;
      }
    }
    // equals
    return 0;
  }
  
  // copy constructor
  GenericComparator(const GenericComparator &other) : key_schema_{other.key_schema_} {}

  // constructor
  explicit GenericComparator(Schema *key_schema) : key_schema_(key_schema) {}

 private:
  Schema *key_schema_;
};

}  // namespace bustub
