//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.cpp
//
// Identification: src/storage/table/tuple.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

// TODO(Amadou): It does not look like nulls are supported. Add a null bitmap?
Tuple::Tuple(std::vector<Value> values, const Schema *schema) : allocated_(true) {
  assert(values.size() == schema->GetColumnCount());

  // 1. Calculate the size of the tuple.
  //   - 固定部分大小：由 schema->GetLength() 返回
  //   - 对于非内联（varlen）列，需要额外增加：每个变长值的实际长度 + 一个 uint32_t 用来记录偏移量
  uint32_t tuple_size = schema->GetLength();
  for (auto &i : schema->GetUnlinedColumns()) {
    auto len = values[i].GetLength();
    if (len == BUSTUB_VALUE_NULL) {
      len = 0;
    }
    tuple_size += (len + sizeof(uint32_t));
  }

  // 2. Allocate memory : 为整个元组数据分配一块连续的存储空间，并初始化为 0
  size_ = tuple_size;
  data_ = new char[size_];
  std::memset(data_, 0, size_);

  // 3. Serialize each attribute based on the input value.
  // 固定部分数据在 schema->GetLength() 范围内存放，
  // 变长数据则存放在固定部分之后，offset 用于记录变长数据在 data_ 中的位置。
  uint32_t column_count = schema->GetColumnCount();
  uint32_t offset = schema->GetLength();

  for (uint32_t i = 0; i < column_count; i++) {
    const auto &col = schema->GetColumn(i);
    if (!col.IsInlined()) {
      // Serialize relative offset, where the actual varchar data is stored.
      // 对于非内联的列，先在固定部分存放该列的偏移量
      *reinterpret_cast<uint32_t *>(data_ + col.GetOffset()) = offset;
      // Serialize varchar value, in place (size+data).
      // 将变长数据序列化到 offset 处（包括数据长度和实际数据）
      values[i].SerializeTo(data_ + offset);
      auto len = values[i].GetLength();
      if (len == BUSTUB_VALUE_NULL) {
        len = 0;
      }
      offset += (len + sizeof(uint32_t));
    } else {
      // 对于内联的列，直接在固定位置序列化数据
      values[i].SerializeTo(data_ + col.GetOffset());
    }
  }
}

// 拷贝构造函数
Tuple::Tuple(const Tuple &other) : allocated_(other.allocated_), rid_(other.rid_), size_(other.size_) {
  if (allocated_) {
    delete[] data_;
  }
  if (allocated_) {
    // Deep copy.
    // 深拷贝：为 data_ 分配新的内存，并复制内容
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  } else {
    // Shallow copy.
    // 浅拷贝：直接拷贝指针，不重新分配内存
    data_ = other.data_;
  }
}

// 赋值构造函数
auto Tuple::operator=(const Tuple &other) -> Tuple & {
  if (allocated_) {
    delete[] data_;
  }
  allocated_ = other.allocated_;
  rid_ = other.rid_;
  size_ = other.size_;

  if (allocated_) {
    // Deep copy.
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  } else {
    // Shallow copy.
    data_ = other.data_;
  }

  return *this;
}

auto Tuple::GetValue(const Schema *schema, const uint32_t column_idx) const -> Value {
  assert(schema);
  assert(data_);
  const TypeId column_type = schema->GetColumn(column_idx).GetType();
  const char *data_ptr = GetDataPtr(schema, column_idx);
  // the third parameter "is_inlined" is unused
  return Value::DeserializeFrom(data_ptr, column_type);
}

// 从当前元组中提取构成索引键的部分数据。
auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs/*列索引向量，指定了哪些列构成索引键*/)
    -> Tuple {
  std::vector<Value> values;
  values.reserve(key_attrs.size());
  for (auto idx : key_attrs) {
    values.emplace_back(this->GetValue(&schema, idx));
  }
  return {values, &key_schema};
}

// 用于获取指定列在 data_ 中的存储地址
auto Tuple::GetDataPtr(const Schema *schema, const uint32_t column_idx) const -> const char * {
  assert(schema);
  assert(data_);
  const auto &col = schema->GetColumn(column_idx);
  bool is_inlined = col.IsInlined();
  // For inline type, data is stored where it is.
  if (is_inlined) {
    return (data_ + col.GetOffset());
  }
  // We read the relative offset from the tuple data.
  int32_t offset = *reinterpret_cast<int32_t *>(data_ + col.GetOffset());
  // And return the beginning address of the real data for the VARCHAR type.
  return (data_ + offset);
}

// 生成一个字符串来表示元组的内容
auto Tuple::ToString(const Schema *schema) const -> std::string {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";
  os << " Tuple size is " << size_;

  return os.str();
}

// 序列化元组数据到指定的存储位置
void Tuple::SerializeTo(char *storage) const {
  memcpy(storage, &size_, sizeof(int32_t));
  memcpy(storage + sizeof(int32_t), data_, size_);
}

// 从指定的存储位置反序列化元组数据
void Tuple::DeserializeFrom(const char *storage) {
  uint32_t size = *reinterpret_cast<const uint32_t *>(storage);
  // Construct a tuple.
  this->size_ = size;
  if (this->allocated_) {
    delete[] this->data_;
  }
  this->data_ = new char[this->size_];
  memcpy(this->data_, storage + sizeof(int32_t), this->size_);
  this->allocated_ = true;
}

}  // namespace bustub
