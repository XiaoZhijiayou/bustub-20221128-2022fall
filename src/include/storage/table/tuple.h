//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.h
//
// Identification: src/include/storage/table/tuple.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/rid.h"
#include "type/value.h"

namespace bustub {

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */

 /** Tuple 类代表数据库中的一行数据（记录）。
  记录的数据按照固定格式存储在内存中，其中包含固定长度数据部分以及变长字段的偏移信息。
  类中提供了序列化和反序列化接口，用于在内存和磁盘之间转换记录数据。 */
class Tuple {
  friend class TablePage;
  friend class TableHeap;
  friend class TableIterator;

 public:
  // Default constructor (to create a dummy tuple)
  Tuple() = default;

  // constructor for table heap tuple
  explicit Tuple(RID rid) : rid_(rid) {}

  // constructor for creating a new tuple based on input value
  Tuple(std::vector<Value> values, const Schema *schema);

  // copy constructor, deep copy
  Tuple(const Tuple &other);

  // assign operator, deep copy
  auto operator=(const Tuple &other) -> Tuple &;

  ~Tuple() {
    if (allocated_) {
      delete[] data_;
    }
    allocated_ = false;
    data_ = nullptr;
  }
  // serialize tuple data
  void SerializeTo(char *storage) const;

  // deserialize tuple data(deep copy)
  void DeserializeFrom(const char *storage);

  // return RID of current tuple
  inline auto GetRid() const -> RID { return rid_; }

  // Get the address of this tuple in the table's backing store
  inline auto GetData() const -> char * { return data_; }

  // Get length of the tuple, including varchar legth
  inline auto GetLength() const -> uint32_t { return size_; }

  // Get the value of a specified column (const)
  // checks the schema to see how to return the Value.
  auto GetValue(const Schema *schema, uint32_t column_idx) const -> Value;

  // Generates a key tuple given schemas and attributes
  auto KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs) -> Tuple;

  // Is the column value null ?
  inline auto IsNull(const Schema *schema, uint32_t column_idx) const -> bool {
    Value value = GetValue(schema, column_idx);
    return value.IsNull();
  }
  inline auto IsAllocated() -> bool { return allocated_; }

  auto ToString(const Schema *schema) const -> std::string;

 private:
  // Get the starting storage address of specific column
  auto GetDataPtr(const Schema *schema, uint32_t column_idx) const -> const char *;

  bool allocated_{false};  // is allocated? 标记 data_ 是否由当前对象分配了内存
  RID rid_{};              // if pointing to the table heap, the rid is valid 存储该记录的唯一标识符，由页面 ID 和槽编号组成，用于定位记录在表堆中的位置。
  uint32_t size_{0};       // 表示记录数据的总字节数（长度）。
  char *data_{nullptr};    // 指向记录数据的缓冲区，这个缓冲区中存储了记录的实际数据，可能包括固定长度部分和变长部分。
};

}  // namespace bustub
