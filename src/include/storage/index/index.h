//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index.h
//
// Identification: src/include/storage/index/index.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

class Transaction;

/**
 * 类 IndexMetadata - 存储索引对象的元数据
 *
 * 元数据对象维护了索引的元组模式和键属性，因为外部调用者不知道索引键的实际结构，
 * 所以由索引来负责维护这样的映射关系，并进行元组键和索引键之间的转换。
 */
/**
 * class IndexMetadata - Holds metadata of an index object.
 *
 * The metadata object maintains the tuple schema and key attribute of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 */
 // 存储与索引相关的元数据
class IndexMetadata {
 public:
  IndexMetadata() = delete;

  /**
   * Construct a new IndexMetadata instance.
   * @param index_name The name of the index
   * @param table_name The name of the table on which the index is created
   * @param tuple_schema The schema of the indexed key
   * @param key_attrs The mapping from indexed columns to base table columns
   */
  IndexMetadata(std::string index_name, std::string table_name, const Schema *tuple_schema,
                std::vector<uint32_t> key_attrs)
      : name_(std::move(index_name)), table_name_(std::move(table_name)), key_attrs_(std::move(key_attrs)) {
    key_schema_ = std::make_shared<Schema>(Schema::CopySchema(tuple_schema, key_attrs_));
  }

  ~IndexMetadata() = default;

  /** @return The name of the index */
  inline auto GetName() const -> const std::string & { return name_; }

  /** @return The name of the table on which the index is created */
  inline auto GetTableName() -> const std::string & { return table_name_; }

  /** @return A schema object pointer that represents the indexed key */
  inline auto GetKeySchema() const -> Schema * { return key_schema_.get(); }

  /**
   * @return The number of columns inside index key (not in tuple key)
   *
   * NOTE: this must be defined inside the cpp source file because it
   * uses the member of catalog::Schema which is not known here.
   */
  auto GetIndexColumnCount() const -> std::uint32_t { return static_cast<uint32_t>(key_attrs_.size()); }

  /** @return The mapping relation between indexed columns and base table columns */
  inline auto GetKeyAttrs() const -> const std::vector<uint32_t> & { return key_attrs_; }

  /** @return A string representation for debugging */
  auto ToString() const -> std::string {
    std::stringstream os;

    os << "IndexMetadata["
       << "Name = " << name_ << ", "
       << "Type = B+Tree, "
       << "Table name = " << table_name_ << "] :: ";
    os << key_schema_->ToString();

    return os.str();
  }

 private:
  /** The name of the index */
  std::string name_;                            // 索引的名称
  /** The name of the table on which the index is created */
  std::string table_name_;                      // 索引所在表的名称
  /** The mapping relation between key schema and tuple schema */
  const std::vector<uint32_t> key_attrs_;       // 索引键的列与表列之间的映射关系
  /** The schema of the indexed key */
  std::shared_ptr<Schema> key_schema_;          // 索引键的模式
};

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 * class Index - Base class for derived indices of different types
 *
 * The index structure majorly maintains information on the schema of the
 * underlying table and the mapping relation between index key
 * and tuple key, and provides an abstracted way for the external world to
 * interact with the underlying index implementation without exposing
 * the actual implementation's interface.
 *
 * Index object also handles predicate scan, in addition to simple insert,
 * delete, predicate insert, point query, and full index scan. Predicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */

 /**
 * 类 Index - 用于派生出不同类型索引的基类
 *
 * 索引结构主要用于维护底层表的模式信息，以及索引键与元组键之间的映射关系，并提供一种抽象方式，
 * 让外部可以与底层索引实现进行交互，而不暴露实际实现的接口。
 *
 * 索引对象还处理谓词扫描，除了简单的插入、删除、谓词插入、点查询和全索引扫描外。谓词扫描仅支持合取操作，
 * 并且可能会根据谓词中表达式的类型进行优化，也可能不会。
 */
 // 索引的基类
class Index {
 public:
  /**
   * Construct a new Index instance.
   * @param metdata An owning pointer to the index metadata
   */
  explicit Index(std::unique_ptr<IndexMetadata> &&metadata) : metadata_{std::move(metadata)} {}

  virtual ~Index() = default;

  /** @return A non-owning pointer to the metadata object associated with the index */
  auto GetMetadata() const -> IndexMetadata * { return metadata_.get(); }

  /** @return The number of indexed columns */
  auto GetIndexColumnCount() const -> std::uint32_t { return metadata_->GetIndexColumnCount(); }

  /** @return The index name */
  auto GetName() const -> const std::string & { return metadata_->GetName(); }

  /** @return The index key schema */
  auto GetKeySchema() const -> Schema * { return metadata_->GetKeySchema(); }

  /** @return The index key attributes */
  auto GetKeyAttrs() const -> const std::vector<uint32_t> & { return metadata_->GetKeyAttrs(); }

  /** @return A string representation for debugging */
  auto ToString() const -> std::string {
    std::stringstream os;
    os << "INDEX: (" << GetName() << ")";
    os << metadata_->ToString();
    return os.str();
  }

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////

  /**
   * Insert an entry into the index.
   * @param key The index key
   * @param rid The RID associated with the key
   * @param transaction The transaction context
   */
  virtual void InsertEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

  /**
   * Delete an index entry by key.
   * @param key The index key
   * @param rid The RID associated with the key (unused)
   * @param transaction The transaction context
   */
  virtual void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) = 0;

  /**
   * Search the index for the provided key.
   * @param key The index key
   * @param result The collection of RIDs that is populated with results of the search
   * @param transaction The transaction context
   */
  virtual void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) = 0;

 private:
  /** The Index structure owns its metadata */
  std::unique_ptr<IndexMetadata> metadata_; // 索引的元数据
};

}  // namespace bustub
