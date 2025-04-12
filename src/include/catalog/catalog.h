//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * The TableInfo class maintains metadata about a table.
 */
struct TableInfo {
  /**
   * Construct a new TableInfo instance.
   * @param schema The table schema
   * @param name The table name
   * @param table An owning pointer to the table heap
   * @param oid The unique OID for the table
   */
  TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_{std::move(schema)}, name_{std::move(name)}, table_{std::move(table)}, oid_{oid} {}

  /** The table schema */
  /** 表的模式，描述表中各列的结构（类型、名称等） */
  Schema schema_;

  /** The table name */
  /** 表的名称 */
  const std::string name_;

  /** An owning pointer to the table heap */
  /** 一个拥有指针，指向表堆对象 */
  std::unique_ptr<TableHeap> table_;

  /** The table OID */
  /** 表的唯一标识符 */
  const table_oid_t oid_;
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
struct IndexInfo {
  /**
   * Construct a new IndexInfo instance.
   * @param key_schema The schema for the index key
   * @param name The name of the index
   * @param index An owning pointer to the index
   * @param index_oid The unique OID for the index
   * @param table_name The name of the table on which the index is created
   * @param key_size The size of the index key, in bytes
   */
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_{std::move(key_schema)},
        name_{std::move(name)},
        index_{std::move(index)},
        index_oid_{index_oid},
        table_name_{std::move(table_name)},
        key_size_{key_size} {}
  /** The schema for the index key */
  /** 索引键的模式，描述索引键的结构 */
  Schema key_schema_;

  /** The name of the index */
  /** 索引的名称 */
  std::string name_;

  /** An owning pointer to the index */
  /** 指向索引对象的智能指针 */
  std::unique_ptr<Index> index_;

  /** The unique OID for the index */
  /** 索引的唯一标识符 */
  index_oid_t index_oid_;

  /** The name of the table on which the index is created */
  /** 索引所在表的名称 */
  std::string table_name_;

  /** The size of the index key, in bytes */
  /** 索引键的大小（以字节为单位） */
  const size_t key_size_;
};

/**
 * The Catalog is a non-persistent catalog that is designed for
 * use by executors within the DBMS execution engine. It handles
 * table creation, table lookup, index creation, and index lookup.
 */
 /** Catalog 类是一个非持久化的系统目录，主要由 DBMS 执行器使用。它负责维护和查找表和索引的元数据，并提供创建和查询表/索引的接口。 */
class Catalog {
 public:
  /** Indicates that an operation returning a `TableInfo*` failed */
  static constexpr TableInfo *NULL_TABLE_INFO{nullptr};

  /** Indicates that an operation returning a `IndexInfo*` failed */
  static constexpr IndexInfo *NULL_INDEX_INFO{nullptr};

  /**
   * Construct a new Catalog instance.
   * @param bpm The buffer pool manager backing tables created by this catalog
   * @param lock_manager The lock manager in use by the system
   * @param log_manager The log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn The transaction in which the table is being created
   * @param table_name The name of the new table, note that all tables beginning with `__` are reserved for the system.
   * @param schema The schema of the new table
   * @param create_table_heap whether to create a table heap for the new table
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema, bool create_table_heap = true)
      -> TableInfo * {
    if (table_names_.count(table_name) != 0) {
      return NULL_TABLE_INFO;
    }

    // Construct the table heap
    std::unique_ptr<TableHeap> table = nullptr;

    // TODO(Wan,chi): This should be refactored into a private ctor for the binder tests, we shouldn't allow nullptr.
    // When create_table_heap == false, it means that we're running binder tests (where no txn will be provided) or
    // we are running shell without buffer pool. We don't need to create TableHeap in this case.
    if (create_table_heap) {
      table = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    }

    // Fetch the table OID for the new table
    const auto table_oid = next_table_oid_.fetch_add(1);

    // Construct the table information
    auto meta = std::make_unique<TableInfo>(schema, table_name, std::move(table), table_oid);
    auto *tmp = meta.get();

    // Update the internal tracking mechanisms
    tables_.emplace(table_oid, std::move(meta));
    table_names_.emplace(table_name, table_oid);
    index_names_.emplace(table_name, std::unordered_map<std::string, index_oid_t>{});

    return tmp;
  }

  /**
   * Query table metadata by name.
   * @param table_name The name of the table
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto GetTable(const std::string &table_name) const -> TableInfo * {
    auto table_oid = table_names_.find(table_name);
    if (table_oid == table_names_.end()) {
      // Table not found
      return NULL_TABLE_INFO;
    }

    auto meta = tables_.find(table_oid->second);
    BUSTUB_ASSERT(meta != tables_.end(), "Broken Invariant");

    return (meta->second).get();
  }

  /**
   * Query table metadata by OID
   * @param table_oid The OID of the table to query
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto GetTable(table_oid_t table_oid) const -> TableInfo * {
    auto meta = tables_.find(table_oid);
    if (meta == tables_.end()) {
      return NULL_TABLE_INFO;
    }

    return (meta->second).get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.
   * @param txn The transaction in which the table is being created
   * @param index_name The name of the new index
   * @param table_name The name of the table
   * @param schema The schema of the table
   * @param key_schema The schema of the key
   * @param key_attrs Key attributes
   * @param keysize Size of the key
   * @param hash_function The hash function for the index
   * @return A (non-owning) pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  auto CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name, const Schema &schema,
                   const Schema &key_schema, const std::vector<uint32_t> &key_attrs, std::size_t keysize,
                   HashFunction<KeyType> hash_function) -> IndexInfo * {
    // Reject the creation request for nonexistent table
    if (table_names_.find(table_name) == table_names_.end()) {
      return NULL_INDEX_INFO;
    }

    // If the table exists, an entry for the table should already be present in index_names_
    BUSTUB_ASSERT((index_names_.find(table_name) != index_names_.end()), "Broken Invariant");

    // Determine if the requested index already exists for this table
    auto &table_indexes = index_names_.find(table_name)->second;
    if (table_indexes.find(index_name) != table_indexes.end()) {
      // The requested index already exists for this table
      return NULL_INDEX_INFO;
    }

    // Construct index metdata
    auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs);

    // Construct the index, take ownership of metadata
    // TODO(Kyle): We should update the API for CreateIndex
    // to allow specification of the index type itself, not
    // just the key, value, and comparator types

    // TODO(chi): support both hash index and btree index
    auto index = std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_);

    // Populate the index with all tuples in table heap
    auto *table_meta = GetTable(table_name);
    auto *heap = table_meta->table_.get();
    for (auto tuple = heap->Begin(txn); tuple != heap->End(); ++tuple) {
      index->InsertEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), tuple->GetRid(), txn);
    }

    // Get the next OID for the new index
    const auto index_oid = next_index_oid_.fetch_add(1);

    // Construct index information; IndexInfo takes ownership of the Index itself
    auto index_info =
        std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, keysize);
    auto *tmp = index_info.get();

    // Update internal tracking
    indexes_.emplace(index_oid, std::move(index_info));
    table_indexes.emplace(index_name, index_oid);

    return tmp;
  }

  /**
   * Get the index `index_name` for table `table_name`.
   * @param index_name The name of the index for which to query
   * @param table_name The name of the table on which to perform query
   * @return A (non-owning) pointer to the metadata for the index
   */
  auto GetIndex(const std::string &index_name, const std::string &table_name) -> IndexInfo * {
    auto table = index_names_.find(table_name);
    if (table == index_names_.end()) {
      BUSTUB_ASSERT((table_names_.find(table_name) == table_names_.end()), "Broken Invariant");
      return NULL_INDEX_INFO;
    }

    auto &table_indexes = table->second;

    auto index_meta = table_indexes.find(index_name);
    if (index_meta == table_indexes.end()) {
      return NULL_INDEX_INFO;
    }

    auto index = indexes_.find(index_meta->second);
    BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");

    return index->second.get();
  }

  /**
   * Get the index `index_name` for table identified by `table_oid`.
   * @param index_name The name of the index for which to query
   * @param table_oid The OID of the table on which to perform query
   * @return A (non-owning) pointer to the metadata for the index
   */
  auto GetIndex(const std::string &index_name, const table_oid_t table_oid) -> IndexInfo * {
    // Locate the table metadata for the specified table OID
    auto table_meta = tables_.find(table_oid);
    if (table_meta == tables_.end()) {
      // Table not found
      return NULL_INDEX_INFO;
    }

    return GetIndex(index_name, table_meta->second->name_);
  }

  /**
   * Get the index identifier by index OID.
   * @param index_oid The OID of the index for which to query
   * @return A (non-owning) pointer to the metadata for the index
   */
  auto GetIndex(index_oid_t index_oid) -> IndexInfo * {
    auto index = indexes_.find(index_oid);
    if (index == indexes_.end()) {
      return NULL_INDEX_INFO;
    }

    return index->second.get();
  }

  /**
   * Get all of the indexes for the table identified by `table_name`.
   * @param table_name The name of the table for which indexes should be retrieved
   * @return A vector of IndexInfo* for each index on the given table, empty vector
   * in the event that the table exists but no indexes have been created for it
   */
  auto GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *> {
    // Ensure the table exists
    if (table_names_.find(table_name) == table_names_.end()) {
      return std::vector<IndexInfo *>{};
    }

    auto table_indexes = index_names_.find(table_name);
    BUSTUB_ASSERT((table_indexes != index_names_.end()), "Broken Invariant");

    std::vector<IndexInfo *> indexes{};
    indexes.reserve(table_indexes->second.size());
    for (const auto &index_meta : table_indexes->second) {
      auto index = indexes_.find(index_meta.second);
      BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");
      indexes.push_back(index->second.get());
    }

    return indexes;
  }

  auto GetTableNames() -> std::vector<std::string> {
    std::vector<std::string> result;
    for (const auto &x : table_names_) {
      result.push_back(x.first);
    }
    return result;
  }

 private:
  // 缓冲池管理器
  [[maybe_unused]] BufferPoolManager *bpm_;

  // 锁管理器
  [[maybe_unused]] LockManager *lock_manager_;

  // 日志管理器
  [[maybe_unused]] LogManager *log_manager_;

  /**
   * Map table identifier -> table metadata.
   * 表 OID -> 表元数据（TableInfo）
   * NOTE: `tables_` owns all table metadata.
   */
  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;

  /** Map table name -> table identifiers. */
  /** 表名 -> 表 OID */
  std::unordered_map<std::string, table_oid_t> table_names_;

  /** The next table identifier to be used. */
  /** 生成新的表 OID */
  std::atomic<table_oid_t> next_table_oid_{0};

  /**
   * Map index identifier -> index metadata.
   * 索引 OID -> 索引元数据（IndexInfo）
   * NOTE: that `indexes_` owns all index metadata.
   */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;

  /** Map table name -> index names -> index identifiers. */
  /** 表名 -> (索引名 -> 索引 OID) */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;

  /** The next index identifier to be used. */
  /** 用于生成新的索引 OID */
  std::atomic<index_oid_t> next_index_oid_{0};
};

}  // namespace bustub
