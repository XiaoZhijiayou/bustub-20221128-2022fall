//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * =======================INSERT=======================
 *
 * The InsertExecutor inserts tuples into a table and updates indexes.
 * 用于将元组插入到表中并更新索引
 *
 * Input: It has exactly one child producing values to be inserted into the table.
 * The planner will ensure values have the same schema as the table.
 * 输入：它有一个子节点产生要插入到表中的值.规划器将确保这些值与表的模式一致。
 *
 * Output: The executor will produce a single tuple of an integer number as the output,
 * indicating how many rows have been inserted into the table, after all rows are inserted.
 * 输出 ：执行器将在所有行插入后，产生一个整数类型的元组，表示插入到表中的行数。
 * Remember to update the index when inserting into the table, if it has an index associated with it.
 * 请记得在插入到表中时更新索引，如果表有与之关联的索引的话。
 * Hint: You will need to lookup table information for the target of the insert during executor initialization.
 * See the System Catalog section below for additional information on accessing the catalog.
 * 提示：在执行器初始化期间，您需要查找插入目标的表信息。有关访问目录的更多信息，请参见系统目录部分。
 * Hint: You will need to update all indexes for the table into which tuples are inserted.
 * See the Index Updates section below for further details.
 * 提示：您需要更新插入到其中的所有索引表。有关更多详细信息，请参见索引更新部分。
 * Hint: You will need to use the TableHeap class to perform table modifications.
 * 提示：您需要使用TableHeap类来执行表修改。
 * Keys: table , index.
 */

 
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    child_executor_->Init();
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    table_ = table_info->table_.get();
    schema_ = std::make_unique<Schema>(table_info->schema_);
    indexes_ = std::make_unique<std::vector<IndexInfo *>>(exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_));
    done_ = false;
    // Lock table， IX mode in any isolation level
    try {
        auto ok = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), 
                                                            LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
        if (!ok) {
            exec_ctx_->GetTransaction()->LockTxn();
            exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
            exec_ctx_->GetTransaction()->UnlockTxn();
            throw ExecutionException("InsertExecutor fails to lock table");
        }
    } catch (TransactionAbortException &err) {
        throw ExecutionException(err.GetInfo());
    }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if (done_) {
        // Unlock the rows. It is not allowed under any isolation level.
        //  Unlock the table. Under repeatable read.
        return false;
    }
    // tuple_to_insert 和 rid_to_insert：用于存储从子执行器中获取的待插入的元组及其对应的记录标识符。
    Tuple tuple_to_insert;
    RID rid_to_insert;
    // 计数器，记录成功插入的元组数量。
    int32_t num_inserted(0);
    // 定义了输出元组的模式，这里只包含一列 "size"，类型为 INTEGER。最终输出的元组将只包含一个整数，表示插入的行数。
    Schema schema(std::vector<Column>{Column("size", TypeId::INTEGER)});
    while (child_executor_->Next(&tuple_to_insert, &rid_to_insert)) {
        //  Lock the row in X mode under any isolation level.
        // 为当前插入的元组加独占锁（写锁），确保在插入过程中其他事务不会修改该行。
        try {
            auto ok = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, 
                                                            plan_->table_oid_, rid_to_insert);
            if (!ok) {
                exec_ctx_->GetTransaction()->LockTxn();
                exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
                exec_ctx_->GetTransaction()->UnlockTxn();
                throw ExecutionException("InsertExecutor fails to lock row");
            }
            locked_rids_.emplace_back(rid_to_insert);
        } catch (TransactionAbortException &err) {
            throw ExecutionException(err.GetInfo());    
        }
        // 调用表堆（TableHeap）的 InsertTuple 方法，将从子执行器获取的元组插入到目标表中，并更新 rid_to_insert 为实际插入后的记录标识符。
        table_->InsertTuple(tuple_to_insert, &rid_to_insert, exec_ctx_->GetTransaction());
        // 下面相当于，从元组中提取出来索引的元组index_tuple
        // 然后再将index_tuple插入到索引中去，构建键值与实际记录位置之间的映射关系
        for (auto index_info : *indexes_) {
            Tuple index_tuple = 
                tuple_to_insert.KeyFromTuple(*schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->InsertEntry(index_tuple, rid_to_insert, exec_ctx_->GetTransaction());
        }
        ++num_inserted;
    }
    // 构造了一个包含单个整数值的元组，用于表示插入操作完成后插入的行数
    *tuple = Tuple(std::vector<Value>{Value(TypeId::INTEGER, num_inserted)}, &schema);
    done_ = true;
    return true;
}

}  // namespace bustub
