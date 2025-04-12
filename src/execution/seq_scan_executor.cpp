//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <algorithm>
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
        : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    // 1. 从 Catalog 获取目标表的 TableHeap
    table_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    // 2. 创建两个 TableIterator 对象，一个指向表的开始，一个指向结束
    table_current_iterator_ = std::make_unique<TableIterator>(table_->Begin(exec_ctx_->GetTransaction()));
    table_end_iterator_ = std::make_unique<TableIterator>(table_->End());
    // lock the table
    // 3. 根据隔离级别，对目标表加锁,
    // 对于当前事务的隔离级别（非 READ_UNCOMMITTED），对整个表进行意向共享锁（INTENTION_SHARED）加锁，确保扫描期间表不会被其他事务不当修改。
    try {
        auto tnx = exec_ctx_->GetTransaction();
        if (tnx->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            auto ok = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction()
                                                                , LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
            if (!ok) {
                throw ExecutionException("SeqScanExecutor fails to lock table");
            }
        }
    } catch (TransactionAbortException &err) {
        throw ExecutionException(err.GetInfo());
    }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (*table_current_iterator_ ==  table_->End()) {
        // 当迭代器到达表尾，执行解锁操作（根据隔离级别）并返回 false。
        // Unlock the row.
        if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
           std::for_each(locked_rids_.cbegin(), locked_rids_.cend(), [&](const RID &locked_rid) {
                auto ok = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, locked_rid);
                if (!ok) {
                    exec_ctx_->GetTransaction()->LockTxn();
                    exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
                    exec_ctx_->GetTransaction()->UnlockTxn();
                }
            });
            // Unlock the table
            if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
                auto ok = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);
                if (!ok) {
                    exec_ctx_->GetTransaction()->LockTxn();
                    exec_ctx_->GetTransaction()->SetState(TransactionState::ABORTED);
                    exec_ctx_->GetTransaction()->UnlockTxn();
                }
            } 
        }
        return false;
    }
    // 将当前迭代器指向的元组的 RID 返回给调用者
    *rid = (*table_current_iterator_)->GetRid();
    // 从表堆中读取对应的元组数据
    table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());

    // 如果隔离级别不为 READ_UNCOMMITTED，对该行进行共享锁定
    try{
        auto txn = exec_ctx_->GetTransaction();
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
            auto ok = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction()
                                                           , LockManager::LockMode::SHARED, plan_->GetTableOid(), *rid);
            if (!ok) {
                throw ExecutionException("SeqScanExecutor fails to lock row");
            }
            locked_rids_.emplace_back(*rid);
        }
    } catch (TransactionAbortException &err) {
        throw ExecutionException(err.GetInfo());
    }
    // 前进迭代器
    ++(*table_current_iterator_);
    return true;
}
    
}  // namespace bustub
