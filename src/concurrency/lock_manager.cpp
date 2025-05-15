
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <algorithm>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

/******************************* < Hoo Debug Log - Begin > *******************************/

// Uncomment the following define statement will show the log, otherwise, not.
#define HOO_ALLOW_DEBUG_LOG
#ifdef HOO_ALLOW_DEBUG_LOG
static std::mutex debug_log_mutex;
static auto InternalGetDebugThreadID() -> int64_t {
  static std::vector<std::thread::id> thread_ids;
  static std::mutex local_mutex;
  std::scoped_lock lock(local_mutex);
  auto my_id = std::this_thread::get_id();
  for (size_t i = 0; i < thread_ids.size(); ++i) {
    if (thread_ids[i] == my_id) {
      return i;
    }
  }
  thread_ids.push_back(my_id);
  return thread_ids.size() - 1;
}

#define DEBUG_THREAD_ID (InternalGetDebugThreadID())
#define THREAD_DEBUG_LOG(...) \
  debug_log_mutex.lock();     \
  LOG_DEBUG(__VA_ARGS__);     \
  debug_log_mutex.unlock();
#else
template <typename... Args>
inline void foo(Args... args) {}
#define DEBUG_THREAD_ID (1L)
#define THREAD_DEBUG_LOG(...) foo(__VA_ARGS__);

#endif


namespace bustub {

// 对表oid申请lock_mode锁
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  THREAD_DEBUG_LOG("[thread T%ld] txn %d(%p - %s) acquire %s lock on table %d: start", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), txn, TransactionStateToString(txn->GetState()).data(),
                   LockModeToString(lock_mode).data(), oid);
  // 对事务加锁                
  txn->LockTxn();
  // 如果事务被标记为aborted，直接返回false
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    return false;
  }
  // 检查在当前的隔离级别下，是否允许申请该锁
  if (AbortReason abort_reason;
      !LockRestrictionCheck(txn->GetState(), txn->GetIsolationLevel(), lock_mode, abort_reason)) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'isolation and state restriction'", DEBUG_THREAD_ID,
                     txn->GetTransactionId());
    AbortTransaction(txn, abort_reason);
  }

  // 获取表的请求队列
  table_lock_map_latch_.lock(); 
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  // 检查事务是否已经持有该表的锁
  bool granted = false;
  LockMode held_lock_mode;
  std::shared_ptr<std::unordered_set<table_oid_t>> held_lock_set;
  if (IsTransactionHoldLockOnTable(txn, oid, held_lock_mode, held_lock_set)) {
    // Case A: transaction hold  lock on the table.
    if (held_lock_mode == lock_mode) {
      // The lock is already held by the txn.
      granted = true;
    } else if (LockUpgradeCheck(held_lock_mode, lock_mode)) {
      // 检查是否允许升级锁
      {
        // Release the lock on time, the concurrent lock can be possible.
        std::scoped_lock lock(request_queue->latch_);
        if (request_queue->upgrading_ != INVALID_TXN_ID) {
          // 已经有其他事务在升级同一资源，此时事务不能同时升级，触发升级冲突，直接中止当前的事务
          THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'request_queue->upgrading_ != INVALID_TXN_ID'",
                           DEBUG_THREAD_ID, txn->GetTransactionId());
          AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
        }
        // 标记本事务开始升级
        request_queue->upgrading_ = txn->GetTransactionId();
      }
      // 从原集合移除
      held_lock_set->erase(oid);
      // Move this lock upgrade request to the first position of the waiting list.
      std::unique_lock request_queue_lock(request_queue->latch_);
      auto held_lock_request_iter =
          std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                       [txn](LockRequest *request) { return request->txn_id_ == txn->GetTransactionId(); });
      // 在队列中找到原来那个原本属于本事务的LockRequest节点，并取出它的指针。
      auto held_lock_request = *held_lock_request_iter;
      // 从原位删除
      request_queue->request_queue_.erase(held_lock_request_iter);
      // 插入到第一个还没被授予的请求位置
      auto first_waiting_lock_request =
          std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                       [](LockRequest *request) { return !request->granted_; });
      // 把旧的请求插入到第一个未授予请求之前，也就是优先处理升级
      auto this_request_iter = request_queue->request_queue_.insert(first_waiting_lock_request, held_lock_request);
      // 找到原请求的位置并且移除
      // 插入到第一个还没被授予的请求界面
      held_lock_request->granted_ = false;
      held_lock_request->lock_mode_ = lock_mode;
      bool want_wake;
      // 如果与前面的已经授予的锁不兼容，则阻塞等待
      // 醒来后重新检查事务状态，最终将granted_ = true 或失败并返回
      std::tie(granted, want_wake) = RequestLock(txn, lock_mode, request_queue, this_request_iter, request_queue_lock);
      // 清除升级标记，清除标记
      request_queue->upgrading_ = INVALID_TXN_ID;
      // 如果没有拿到锁，要做清理
      if (!granted) {
        delete held_lock_request;
        txn->UnlockTxn();
        if (want_wake) {
          request_queue_lock.unlock();
          request_queue->cv_.notify_all();
        }
        return false;
      }
      // 如果需要，还需要唤醒下一个兼容事务
      if (want_wake) {
        request_queue_lock.unlock();
        request_queue->cv_.notify_all();
      }
    } else {
      // Lock upgrade is not allowed.
      THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'not allowed upgrade'", DEBUG_THREAD_ID, txn->GetTransactionId());
      AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else {
    // Case B: Txn does not hold any lock on the table.
    auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    std::unique_lock request_queue_lock(request_queue->latch_);
    // 将新请求追加到request_queue->request_queue_的末尾，并记录迭代器
    request_queue->request_queue_.emplace_back(lock_request);
    auto lock_request_iter = std::prev(request_queue->request_queue_.end());
    bool want_wake;
    std::tie(granted, want_wake) = RequestLock(txn, lock_mode, request_queue, lock_request_iter, request_queue_lock);
    // 失败处理
    if (!granted) {
      delete lock_request;
      txn->UnlockTxn();
      if (want_wake) {
        request_queue_lock.unlock();
        request_queue->cv_.notify_all();
      }
      return false;
    }
    // 成功后唤醒
    if (want_wake) {
      request_queue_lock.unlock();
      request_queue->cv_.notify_all();
    }
  }
  // Update the status of the txn lock set. In this point, granted is true,
  // 更新事务的锁集合
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
      break;
  }
  THREAD_DEBUG_LOG("[thread T%ld] txn %d acquire %s lock on table %d: done", DEBUG_THREAD_ID, txn->GetTransactionId(),
                   LockModeToString(lock_mode).data(), oid);
  txn->UnlockTxn();
  return granted;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  THREAD_DEBUG_LOG("[thread T%ld] txn %d(%p - %s) begins to release lock on table %d: start", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), txn, TransactionStateToString(txn->GetState()).data(), oid);
  txn->LockTxn();
  // txn hold any lock on table oid?
  // 事务在任何一种表锁集合中都找不到oid，说明他没有持有这把锁，触发异常并终止。
  if (txn->GetSharedTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
      txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
      txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
      txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'unlock the unlocked lock'", DEBUG_THREAD_ID,
                     txn->GetTransactionId());
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // txn hold any row lock of this table?
  // 如果事务对该表下的行加了锁，则不能释放表锁，必须先释放行锁，否则中止处理。
  if (TransactionIsLockingRowsOfTable(txn, oid)) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'row locking is held'", DEBUG_THREAD_ID, txn->GetTransactionId());
    AbortTransaction(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // 获取表oid的锁请求队列
  table_lock_map_latch_.lock();
  auto &request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  LockMode lock_mode;
  {
    // Remove
    std::unique_lock lock(request_queue->latch_);
    // 删除自身的请求和记录锁的模式
    auto lock_request = std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                                     [txn, oid](LockRequest *request) {
                                       return request->txn_id_ == txn->GetTransactionId() && oid == request->oid_;
                                     });
    lock_mode = (*lock_request)->lock_mode_;
    delete *lock_request;
    request_queue->request_queue_.erase(lock_request);
    // Wake
    // 唤醒下一个等待者，找到第一个还没被授予的请求，把它的事务ID写入wake_id_
    // 解锁队列互斥锁后，唤醒在该cv_上等待的线程
    auto first_waiting = std::find_if(request_queue->request_queue_.cbegin(), request_queue->request_queue_.cend(),
                                      [](LockRequest *request) { return !request->granted_; });
    if (first_waiting != request_queue->request_queue_.cend()) {
      request_queue->wake_id_ = (*first_waiting)->txn_id_;
      THREAD_DEBUG_LOG("[thread %ld]txn %d notify %d. Queue: %s", DEBUG_THREAD_ID, txn->GetTransactionId(),
                       request_queue->wake_id_, request_queue->ToString().c_str());
      lock.unlock();
      request_queue->cv_.notify_all();
    } else {
      THREAD_DEBUG_LOG("[thread %ld]txn %d notify no one", DEBUG_THREAD_ID, txn->GetTransactionId());
    }
  }
  // 根据事务的隔离级别更新事务状态
  switch (txn->GetIsolationLevel()) {
    // REPEATABLE_READ：释放 S/X 都转入 SHRINKING
    case IsolationLevel::REPEATABLE_READ:
      if ((lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    // READ_COMMITTED：仅释放 X 转 SHRINKING
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    // READ_UNCOMMITTED：释放 X 转 SHRINKING，释放 S 视为错误
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      } else if (lock_mode == LockMode::SHARED) {
        // This is a UDB according to the [UNLOCK_NOTE]. But this abort reason seems good enough.
        AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }
      break;
  }
  // 更新事务锁的集合
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
  txn->UnlockTxn();
  THREAD_DEBUG_LOG("[thread T%ld] txn %d release the %s lock on the table %d: done", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), LockModeToString(lock_mode).data(), oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  THREAD_DEBUG_LOG("[thread T%ld] txn %d(%p,%s) acquire %s lock on %d:%ld: start", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), txn, TransactionStateToString(txn->GetState()).data(),
                   LockModeToString(lock_mode).data(), oid, rid.Get());
  txn->LockTxn();
  // 事务状态检查
  if (txn->GetState() == TransactionState::ABORTED) {
    txn->UnlockTxn();
    return false;
  }
  AbortReason abort_reason;
  // 检查锁的模式是否合法，只允许对行申请共享锁和排他锁，其他模式的锁视为非法，立即终止事务
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  // 根据事务的隔离级别和状态判断是否允许申请此锁，不通过则终止并返回相应的错误码
  if (!LockRestrictionCheck(txn->GetState(), txn->GetIsolationLevel(), lock_mode, abort_reason)) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'isolation and state restriction'", DEBUG_THREAD_ID,
                     txn->GetTransactionId());
    AbortTransaction(txn, abort_reason);
  }
  // 检查事务是否持有表锁，合适的意向锁（IS/IX/SIX）或更强锁
  if (!EnsureProperTableLockForRow(lock_mode, oid)) {
    AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // 获取或创建对应行的锁队列
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  bool granted;
  LockMode held_lock_mode;
  std::unordered_set<RID> *held_lock_set;
  // 检查事务是否已持有该行锁
  bool txn_holding_this_row_lock = false;
  if ((*(txn->GetSharedRowLockSet()))[oid].count(rid) > 0) {
    held_lock_set = &(*(txn->GetSharedRowLockSet()))[oid];
    held_lock_mode = LockMode::SHARED;
    txn_holding_this_row_lock = true;
  } else if ((*(txn->GetExclusiveRowLockSet()))[oid].count(rid) > 0) {
    held_lock_set = &(*(txn->GetExclusiveRowLockSet()))[oid];
    held_lock_mode = LockMode::EXCLUSIVE;
    txn_holding_this_row_lock = true;
  }
  if (txn_holding_this_row_lock) {
    // Case A: transaction is holding a lock on the row.
    THREAD_DEBUG_LOG("[thread T%ld] txn %d is holding row lock", DEBUG_THREAD_ID, txn->GetTransactionId());
    if (held_lock_mode == lock_mode) {
      // Same lock granted to txn
      granted = true;
    } else if (RowLockUpgradeCheck(held_lock_mode, lock_mode)) {
      // 锁进行升级
      // Lock Upgrade.
      // 1. 先标记upgrading_ 
      {
        // Release the lock on time, the concurrent lock can be possible.
        std::scoped_lock lock(request_queue->latch_);
        if (request_queue->upgrading_ != INVALID_TXN_ID) {
          THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'request_queue->upgrading_ != INVALID_TXN_ID'",
                           DEBUG_THREAD_ID, txn->GetTransactionId());
          AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
        }
        request_queue->upgrading_ = txn->GetTransactionId();
      }
      THREAD_DEBUG_LOG("[thread T%ld] txn %d row lock upgrade", DEBUG_THREAD_ID, txn->GetTransactionId());
      // 2. 从held_lock_set中删除rid
      held_lock_set->erase(rid);
      // Move this lock upgrade request to the first position of the waiting list.
      std::unique_lock request_queue_lock(request_queue->latch_);
      // 3. 在request_queue中找到原本属于本事务的LockRequest节点，并取出它的指针。将其再插入到第一个未授予请求之前
      auto held_lock_request_iter =
          std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                       [txn](LockRequest *request) { return request->txn_id_ == txn->GetTransactionId(); });
      auto held_lock_request = *held_lock_request_iter;
      request_queue->request_queue_.erase(held_lock_request_iter);
      auto first_waiting_lock_request =
          std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                       [](LockRequest *request) { return !request->granted_; });
      auto this_request_iter = request_queue->request_queue_.insert(first_waiting_lock_request, held_lock_request);
      // 4. 更新该LockRequest的granted_和lock_mode_
      held_lock_request->granted_ = false;
      held_lock_request->lock_mode_ = lock_mode;
      bool want_wake;
      // 5. 检查是否与前面的已经授予的锁不兼容，如果不兼容，则阻塞等待
      std::tie(granted, want_wake) = RequestLock(txn, lock_mode, request_queue, this_request_iter, request_queue_lock);
      // 6. 清除升级标记，清除标记，并且唤醒其他等待线程
      request_queue->upgrading_ = INVALID_TXN_ID;
      if (!granted) {
        delete held_lock_request;
        txn->UnlockTxn();
        if (want_wake) {
          request_queue_lock.unlock();
          request_queue->cv_.notify_all();
        }
        return false;
      }
      if (want_wake) {
        request_queue_lock.unlock();
        request_queue->cv_.notify_all();
      }
    } else {
      // Lock upgrade is not allowed.
      THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'not allowed upgrade'", DEBUG_THREAD_ID, txn->GetTransactionId());
      AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else {
    // 事务未持有行锁，需要申请新的锁请求
    // Case B: Transaction does not hold any lock on the row.
    std::unique_lock request_queue_lock(request_queue->latch_);
    // 1. 创建请求，兼容性检查
    auto lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    auto lock_request_iter = request_queue->request_queue_.insert(request_queue->request_queue_.end(), lock_request);
    // Whether this request lock can be granted immediately or wait.
    bool want_wake;
    std::tie(granted, want_wake) = RequestLock(txn, lock_mode, request_queue, lock_request_iter, request_queue_lock);
    // 2. 处理失败，删除请求，解锁队列，唤醒其他等待线程
    if (!granted) {
      delete lock_request;
      txn->UnlockTxn();
      if (want_wake) {
        request_queue_lock.unlock();
        request_queue->cv_.notify_all();
      }
      return false;
    }
    if (want_wake) {
      request_queue_lock.unlock();
      request_queue->cv_.notify_all();
    }
  }
  // Update the status of the txn lock set. In this point, granted is true,
  // 更新事务的行锁的集合
  switch (lock_mode) {
    case LockMode::SHARED:
      (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
      break;
    default:
      BUSTUB_ASSERT(false, "Attempted intention lock on row and reach unexpected code");
  }
  THREAD_DEBUG_LOG("[thread T%ld] txn %d acquire %s lock on %d:%ld: done", DEBUG_THREAD_ID, txn->GetTransactionId(),
                   LockModeToString(lock_mode).data(), oid, rid.Get());
  txn->UnlockTxn();
  return granted;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  THREAD_DEBUG_LOG("[thread T%ld] txn %d(%p,%s) begins to release lock on %d:%ld: start", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), txn, TransactionStateToString(txn->GetState()).data(), oid, rid.Get());
  txn->LockTxn();
  // txn hold any lock on table oid?
  // 校验事务确实持有该行锁？
  if (txn->GetSharedRowLockSet()->count(oid) == 0 && txn->GetExclusiveRowLockSet()->count(oid) == 0) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d violated 'unlock the unlocked lock'", DEBUG_THREAD_ID,
                     txn->GetTransactionId());
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  row_lock_map_latch_.lock();
  auto &request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  LockMode lock_mode;
  {
    // Remove
    std::unique_lock lock(request_queue->latch_);
    // 找到并删除当前事务的lock_request节点
    auto lock_request = std::find_if(request_queue->request_queue_.begin(), request_queue->request_queue_.end(),
                                     [txn, oid](LockRequest *request) {
                                       return request->txn_id_ == txn->GetTransactionId() && oid == request->oid_;
                                     });
    BUSTUB_ASSERT(lock_request != request_queue->request_queue_.end(), "?");
    lock_mode = (*lock_request)->lock_mode_;
    delete *lock_request;
    request_queue->request_queue_.erase(lock_request);
    // 查找下一个未granted的请求
    auto first_waiting = std::find_if(request_queue->request_queue_.cbegin(), request_queue->request_queue_.cend(),
                                      [](LockRequest *request) { return !request->granted_; });
    // 判断请求是否与前面所有已授予的请求兼容
    auto compatibility_check = [&request_queue, first_waiting, this]() {
      if (first_waiting == request_queue->request_queue_.cend()) {
        return false;
      }
      if (request_queue->request_queue_.size() == 1) {
        return true;
      }
      return std::all_of(request_queue->request_queue_.cbegin(), first_waiting, [&](const LockRequest *request) {
        return IsLockModeCompatible(request->lock_mode_, (*first_waiting)->lock_mode_);
      });
    }();
    // 如果可以唤醒，则记录 wake_id_ 并通知所有等待线程
    if (first_waiting != request_queue->request_queue_.cend() && compatibility_check) {
      request_queue->wake_id_ = (*first_waiting)->txn_id_;
      lock.unlock();
      request_queue->cv_.notify_all();
    }
  }
  // 根据事务的隔离级别更新事务状态
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if ((lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE && !IsTransactionEnded(txn)) {
        txn->SetState(TransactionState::SHRINKING);
      } else if (lock_mode == LockMode::SHARED) {
        // This is a UDB according to the [UNLOCK_NOTE]. But this abort reason seems good enough.
        AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }
      break;
  }
  // Update the transaction lock set.
  // 从事务内部锁集合删除该行
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      break;
    default:
      BUSTUB_ASSERT(false, "Attempted unlock intention lock on row and reach unexpected code");
  }
  // 解锁事务
  txn->UnlockTxn();
  THREAD_DEBUG_LOG("[thread T%ld] txn %d release the %s lock on the table %d: done", DEBUG_THREAD_ID,
                   txn->GetTransactionId(), LockModeToString(lock_mode).data(), oid);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

// No lock is acquired in this function directly,
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 对每个原事务调用一次深度优先搜索
  return std::any_of(waits_for_.cbegin(), waits_for_.cend(), 
                     [this, txn_id](const std::pair<txn_id_t, std::set<txn_id_t>> &source) {
                       return DepthFirstSearchCycle(source.first, *txn_id, std::unordered_set<txn_id_t>{});
                     });
}

// No lock is acquired in this function.
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[source, tails] : waits_for_) {
    for (const auto &tail : tails) {
      edges.emplace_back(source, tail);
    }
  }
  return edges;
}

// 后台的死锁检测线程
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    // 定时唤醒原则
    {
      std::scoped_lock lock(waits_for_latch_); // 加锁
      txn_id_t cycle_maker = INVALID_TXN_ID;
    DETECTION:
    // 重构等待图
      BuildWaitForGraph();
      // 检查是否又回环，如果有回环就说明有死锁产生
      if (HasCycle(&cycle_maker)) {
        THREAD_DEBUG_LOG("[thread %ld] DeadlockDetector: begin to abort %d", DEBUG_THREAD_ID, cycle_maker);
        // 终止环路制造者的事务
        auto cycle_maker_txn = TransactionManager::GetTransaction(cycle_maker);
        cycle_maker_txn->SetState(TransactionState::ABORTED);
        BUSTUB_ASSERT(waiting_transactions_.count(cycle_maker) != 0,
                      "cycle maker has to be in the waiting transactions");
                      //唤醒相关的等待队列
        for (auto &queue : waiting_transactions_[cycle_maker]) {
          {
            std::lock_guard queue_lock(queue->latch_);
            queue->wake_id_ = cycle_maker;
          }
          queue->cv_.notify_all();
        }
        // 清空等待图，并重新检测
        waits_for_.clear();
        goto DETECTION;
      }
    }
  }
}

/******************************** Helper functions *******************************/

auto LockManager::IsLockModeCauseWait(const bustub::LockManager::LockRequestQueue &request_queue,
                                      const bustub::LockManager::LockMode &new_mode,
                                      std::list<LockRequest *>::const_iterator request_iter) -> bool {
                                        // 保证请求队列非空
  BUSTUB_ASSERT(!request_queue.request_queue_.empty(), "lock request should not be empty when checking wait");
  // 如果这是队列中唯一的请求，则不会等待
  if (request_queue.request_queue_.size() == 1) {
    return false;
  }
  // 从当前请求向队头反向遍历，检查所有在"在它前面“已授予锁
  std::list<LockRequest *>::const_reverse_iterator reversed_request_iter(request_iter);
  // 如果存在任何一个前面的请求：
  //   - 已经被 granted_（已授权），且
  //   - 与 new_mode **不兼容**，
  // 则新请求就必须等待。
  // 这里用 all_of 检查“所有前面已授予的锁都兼容 new_mode”，
  // 如果不是全部兼容（!all_of），就返回 true（需要等待）。
  return !std::all_of(reversed_request_iter, request_queue.request_queue_.crend(),
                      [new_mode, this](const LockRequest *lock_request) {
                        return lock_request->granted_ && IsLockModeCompatible(new_mode, lock_request->lock_mode_);
                      });
}

// 检查给定事务txn是否已经对表 oid 加了某种表级锁。
auto LockManager::IsTransactionHoldLockOnTable(bustub::Transaction *txn, const bustub::table_oid_t &oid,
                                               bustub::LockManager::LockMode &held_lock_mode,
                                               std::shared_ptr<std::unordered_set<table_oid_t>> &table_lock_set)
    -> bool {
  if (txn->GetSharedTableLockSet()->count(oid) > 0) {
    table_lock_set = txn->GetSharedTableLockSet();
    held_lock_mode = LockMode::SHARED;
    return true;
  }
  if (txn->GetExclusiveTableLockSet()->count(oid) > 0) {
    table_lock_set = txn->GetExclusiveTableLockSet();
    held_lock_mode = LockMode::EXCLUSIVE;
    return true;
  }
  if (txn->GetIntentionSharedTableLockSet()->count(oid) > 0) {
    table_lock_set = txn->GetIntentionSharedTableLockSet();
    held_lock_mode = LockMode::INTENTION_SHARED;
    return true;
  }
  if (txn->GetIntentionExclusiveTableLockSet()->count(oid) > 0) {
    table_lock_set = txn->GetIntentionExclusiveTableLockSet();
    held_lock_mode = LockMode::INTENTION_EXCLUSIVE;
    return true;
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) > 0) {
    table_lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
    held_lock_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
    return true;
  }
  return false;
}

auto LockManager::IsLockModeCompatible(const LockMode &lhs, const LockMode &rhs) -> bool {
  switch (lhs) {
    case LockMode::INTENTION_SHARED:
      // IS 与除 EXCLUSIVE 之外的任何锁都兼容
      return rhs != LockMode::EXCLUSIVE;
    case LockMode::INTENTION_EXCLUSIVE:
      // IX 仅与 IS 或 IX 兼容
      return rhs == LockMode::INTENTION_SHARED || rhs == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED:
      // S 仅与 S 或 IS 兼容
      return rhs == LockMode::SHARED || rhs == LockMode::INTENTION_SHARED;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      // SIX 仅与 IS 兼容
      return rhs == LockMode::INTENTION_SHARED;
    case LockMode::EXCLUSIVE:
      // X 与任何锁都不兼容
      return false;
  }
}

auto LockManager::LockUpgradeCheck(const LockMode &old_mode, const LockMode &new_mode) -> bool {
  /**
   * Allowed upgrade:
   *        S -> [X, SIX]
   *        IS -> [S, X, IX, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   */
  switch (old_mode) {
    case LockMode::SHARED:
      // 共享锁 S 只能升级到 X 或 SIX
      return new_mode == LockMode::EXCLUSIVE || new_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::INTENTION_SHARED:
      // 意向共享 IS 能升级到除了自己以外的任意模式
      return new_mode != LockMode::INTENTION_SHARED;
    case LockMode::INTENTION_EXCLUSIVE:
      // 意向排它 IX 只能升级到 X 或 SIX
      return new_mode == LockMode::EXCLUSIVE || new_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      // SIX 只能升级到最强的 X
      return new_mode == LockMode::EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      // X 已经是最强模式，不可再升级
      return false;
  }
}

// 在给事务申请一个新锁之前，根据当前事务状态（Growing/Shrinking）和隔离级别，判断该锁请求是否被允许
auto LockManager::LockRestrictionCheck(const TransactionState &state, const IsolationLevel &isolation_level,
                                       const LockMode &lock_mode, AbortReason &abort_reason) -> bool {
  bool res = true;
  switch (isolation_level) {
    case IsolationLevel::REPEATABLE_READ:
      // All locks are allowed in the GROWING state
      // No locks are allowed in the SHRINKING state
      if (state != TransactionState::GROWING) {
        res = false;
        abort_reason = AbortReason::LOCK_ON_SHRINKING;
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if ((state != TransactionState::GROWING) &&
          (state != TransactionState::SHRINKING ||
           (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED))) {
        res = false;
        // No proper given abort reason, this might be fine.
        abort_reason = AbortReason::LOCK_ON_SHRINKING;
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // X, IX locks are allowed in the GROWING state.
      // S, IS, SIX locks are never allowed
      if ((lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) ||
          state != TransactionState::GROWING) {
        res = false;
        abort_reason = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
      }
      break;
  }
  return res;
}

// 检查给定事务在指定表上是否持有任意行级锁（共享或排它）
auto LockManager::TransactionIsLockingRowsOfTable(Transaction *txn, const table_oid_t &oid) -> bool {
  return !(*txn->GetExclusiveRowLockSet())[oid].empty() || !(*txn->GetSharedRowLockSet())[oid].empty();
}

// 将锁模式转换为字符串
auto LockManager::LockModeToString(bustub::LockManager::LockMode lock_mode) -> std::string_view {
  switch (lock_mode) {
    case LockMode::SHARED:
      return "SHARED";
    case LockMode::EXCLUSIVE:
      return "EXCLUSIVE";
    case LockMode::INTENTION_SHARED:
      return "INTENTION_SHARED";
    case LockMode::INTENTION_EXCLUSIVE:
      return "INTENTION_EXCLUSIVE";
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SHARED_INTENTION_EXCLUSIVE";
  }
}

//校验一次行级锁的升级操作是否被允许
auto LockManager::RowLockUpgradeCheck(const LockMode &old_mode, const LockMode &new_mode) -> bool {
  return old_mode == LockMode::SHARED && new_mode == LockMode::EXCLUSIVE;
}

// 在真正给某行上加行级锁（S 或 X）之前，验证事务是否已经持有与之匹配的“表级意向锁”或更高级的表级锁。
auto LockManager::EnsureProperTableLockForRow(const LockMode &row_lock_mode, const table_oid_t &oid) -> bool {
  // 1) 先从 table_lock_map_ 中取出对应表的锁队列指针
  table_lock_map_latch_.lock();
  auto no_table_lock = (table_lock_map_.count(oid) == 0);
  std::shared_ptr<LockRequestQueue> table_lock_request_queue;
  if (!no_table_lock) {
    //  如果连表级队列都不存在，直接返回 false
    table_lock_request_queue = table_lock_map_[oid];
  }
  table_lock_map_latch_.unlock();
  if (no_table_lock) {
    return false;
  }
  // 2) 锁住队列，收集所有已“granted”（已授予）的表级锁模式
  std::scoped_lock lock(table_lock_request_queue->latch_);
  std::unordered_set<LockMode> found_lock_modes;

  for (const auto &request : table_lock_request_queue->request_queue_) {
    if (!request->granted_) {
      break;  // 队列后面的都是还没被授予的，不看了
    }
    found_lock_modes.insert(request->lock_mode_);
  }

  // 3) 根据要加的行锁模式，检查表级上是否已有“合适的”意向或共享／排它锁
  bool ensured = false;
  switch (row_lock_mode) {
    case LockMode::SHARED:
      // 如果要加行级 S，表级必须先有 IS/S/IX/SIX/X 之一
      ensured = found_lock_modes.count(LockMode::INTENTION_SHARED) > 0 ||
                found_lock_modes.count(LockMode::SHARED_INTENTION_EXCLUSIVE) > 0 ||
                found_lock_modes.count(LockMode::INTENTION_EXCLUSIVE) > 0 ||
                found_lock_modes.count(LockMode::SHARED) > 0 || found_lock_modes.count(LockMode::EXCLUSIVE) > 0;
      break;
    case LockMode::EXCLUSIVE:
      // 如果要加行级 X，表级必须先有 IX/SIX/X 之一
      ensured = found_lock_modes.count(LockMode::INTENTION_EXCLUSIVE) > 0 ||
                found_lock_modes.count(LockMode::SHARED_INTENTION_EXCLUSIVE) > 0 ||
                found_lock_modes.count(LockMode::EXCLUSIVE) > 0;
      break;
    default:
      BUSTUB_ASSERT(false, "EnsureProperTableLockForRow row_lock_mode should not be intention lock");
  }
  return ensured;
}

// 检查一个事务是否已经结束——要么正常提交（COMMITTED），要么被中止（ABORTED）
auto LockManager::IsTransactionEnded(bustub::Transaction *txn) -> bool {
  return txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED;
}

// 将指定事务强制中止，并通过抛出异常通知上层逻辑。
void LockManager::AbortTransaction(Transaction *txn, const AbortReason &abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  txn->UnlockTxn();
  TransactionAbortException exception(txn->GetTransactionId(), abort_reason);
  THREAD_DEBUG_LOG("Transaction %d aborted, exception: %s", txn->GetTransactionId(), exception.GetInfo().data());
  throw exception;
}

// 在给事务分配某个资源的锁时，真正执行“等待或授予”的逻辑，并决定是否要唤醒后续队列中的下一个事务。
auto LockManager::RequestLock(Transaction *txn, const LockManager::LockMode &lock_mode,
                              const std::shared_ptr<LockRequestQueue> &request_queue,
                              std::list<LockRequest *>::iterator request_iter,
                              std::unique_lock<std::mutex> &request_queue_lock) -> std::pair<bool, bool> {
  auto lock_request = *request_iter;
  // 1) 如果需要等待，先释放 txn 层锁，再在条件变量上阻塞
  if (IsLockModeCauseWait(*request_queue, lock_mode, request_iter)) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d waits. Queue: %s", DEBUG_THREAD_ID, txn->GetTransactionId(),
                     request_queue->ToString().c_str());
    txn->UnlockTxn();
    request_queue->cv_.wait(request_queue_lock, [&request_queue, txn]() {
      THREAD_DEBUG_LOG(
          "%s",
          fmt::format("txn {} is notified. Queue: {}\n", txn->GetTransactionId(), request_queue->ToString()).c_str());
      return request_queue->wake_id_ == txn->GetTransactionId();
    });
    THREAD_DEBUG_LOG("[thread T%ld] txn %d wakes up", DEBUG_THREAD_ID, txn->GetTransactionId());
    txn->LockTxn();
  }

  // 2) 如果队列中还有后续请求，并且下一个请求与当前请求兼容，准备唤醒它
  auto want_wake = false;
  if (request_queue->request_queue_.back() != lock_request) {
    if (IsLockModeCompatible((*std::next(request_iter))->lock_mode_, lock_request->lock_mode_) ||
        txn->GetState() == TransactionState::ABORTED) {
      // If the later transactions in the waiting list can be compatible with this, we grant.
      // Consider the case,for a resource, the request queue: X(granted), S(waiting), S(waiting).
      request_queue->wake_id_ = (*std::next(request_iter))->txn_id_;
      want_wake = true;
    }
  }
  
  // 3) 如果在等待过程中被中止，删除请求并返回失败
  if (txn->GetState() == TransactionState::ABORTED) {
    THREAD_DEBUG_LOG("[thread T%ld] txn %d was aborted.", DEBUG_THREAD_ID, txn->GetTransactionId());
    request_queue->request_queue_.erase(request_iter);
    return {false, want_wake};
  }
  // 4) 成功授予锁
  lock_request->granted_ = true;
  return {true, want_wake};
}

// No lock is acquired in this function directly.
auto LockManager::DepthFirstSearchCycle(txn_id_t source, txn_id_t &cycle_maker,
                                        const std::unordered_set<txn_id_t> &search_history) -> bool {
  if (waits_for_[source].empty()) {
    return false;
  }
  // 获取最大的事务id
  // 检查回环的逻辑
  if (search_history.count(source) != 0) {
    cycle_maker = *std::max_element(search_history.cbegin(), search_history.cend());
    return true;
  }
  // 递归遍历
  auto new_search_history = search_history;
  new_search_history.insert(source);
  return std::any_of(waits_for_[source].begin(), waits_for_[source].end(),
                     [&cycle_maker, &new_search_history, this](txn_id_t tail) {
                       return DepthFirstSearchCycle(tail, cycle_maker, new_search_history);
                     });
}

// No lock is locked in this function directly.
void LockManager::BuildWaitForGraph() {
  // 遍历所有表级锁队列，构建对应的等待边
  for (const auto &[_, request_queue] : table_lock_map_) {
    BuildWaitForGraphHelper(request_queue);
  }
  // 遍历所有行级锁队列，继续构建等待边
  for (const auto &[_, request_queue] : row_lock_map_) {
    BuildWaitForGraphHelper(request_queue);
  }
}

// Request queue latch is held in this function.
void LockManager::BuildWaitForGraphHelper(const std::shared_ptr<LockRequestQueue> &request_queue) {
  // 对单个队列加锁，防止并发修改
  std::scoped_lock queue_lock(request_queue->latch_);
  const auto &request_list = request_queue->request_queue_;
  // 第一个在等待的请求
  auto first_wait = std::find_if(request_list.cbegin(), request_list.cend(),
                                 [request_queue](const LockRequest *request) { return !request->granted_; });
  // 如果没有等待者，就跳过
  if (first_wait == request_list.cend()) {
    return;
  }
  // 对第一个等待者之前的所有已授予请求，都加一条边：source -> first_wait
  // 表示 first_wait 所属事务在等这些已拿到锁的事务释放。
  std::for_each(request_list.cbegin(), first_wait,
                [source = (*first_wait)->txn_id_, this, request_queue](LockRequest *request) {
                  auto dest = request->txn_id_;
                  // 只为未中止的事务添加依赖边
                  if (TransactionManager::GetTransaction(source)->GetState() != TransactionState::ABORTED &&
                      TransactionManager::GetTransaction(dest)->GetState() != TransactionState::ABORTED) {
                    AddEdge(source, dest);
                  }
                });
  // 记录这个等待队列，稍后用来唤醒相关事务
  waiting_transactions_[(*first_wait)->txn_id_].emplace(request_queue);
  // 再为同一个队列中后续的等待者，按链式也加依赖边
  // 比如 A(wait) B(wait) C(wait)：B -> A，C -> B
  for (auto waiting_request = std::next(first_wait); waiting_request != request_list.cend(); ++waiting_request) {
    auto source = (*waiting_request)->txn_id_;
    auto dest = (*std::prev(waiting_request))->txn_id_;
    waiting_transactions_[source].emplace(request_queue);
    if (TransactionManager::GetTransaction(source)->GetState() != TransactionState::ABORTED &&
        TransactionManager::GetTransaction(dest)->GetState() != TransactionState::ABORTED) {
      AddEdge(source, dest);
    }
  }
}

// 事务的状态转换成字符串
auto LockManager::TransactionStateToString(const TransactionState &state) const -> std::string_view {
  switch (state) {
    case TransactionState::GROWING:
      return {"GROWING"};
    case TransactionState::SHRINKING:
      return {"SHRINKING"};
    case TransactionState::COMMITTED:
      return {"COMMITTED"};
    case TransactionState::ABORTED:
      return {"ABORTED"};
  }
}

// 对象里状态格式化成一段可读的字符串
auto LockManager::LockRequestQueue::ToString() -> std::string {
  if (request_queue_.empty()) {
    return std::string{"empty lock request queue"};
  }
  std::stringstream stream;
  stream << fmt::format("lock request queue({},{},{},wake_id={}) - size:{}:", request_queue_.front()->oid_,
                        request_queue_.front()->rid_.GetPageId(), request_queue_.front()->rid_.GetSlotNum(), wake_id_,
                        request_queue_.size());
  for (const auto &request : request_queue_) {
    stream << fmt::format("(txn:{},mode:{},granted:{}) ", request->txn_id_,
                          LockManager::LockModeToString(request->lock_mode_), request->granted_ ? "true" : "false");
  }
  return stream.str();
}

}  // namespace bustub
