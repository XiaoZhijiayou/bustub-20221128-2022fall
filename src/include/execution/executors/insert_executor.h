//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  
   /**
    * 返回插入到表中的行数。
    * @param[out] tuple 一个整数元组，表示插入到表中的行数
    * @param[out] rid 下一个由插入产生的元组的 RID（忽略，不使用）
    * @return 如果产生了元组则返回 `true`，如果没有更多元组则返回 `false`
    *
    * 注意：InsertExecutor::Next() 不会使用 `rid` 输出参数。
    * 注意：InsertExecutor::Next() 只会一次性返回插入的行数，并返回 true。
    */

  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  bool done_;
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  TableHeap *table_;
  std::unique_ptr<Schema> schema_;
  std::unique_ptr<std::vector<IndexInfo *>> indexes_;
  std::vector<RID> locked_rids_;
};

}  // namespace bustub
