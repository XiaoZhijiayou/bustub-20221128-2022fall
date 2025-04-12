//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 * 嵌套索引连接执行器，索引嵌套循环连接执行器
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
 void AddTupleValuesTo(std::vector<Value> &values, Tuple *tuple, const Schema &schema);
  /** The nested index join plan node. */
  /** 保存了连接操作的计划信息 */
  const NestedIndexJoinPlanNode *plan_;
  /** 代表外层的执行器，用于提供每个外层的Tuple */
  std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub
