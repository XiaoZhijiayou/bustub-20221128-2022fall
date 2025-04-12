//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// abstract_plan.h
//
// Identification: src/include/execution/plans/abstract_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "fmt/format.h"

namespace bustub {

#define BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(cname)                                                            \
  auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const -> std::unique_ptr<AbstractPlanNode> \
      override {                                                                                               \
    auto plan_node = cname(*this);                                                                             \
    plan_node.children_ = children;                                                                            \
    return std::make_unique<cname>(std::move(plan_node));                                                      \
  }

/** PlanType represents the types of plans that we have in our system. */
// 表示系统中支持的不同类型的计划节点
enum class PlanType {
  SeqScan,            // 顺序扫描
  IndexScan,          // 索引扫描
  Insert,             // 插入操作
  Update,             // 更新操作
  Delete,             // 删除操作
  Aggregation,        // 聚合操作
  Limit,              // 限制操作
  NestedLoopJoin,     // 嵌套循环连接
  NestedIndexJoin,    // 嵌套索引连接
  HashJoin,           // 哈希连接
  Filter,             // 过滤操作
  Values,             // 常量值操作
  Projection,         // 投影操作
  Sort,               // 排序操作
  TopN,               // 前N项操作
  MockScan            // 模拟扫描（测试或占位使用）
};


class AbstractPlanNode;
using AbstractPlanNodeRef = std::shared_ptr<const AbstractPlanNode>;

/**
 * AbstractPlanNode represents all the possible types of plan nodes in our system.
 * Plan nodes are modeled as trees, so each plan node can have a variable number of children.
 * Per the Volcano model, the plan node receives the tuples of its children.
 * The ordering of the children may matter.
 */
class AbstractPlanNode {
 public:
  /**
   * Create a new AbstractPlanNode with the specified output schema and children.
   * @param output_schema the schema for the output of this plan node
   * @param children the children of this plan node
   */
  AbstractPlanNode(SchemaRef output_schema, std::vector<AbstractPlanNodeRef> children)
      : output_schema_(std::move(output_schema)), children_(std::move(children)) {}

  /** Virtual destructor. */
  virtual ~AbstractPlanNode() = default;

  /** @return the schema for the output of this plan node */
  /** @return 返回该节点的输出模式，表示该节点的结果数据的结构 */
  auto OutputSchema() const -> const Schema & { return *output_schema_; }

  /** @return the child of this plan node at index child_idx */
  /** @return 返回该节点的第 child_idx 个子节点的引用 */
  auto GetChildAt(uint32_t child_idx) const -> AbstractPlanNodeRef { return children_[child_idx]; }

  /** @return the children of this plan node */
  /** @return 返回该节点的所有子节点 */
  auto GetChildren() const -> const std::vector<AbstractPlanNodeRef> & { return children_; }

  /** @return the type of this plan node */
  /** @return 返回该计划节点的类型 */
  virtual auto GetType() const -> PlanType = 0;

  /** @return the string representation of the plan node and its children */
  /** @return 将该计划节点及其子节点的字符串表示形式转换为字符串。with_schema 参数控制是否在输出中包含输出模式的描述。 */
  auto ToString(bool with_schema = true) const -> std::string {
    if (with_schema) {
      return fmt::format("{} | {}{}", PlanNodeToString(), output_schema_, ChildrenToString(2, with_schema));
    }
    return fmt::format("{}{}", PlanNodeToString(), ChildrenToString(2, with_schema));
  }

  /** @return the cloned plan node with new children */
  /** @return 克隆当前的计划节点，并将其子节点替换为 children */
  virtual auto CloneWithChildren(std::vector<AbstractPlanNodeRef> children) const
      -> std::unique_ptr<AbstractPlanNode> = 0;

  /**
   * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
   * and this tells you what schema this plan node's tuples will have.
   */
  // 指向一个包含列定义的 Schema 对象
  SchemaRef output_schema_;

  /** The children of this plan node. */
  // 表示该计划节点的子节点。每个计划节点可以有多个子节点，通常在执行查询时，父节点会接收子节点生成的元组。
  // children_ 是一个 AbstractPlanNodeRef 类型的向量，每个元素都是一个子节点的引用。
  std::vector<AbstractPlanNodeRef> children_;

 protected:
  /** @return the string representation of the plan node itself */
  /** @return 用于返回计划节点本身的字符串表示 */
  virtual auto PlanNodeToString() const -> std::string { return "<unknown>"; }

  /** @return the string representation of the plan node's children */
  /** @return 生成所有子节点的字符串表示 */
  auto ChildrenToString(int indent, bool with_schema = true) const -> std::string;

 private:
};

}  // namespace bustub

// 下面两个就是打印AbstractPlanNode的所有计划信息
template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const T &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x.ToString(), ctx);
  }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<bustub::AbstractPlanNode, T>::value, char>>
    : fmt::formatter<std::string> {
  template <typename FormatCtx>
  auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
    return fmt::formatter<std::string>::format(x->ToString(), ctx);
  }
};
