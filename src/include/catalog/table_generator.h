#pragma once

#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/executor_context.h"
#include "storage/table/table_heap.h"

namespace bustub {

static constexpr uint32_t TEST1_SIZE = 1000;
static constexpr uint32_t TEST2_SIZE = 100;
static constexpr uint32_t TEST3_SIZE = 100;
static constexpr uint32_t TEST4_SIZE = 100;
static constexpr uint32_t TEST6_SIZE = 100;
static constexpr uint32_t TEST7_SIZE = 100;
static constexpr uint32_t TEST8_SIZE = 10;
static constexpr uint32_t TEST9_SIZE = 10;
static constexpr uint32_t TEST_VARLEN_SIZE = 10;

// 用于生成测试表格的类
class TableGenerator {
 public:
  /**
   * Constructor
   */
  explicit TableGenerator(ExecutorContext *exec_ctx) : exec_ctx_{exec_ctx} {}

  /**
   * Generate test tables. 生成所有测试表
   */
  void GenerateTestTables();

 private:
  /** Enumeration to characterize the distribution of values in a given column */
  enum class Dist : uint8_t { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial, Cyclic };

  /**
   * Metadata about the data for a given column. Specifically, the type of the
   * column, the distribution of values, a min and max if appropriate.
   * 描述单个列在生成数据时的元信息
   */
  struct ColumnInsertMeta {
    /**
     * Name of the column   列名
     */
    const char *name_;
    /**
     * Type of the column   列的数据类型（例如 INTEGER、VARCHAR 等）
     */
    const TypeId type_;
    /**
     * Whether the column is nullable   是否允许为空
     */
    bool nullable_;
    /**
     * Distribution of values   该列数据的分布类型
     */
    Dist dist_;
    /**
     * Min value of the column   该列数据的最小值
     */
    uint64_t min_;
    /**
     * Max value of the column    该列数据的最大值
     */
    uint64_t max_;
    /**
     * Counter to generate serial data    用于生成顺序数据的计数器
     */
    uint64_t serial_counter_{0};

    /**
     * Constructor
     */
    ColumnInsertMeta(const char *name, const TypeId type, bool nullable, Dist dist, uint64_t min, uint64_t max)
        : name_(name), type_(type), nullable_(nullable), dist_(dist), min_(min), max_(max) {}
  };

  /**
   * Metadata about a table. Specifically, the schema and number of
   * rows in the table.  描述单个测试表的元信息
   */
  struct TableInsertMeta {
    /**
     * Name of the table    表名
     */
    const char *name_;
    /**
     * Number of rows     表中的记录的数量
     */
    uint32_t num_rows_;
    /**
     * Columns    包含所有列的元数据
     */
    std::vector<ColumnInsertMeta> col_meta_;

    /**
     * Constructor
     */
    TableInsertMeta(const char *name, uint32_t num_rows, std::vector<ColumnInsertMeta> col_meta)
        : name_(name), num_rows_(num_rows), col_meta_(std::move(col_meta)) {}
  };

  // 根据提供的表信息（TableInfo）和对应的表的元数据（TableInsertMeta），实际向表中填充数据。
  void FillTable(TableInfo *info, TableInsertMeta *table_meta);
  
  // 根据指定列的元数据信息生成一个长度为 count 的数据向量（std::vector<Value>），这些数据将作为列的值插入到表中。
  auto MakeValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value>;

  // 用于生成数值型数据时的模板函数，根据具体的 C++ 类型生成数字值。
  template <typename CppType>
  auto GenNumericValues(ColumnInsertMeta *col_meta, uint32_t count) -> std::vector<Value>;

 private:
  ExecutorContext *exec_ctx_; // 执行器上下文（ExecutorContext）的指针，包含了查询执行过程中所需要的所有上下文信息
};
}  // namespace bustub
