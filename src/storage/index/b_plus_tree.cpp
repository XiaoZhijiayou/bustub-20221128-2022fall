
#include <algorithm>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

// Uncomment the following define statement will show the log, otherwise, not.
// #define HOO_ALLOW_DEBUG_LOG

#ifdef HOO_ALLOW_DEBUG_LOG
static std::mutex debug_log_mutex;
#define DEBUG_THREAD_ID (pthread_self() % 1000)
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
/*Use to see the benchmark statics*/

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      root_page_id_page_(new Page) {
  THREAD_DEBUG_LOG("Tree scale: leaf_max_size=%d,internal_max_size=%d", leaf_max_size, internal_max_size);
}

/*
 * Helper function to decide whether current b+tree is empty
 *
 * [IMPLEMENTATION] Status: DONE
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 *
 * [IMPLEMENTATION] Status: DONE | Note: transaction is ignored in checkpoint 1.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  THREAD_DEBUG_LOG("Enter | Parameter: key=%s", KeyToString(key).c_str());
  bool optimistic_success;
  const auto leaf = OptimisticSearch(key, SearchMode::Find, transaction, optimistic_success);
  if (leaf == nullptr) {
    return false;
  }
  const auto size = leaf->GetSize();
  auto i = 0;
  for (i = 0; i < size; ++i) {
    const auto value = leaf->KeyAt(i);
    if (comparator_(value, key) == 0) {
      result->emplace_back(leaf->ValueAt(i));
      break;
    }
  }
  // 解除对叶子节点页面的锁定，以便其他线程可以访问该页面
  DisusePage(ToRawPage(leaf), UseMode::Read);
  return (i != size);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
/*
 * 将一个常量键值对插入到 B+ 树中。
 * 如果当前树为空，则新建树，更新根页面 ID 并插入该条目；否则直接插入到叶子页面中。
 * @return: 由于只支持唯一键，如果用户尝试插入重复的键，则返回 false；否则返回 true。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  THREAD_DEBUG_LOG("(thread %ld) Enter | Parameters: key=%s,value=%s", DEBUG_THREAD_ID, KeyToString(key).c_str(),
                   ValueToString(value).c_str());
  // 创建一个用于管理页面的智能指针，当超出作用域时自动释放
  auto latched_pages = std::unique_ptr<LatchedPageContainer, std::function<void(LatchedPageContainer *)>>(
      new LatchedPageContainer, [this](LatchedPageContainer *object) {
        for (auto &page : *object) {
          DisusePage(page, UseMode::Write);
        }
        delete object;
      });
  bool optimistic_success;
  LeafPage *leaf = OptimisticSearch(key, SearchMode::Insert, transaction, optimistic_success);
  // 乐观搜索失败，解除页面的写锁并unpin，然后使用悲观搜索获取叶子节点
  if (!optimistic_success) {
    if (leaf != nullptr) {
      ToRawPage(leaf)->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    // 使用悲观搜索获取叶子节点，使用悲观搜索时会加锁，因此需要传入latched_pages管理页面的list
    leaf = PessimisticSearch(key, SearchMode::Insert, transaction, latched_pages.get());
  } else if (leaf != nullptr) {
    // 乐观搜索成功，直接使用叶子节点
    // 对其进行加锁，并将其添加到latched_pages中
    latched_pages->push_back(ToRawPage(leaf));
    if (leaf->Contain(key, comparator_)) {
      return false;
    }
    // 如果叶子节点已经存在且没有对应的key值，则直接插入键值对
    leaf->Insert(key, value, comparator_);
    return true;
  }
  // 这是处理叶子节点为空的情况
  // 这里对应的是乐观搜索和悲观搜索都失败的情况
  // 如果叶子节点为空，则创建一个新的叶子节点，并将其插入到B+树中
  if (leaf == nullptr) {
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    leaf = ToLeaf(ToTreePage(new_page));
    leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf->SetPageType(IndexPageType::LEAF_PAGE);
    leaf->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    root_page_id_ = new_page_id;
    UpdateRootPageId(new_page_id);
    return true;
  }
  // 下面是叶子节点不为空的情况
  // 检查叶子节点是否包含key值，如果包含则返回false，否则插入键值对
  if (leaf->Contain(key, comparator_)) {
    return false;
  }
  // 判断是否需要分裂叶子节点
  if ((leaf->GetSize() + 1) == leaf->GetMaxSize()) {
    BUSTUB_ASSERT(optimistic_success == false, "unreasonable case");
    THREAD_DEBUG_LOG("(thread %ld) Enter case 3", DEBUG_THREAD_ID);
    leaf->Insert(key, value, comparator_);
    auto last_half = leaf->ExtractHalf();
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_leaf = reinterpret_cast<LeafPage *>(new_page);
    new_leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf->SetPageType(IndexPageType::LEAF_PAGE);
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    new_leaf->EmplaceBack(last_half);
    leaf->SetNextPageId(new_leaf->GetPageId());
    InsertInParent(leaf, last_half.front().first, new_leaf, latched_pages.get(), transaction);
  } else {
    // 如果不需要分裂，则直接插入键值对
    leaf->Insert(key, value, comparator_);
  }
  THREAD_DEBUG_LOG("(thread %ld) Return case 3", DEBUG_THREAD_ID);
  return true;
}
// 在B+树进行节点分裂的时候，将新生成的节点信息插入到父节点中
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(LeafPage *node/*发生分裂的原始叶子节点*/, const KeyType &key/*分裂后再父节点中插入的分界值*/, BPlusTreePage *other_node /*分裂操作中新生成的叶子节点*/,
                                    LatchedPageContainer *latched_pages/*记录本次操作所有被加锁的页面*/, Transaction *transaction/*当前事务的上下文*/) {
  // 获取other_node的page_id
  auto value = other_node->GetPageId();
  // 如果node是当前树的根节点，那么此时分裂会导致树的高度增加
  if (node->GetPageId() == GetRootPageId()) {
    // 这一部分相当于创建一个新的根节点，让这个新节点链接上node和other_node
    page_id_t new_root_id;
    // 分配新的页面，并将返回的页面转换为内部节点，这个页面将作为新的根节点
    auto new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_root_id));
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    // 将key对应的左孩子部分设置为node，右孩子部分设为other_node
    new_root->Put(node->GetPageId(), key, value);
    // 将新建的根节点的类型设置为内部节点
    new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
    // 将node和other_node的父节点设置为新建的根节点
    node->SetParentPageId(new_root_id);
    other_node->SetParentPageId(new_root_id);
    // 更新根节点的ID
    root_page_id_ = new_root_id;
    // 更新根节点的ID到HeaderPage中
    UpdateRootPageId(new_root_id);
    // 将新根节点与新生成的页面根据需要标记为脏页，解除页面锁定并减少pin的计数
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    buffer_pool_manager_->UnpinPage(value, true);
    return;
  }
  // 非根节点的情况下，找到父节点
  auto parent = ToInternal(ToTreePage(*std::find_if(latched_pages->begin(), latched_pages->end(), [node](Page *page) {
    return page->GetPageId() == node->GetParentPageId();
  })));
  // 如果父节点满了，需要进行分裂，创建新内部节点 new_internal，将 pairs 分成两半，左半部分保留在 parent，右半部分放入 new_internal。
  if (parent->GetSize() == parent->GetMaxSize()) {
    // 分配新的页面作为新内部节点，将返回的页面转化为InternalPage*，并将新页面的ID保存在 new_internal_id 中
    page_id_t new_internal_id;
    auto new_internal = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_internal_id));
    auto pairs = parent->ExtractAll();
    // 将parent的所有pair都拿出来，然后再把要插入的key和value插入到pairs中
    pairs.insert(std::find_if(pairs.cbegin(), pairs.cend(),
                              [val = node->GetPageId()](const auto &pair) { return pair.second == val; }) +
                     1,
                 std::make_pair(key, value));
    // 左半部分保留在 parent
    // 右半部分放入 new_internal
    // 通常使用父节点的最小容量作为划分界限
    // 取出来右侧部分第一个键，这将作为新分界传递给上层父节点
    auto right_first_index = parent->GetMinSize();
    auto right_first_key = pairs[right_first_index].first;
    std::vector<std::pair<KeyType, page_id_t>> left_pairs{pairs.cbegin(), pairs.cbegin() + right_first_index};
    std::vector<std::pair<KeyType, page_id_t>> right_pairs{pairs.cbegin() + right_first_index, pairs.cend()};
    parent->EmplaceBack(left_pairs);
    // 插入右边的部分
    new_internal->Init(new_internal_id, INVALID_PAGE_ID, internal_max_size_);
    new_internal->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_internal->EmplaceBack(right_pairs);
    // 如果key小于right_first_key，则将value插入到parent中
    if (comparator_(key, right_first_key) < 0) {
      NodeChangeParent(value, parent->GetPageId(), latched_pages);
    }
    // 将right_pairs中的所有page_id插入到new_internal中
    for (const auto &[_, page_id] : right_pairs) {
      // 遍历右半部分的所有键值对，将每个子节点的父指针更新为新内部节点的页面ID
      NodeChangeParent(page_id, new_internal->GetPageId(), latched_pages);
    }
    // 释放value
    buffer_pool_manager_->UnpinPage(value, true);
    // 递归插入
    InsertInParent(reinterpret_cast<LeafPage *>(parent), right_first_key, new_internal, latched_pages, transaction);
  } else {
    // 如果父节点没有满，则直接插入
    other_node->SetParentPageId(parent->GetPageId());
    parent->InsertAfter(node->GetPageId(), key, value);
    // 释放value
    buffer_pool_manager_->UnpinPage(value, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
/*
 * 删除与输入键关联的键值对
 * 如果当前树为空，立即返回。
 * 如果树不为空，用户首先需要找到正确的叶子页面作为删除目标，然后
 * 从叶子页面中删除条目。记得在必要时处理重新分配或合并操作。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) Enter | Parameters: key=%s", DEBUG_THREAD_ID, KeyToString(key).c_str());
  // 用于存储删除操作期间加锁的页面集合
  auto latched_pages = std::unique_ptr<LatchedPageContainer, std::function<void(LatchedPageContainer *)>>(
      new LatchedPageContainer, [this](LatchedPageContainer *object) {
        for (auto &page : *object) {
          DisusePage(page, UseMode::Write);
        }
        delete object;
      });
  // 进行乐观搜索，快速尝试找到目标键所在的叶子节点。
  bool optimistic_success;
  LeafPage *leaf = OptimisticSearch(key, SearchMode::Delete, transaction, optimistic_success);
  if (!optimistic_success) {
    // 乐观搜索失败了，执行悲观搜索，执行之前先释放叶子节点的锁，并将其从缓冲池中释放
    if (leaf != nullptr) {
      ToRawPage(leaf)->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    leaf = PessimisticSearch(key, SearchMode::Delete, transaction, latched_pages.get());
  } else if (leaf != nullptr) {
    // 如果乐观搜索成功，将叶子节点加锁并加入到latched_pages中
    latched_pages->push_back(ToRawPage(leaf));
    // 删除目标键值对
    leaf->Remove(key, comparator_);
    return;
  }
  if (leaf == nullptr) {
    // 如果叶子节点为空，表示在树中没有找到该键，因此不进行任何删除操作。
    return;
  }
  // 删除键值对并处理合并或重新分配
  RemoveEntry(leaf, key, latched_pages.get(), transaction);
  THREAD_DEBUG_LOG("(thread %ld) Return", DEBUG_THREAD_ID);
}

// 删除叶子节点中的key，其实就是用于应对删除操作后，并在必要的时候进行节点的合并或重新分配操作
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *node, const KeyType &key, LatchedPageContainer *latched_pages,
                                 Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) Enter| Parameters: node_page_id=%d, key=%s", DEBUG_THREAD_ID, node->GetPageId(),
                   KeyToString(key).c_str());
  // 如果node是叶子节点，则调用叶子节点的Remove方法，否则调用内部节点的Remove方法
  node->IsLeafPage() ? ToLeaf(node)->Remove(key, comparator_) : ToInternal(node)->Remove(key, comparator_);
  // N is the root and N has only one remaining child
  // 如果是根节点
  if (node->IsRootPage()) {
    // 如果根节点只有一个子节点，则将子节点设置为新的根节点
    if (node->GetSize() == 1 && !node->IsLeafPage()) {
      THREAD_DEBUG_LOG("(thread %ld) execution branch 1", DEBUG_THREAD_ID);
      BPlusTreePage *child_node;
      // 查找根节点唯一的子节点.如果子节点没有被加锁(不在latched_pages中)，则加锁该子节点并加入到latched_pages中
      auto found_iter = std::find_if(latched_pages->begin(), latched_pages->end(),
                                     [want_page_id = ToInternal(node)->ValueAt(0)](Page *page) {
                                       THREAD_DEBUG_LOG("(thread %ld) want page id %d", DEBUG_THREAD_ID, want_page_id);
                                       return page->GetPageId() == want_page_id;
                                     });
      // 如果child_node不在latched_pages中，则将child_node加入到latched_pages中
      if (found_iter == latched_pages->end()) {
        child_node = ToTreePage(UsePage(ToInternal(node)->ValueAt(0), UseMode::Write, transaction));
        latched_pages->push_back(ToRawPage(child_node));
      } else {
        child_node = ToTreePage(*found_iter);
      }
      // 将child_node设置为本B+树新的根节点
      // 设置child_node的父节点为INVALID_PAGE_ID
      child_node->SetParentPageId(INVALID_PAGE_ID);
      // 将child_node设置为新的根节点
      root_page_id_ = child_node->GetPageId();
      // 更新根节点
      UpdateRootPageId(child_node->GetPageId());
      // 断言child_node在latched_pages中
      BUSTUB_ASSERT(std::find_if(latched_pages->begin(), latched_pages->end(),
                                 [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }) !=
                        latched_pages->end(),
                    "unexpected case");
      // 从latched_pages中删除node，并删除node
      latched_pages->erase(
          std::remove_if(latched_pages->begin(), latched_pages->end(),
                         [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
      // 从缓存池中删除当前根节点
      DeletePage(ToRawPage(node), UseMode::Write, transaction);
    } else if (node->GetSize() == 0) {
      // 如果node没有key，则删除node
      latched_pages->erase(
          std::remove_if(latched_pages->begin(), latched_pages->end(),
                         [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
      DeletePage(ToRawPage(node), UseMode::Write, transaction);
      root_page_id_ = INVALID_PAGE_ID;
    }
  } else if (node->GetSize() < node->GetMinSize()) {
    /**这种情况下是处理非根节点 */
    // 如果node的key小于min_size，则需要进行合并或重新分配，且node不是根节点
    THREAD_DEBUG_LOG("(thread %ld) execution branch 2", DEBUG_THREAD_ID);
    // 找到node的父节点
    auto parent_iter = std::find_if(
        latched_pages->begin(), latched_pages->end(),
        [want_page_id = node->GetParentPageId()](Page *page) { return page->GetPageId() == want_page_id; });
    // 断言parent_iter在latched_pages中
    BUSTUB_ASSERT(parent_iter != latched_pages->end(), "unexpected case");
    // 获取父节点
    auto parent_node = ToInternal(ToTreePage(*(parent_iter)));
    // same level, should not be in the latch_pages.
    // 获取node的相邻节点
    auto adjacent_node = ToTreePage(UsePage(parent_node->Adjacent(node->GetPageId()), UseMode::Write, transaction));
    // 将相邻节点加入到latched_pages中
    latched_pages->insert(
        std::find_if(latched_pages->begin(), latched_pages->end(),
                     [page_id = node->GetPageId()](Page *page) { return page->GetPageId() == page_id; }),
        ToRawPage(adjacent_node));
    // 获取node和相邻节点之间的key的index
    auto between_key_index = parent_node->BetweenKeyIndex(node->GetPageId(), adjacent_node->GetPageId());
    // 获取node和相邻节点之间的key
    auto between_key = parent_node->KeyAt(between_key_index);
    // 判断相邻节点是否是node的前驱节点
    auto adjacent_is_predecessor = parent_node->IsPredecessor(node->GetPageId(), adjacent_node->GetPageId());
    THREAD_DEBUG_LOG(
        "(thread %ld) adjacent(%s,%d) %s node(%s,%d)", DEBUG_THREAD_ID,
        KeyToString(
            (ToInternal(parent_node)->Get())[adjacent_is_predecessor ? between_key_index - 1 : between_key_index].first)
            .c_str(),
        adjacent_node->GetPageId(), adjacent_is_predecessor ? "->" : "<-",
        KeyToString(
            (ToInternal(parent_node)->Get())[adjacent_is_predecessor ? between_key_index : between_key_index - 1].first)
            .c_str(),
        node->GetPageId());

    // Coalesce: entries in N and N′ can fit in a single node
    // 计算单个节点的最大容量
    auto single_node_max = node->IsLeafPage() ? node->GetMaxSize() - 1 : node->GetMaxSize();
    // 如果node和相邻节点合并后不会超过单个节点的最大容量。就进行合并操作
    if ((adjacent_node->GetSize() + node->GetSize()) <= single_node_max) {
      // 根据前驱/后继关系选择合并方向
      if (adjacent_is_predecessor) {
        Coalesce(adjacent_node, node, between_key, latched_pages, transaction);
      } else {
        Coalesce(node, adjacent_node, between_key, latched_pages, transaction);
      }
      // 递归删除父节点中的分隔键
      RemoveEntry(parent_node, between_key, latched_pages, transaction);
    } else /* Redistribution: borrow an entry from N′ */ {// 如果两个节点合并后大于single_node_max，则进行重新分配
      if (adjacent_is_predecessor) {
        THREAD_DEBUG_LOG("(thread %ld) redistribute - page %d <- page %d", DEBUG_THREAD_ID, node->GetPageId(),
                         adjacent_node->GetPageId());
        // 从前驱节点借元素
        if (!node->IsLeafPage()) {
          // 内部节点的重分配，从前驱节点的尾部借出一个元素，然后将其插入到当前节点node的头部
          auto pair = ToInternal(adjacent_node)->PopBack();
          ToInternal(node)->Get()[0].first = between_key;
          ToInternal(node)->PushFront(pair.second);
          parent_node->SetKeyAt(between_key_index, pair.first);
          // 更新node的子节点的父节点指针
          NodeChangeParent(pair.second, node->GetPageId(), latched_pages);
        } else {
          // 叶子节点的重分配
          auto pair = ToLeaf(adjacent_node)->PopBack();
          ToLeaf(node)->Insert(pair.first, pair.second, comparator_);
          parent_node->SetKeyAt(between_key_index, pair.first);
        }
      } else /* Symmetric case*/ {
        // 这个就是相邻节点不是他的前驱节点，是他的后继节点
        // 从后继节点借元素
        if (!node->IsLeafPage()) {
          // 内部节点的重新分配
          auto pair = ToInternal(adjacent_node)->PopFront(); // 取出后继节点的第一个元素
          ToInternal(node)->PushBack(pair.first, pair.second);                   // 插入到当前节点末尾
          parent_node->SetKeyAt(between_key_index, ToInternal(adjacent_node)->Get()[0].first);  // 更新父节点分隔键
          NodeChangeParent(pair.second, node->GetPageId(), latched_pages);  // 更新子节点的父指针
        } else {
          // 如果node是叶子节点的话，并且相邻节点是他的后继节点，就从后继节点借元素
          // 叶子节点的重分配
          auto pair = ToLeaf(adjacent_node)->PopFront();  // 取出后继节点的第一个元素
          ToLeaf(node)->Insert(pair.first, pair.second, comparator_); // 插入到当前节点
          parent_node->SetKeyAt(between_key_index, ToLeaf(adjacent_node)->KeyAt(0));  // 更新父节点分隔键
        }
      }
    }
  }
}

//将 node 这个节点的所有元素合并(coalesce)到它的前驱节点 predecessor 中，然后删除 node 节点本身。
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(bustub::BPlusTreePage *predecessor, bustub::BPlusTreePage *node,
                              const KeyType &between_key, LatchedPageContainer *latched_pages,
                              Transaction *transaction) {
   // 输出调试日志，打印正在做的 coalesce 操作以及涉及的页号
  THREAD_DEBUG_LOG("(thread %ld) coalesce page %d to page %d", DEBUG_THREAD_ID, node->GetPageId(),
                   predecessor->GetPageId());
  // 1. 根据 node 是否是叶子节点， 做不同的处理
  if (!node->IsLeafPage()) {
    // ---- 情形 A：node 是内部节点(InternalPage) ----
    // (a) 从 node 中取出它的所有 (key, page_id) 对
    auto pairs = ToInternal(node)->ExtractAll();

    // pairs[0] 的 key 通常是无意义的“占位”，这里覆盖成 parent 里拿下来的 between_key
    // 这是因为在父节点处，between_key 是分隔 node 和 predecessor 的那把 key。
    // 将父节点中的分隔键 between_key 赋值给 pairs[0] 的 key，
    pairs[0].first = between_key;

    // (b) 把这些 pairs 整体追加到 predecessor（同为 InternalPage）里
    ToInternal(predecessor)->EmplaceBack(pairs);

    // (c) 因为这些 child 原来指向 node 这个内部节点，现在合并到 predecessor 后，
    for (const auto &[_, page_id] : pairs) {
      NodeChangeParent(page_id, predecessor->GetPageId(), latched_pages);
    }
  } else {
    // ---- 情形 B：node 是叶子节点(LeafPage) ----

    // (a) 从 node 中取出它所有 (key, value) 对
    auto pairs = ToLeaf(node)->ExtractAll();

    // (b) 把这些键值对追加到 predecessor（它也是一个叶子节点）里
    ToLeaf(predecessor)->EmplaceBack(pairs);

    // (c) 还需要把 predecessor 的 next 指针改成 node 的 next 指针，
    //     这样就把 node 自身从叶子链表中跳过了
    ToLeaf(predecessor)->SetNextPageId(ToLeaf(node)->GetNextPageId());
  }
  // 2. 从 latch 列表 latched_pages 中把 node 这个页面移除，
  //    因为它马上要被删除，不再需要保持锁。
  latched_pages->erase(
      std::remove_if(latched_pages->begin(), latched_pages->end(),
                     [page_id = node->GetPageId()](Page *page) { return page_id == page->GetPageId(); }));
  // 3. 最后真正删除 node 这个页面(从缓冲池和磁盘上都清掉)
  DeletePage(ToRawPage(node), UseMode::Write, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NodeChangeParent(page_id_t page_id, page_id_t parent_id, LatchedPageContainer *latched_pages) {
  auto found_iter = std::find_if(latched_pages->begin(), latched_pages->end(),
                                 [page_id](Page *page) { return page->GetPageId() == page_id; });
  if (found_iter != latched_pages->end()) {
    // 如果页面已经在 latched_pages 中，则直接更新父节点 ID
    ToTreePage(*found_iter)->SetParentPageId(parent_id);
  } else {
    // 如果页面没有加锁，从缓冲池中加载页面
    auto page = buffer_pool_manager_->FetchPage(page_id);
    page->WLatch();

    // 更新父节点 ID
    ToTreePage(page)->SetParentPageId(parent_id);
    page->WUnlatch();
    // 将页面从缓冲池中解除锁定，并标记为脏页
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
/**
  * 输入参数为空，首先找到最左边的叶子页面，然后构造索引迭代器
  * @return : 索引迭代器
  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // TODO(hoo): Implementation for concurrent correctness after finishing project 2.
  THREAD_DEBUG_LOG("Enters");
  if (GetRootPageId() == INVALID_PAGE_ID) {
    return End();
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  BUSTUB_ASSERT(page != nullptr, "unexpected case");
  while (!page->IsLeafPage()) {
    auto internal_page = static_cast<InternalPage *>(page);
    BUSTUB_ASSERT(internal_page->GetSize() != 0, "unexpected size");
    auto next_page_id = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    THREAD_DEBUG_LOG("next page id = %d, page_id=%d", next_page_id, page->GetPageId());
    BUSTUB_ASSERT(next_page_id == page->GetPageId(), "unexpected case");
  }
  auto itr_page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(itr_page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
/**
  * 输入参数是低键，首先找到包含输入键的叶子页面，然后构造索引迭代器
  * @return : 索引迭代器，就是具体页里面对应的key的迭代器 : INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
  *           - 返回迭代器包含的结构：
                        -   page_id_t page_id_;                             // 当前叶子页的页id
                        -   int index_;                                     // 当前叶子页的索引，用于定位具体的键值对       
                        -   BufferPoolManager *buffer_pool_manager_;        // 缓冲池管理器的指针，用于从内存中获取指定的页
                        -   MappingType pair_;                              // 当前页的键值对 
  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // TODO(hoo): Implementation for concurrent correctness after finishing project 2.
  THREAD_DEBUG_LOG("enters");
  bool success;
  auto leaf = OptimisticSearch(key, SearchMode::Find, nullptr, success);
  const auto size = leaf->GetSize();
  int i;
  for (i = 0; i < size; ++i) {
    if (comparator_(key, leaf->KeyAt(i)) == 0) {
      break;
    }
  }
  auto itr_page_id = (i == size) ? INVALID_PAGE_ID : leaf->GetPageId();
  auto itr_index = i;
  auto itr_buffer_pool_manager = (i == size) ? nullptr : buffer_pool_manager_;
  ToRawPage(leaf)->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  THREAD_DEBUG_LOG("return valid pointer ? %s", (i == size) ? "invalid" : "valid");
  return INDEXITERATOR_TYPE(itr_page_id, itr_index, itr_buffer_pool_manager);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
/**
  * 输入参数为空，构造一个表示叶子节点中键/值对结束位置的索引迭代器
  * @return : 索引迭代器
  */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*根据页面的id，从缓冲池中获取页面，并为该页面加锁*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UsePage(page_id_t page_id, UseMode mode, Transaction *transaction) -> Page * {
  THREAD_DEBUG_LOG("(thread %ld) use page %d status : begin", DEBUG_THREAD_ID, page_id);
  auto page = (page_id != INVALID_PAGE_ID) ? buffer_pool_manager_->FetchPage(page_id) : root_page_id_page_.get();
  // 根据不同的模式加锁
  switch (mode) {
    case UseMode::Read:
      page->RLatch();
      break;
    case UseMode::Write:
      page->WLatch();
      break;
  }
  // 如果事务存在，将页面加入事务的页面集合，用于后续的统一管理
  if (page_id != INVALID_PAGE_ID && transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  THREAD_DEBUG_LOG("(thread %ld) use page %d status : success", DEBUG_THREAD_ID, page_id);
  return page;
}

// 解除对一个页面的锁定并解pin页面
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DisusePage(Page *page, UseMode mode) {
  THREAD_DEBUG_LOG("(thread %ld) disuse page %d : enter", DEBUG_THREAD_ID, page->GetPageId());
  bool is_dirty;
  switch (mode) {
    case UseMode::Read:
      page->RUnlatch();
      is_dirty = false;
      break;
    case UseMode::Write:
      // 如果页面被写锁定，则设置is_dirty = true，表示页面被修改过。
      page->WUnlatch();
      is_dirty = true;
      break;
  }
  if (page->GetPageId() != INVALID_PAGE_ID) {
    // 解除对该页面的pin状态
    buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
  }
  THREAD_DEBUG_LOG("(thread %ld) disuse page %d : done", DEBUG_THREAD_ID, page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePage(Page *page, UseMode mode, bustub::Transaction *transaction) {
  THREAD_DEBUG_LOG("(thread %ld) delete page %d", DEBUG_THREAD_ID, page->GetPageId());
  const auto page_id = page->GetPageId();
  DisusePage(page, mode);
  if (transaction != nullptr) {
    // 事务添加到删除的页面集合中，用于后续的统一管理
    transaction->AddIntoDeletedPageSet(page_id);
  }
  // 缓存池删除页面
  buffer_pool_manager_->DeletePage(page_id);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
// 悲观搜索的实现
// 每次访问页面都会进行加锁，这种方式保证了页面在搜索过程中不会被其他事务修改，直到事务完成
 INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::PessimisticSearch(const KeyType &key, SearchMode mode, Transaction *transaction,
                                       LatchedPageContainer *latched_pages) -> LeafPage * {
  const auto use_mode = UseMode::Write;
   // This would add the page to latched_pages automatically if insert or delete.
  // 用来封装获取页面并加锁的操作。它会在搜索过程中将加锁的页面添加到 latched_pages 中，以确保操作完成后这些页面能够被正确释放。
  const auto smart_use = [this, transaction, latched_pages](page_id_t page_id) -> Page * {
    Page *page;
    page = UsePage(page_id, use_mode, transaction);
    latched_pages->push_back(page);
    return page;
  };
  // 根据mode（插入或删除）来定义一个安全条件。插入模式下要求当前页面的大小小于最大容量，删除模式下要求页面的大小大于最小容量，并且删除后要保证容量不会小于最小值和最大值之间的差距。
  const auto reset_latched_pages = [this, latched_pages](BPlusTreePage *tree_page) {
    auto num_remove_items = latched_pages->size() - 1;
    for (auto iter = latched_pages->begin(); num_remove_items != 0; --num_remove_items) {
      DisusePage(*iter, use_mode);
      ++iter;
      latched_pages->pop_front();
    }
  };
  const auto is_safe_predicate = (mode == SearchMode::Insert)
      ? [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
    return cur_size_for_insert < tree_page->GetMaxSize();
  }
  : [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
      return (cur_size_for_delete > tree_page->GetMinSize()) &&
             (cur_size_for_delete > ((tree_page->IsLeafPage() ? tree_page->GetMaxSize() - 1 : tree_page->GetMaxSize()) -
                                     tree_page->GetMinSize() + 1));
    };
  auto root_id_page = smart_use(INVALID_PAGE_ID);
  (void)(root_id_page);
  const bool has_root = (root_page_id_ != INVALID_PAGE_ID);
  if (!has_root) {
    return nullptr;
  }
  // 获取根节点并且加锁
  auto tree_page = ToTreePage(smart_use(root_page_id_));
  while (!tree_page->IsLeafPage()) {
    if (!tree_page->IsRootPage() && is_safe_predicate(tree_page, tree_page->GetSize(), tree_page->GetSize())) {
      reset_latched_pages(tree_page);
    }
    const auto size = ToInternal(tree_page)->GetSize();
    page_id_t next_page_id = ToInternal(tree_page)->ValueAt(0);
    for (int i = 0; i < size; ++i) {
      if ((i == (size - 1)) || (comparator_(ToInternal(tree_page)->KeyAt(i + 1), key) > 0)) {
        next_page_id = ToInternal(tree_page)->ValueAt(i);
        break;
      }
    }
    tree_page = ToTreePage(smart_use(next_page_id));
  }
  // 如果当前节点不是根节点且满足安全条件，则释放不再需要的页面
  if (!tree_page->IsRootPage() && is_safe_predicate(tree_page, tree_page->GetSize() + 1, tree_page->GetSize())) {
    reset_latched_pages(tree_page);
  }
  // 返回叶子节点
  return static_cast<LeafPage *>(tree_page);
}

/*即它假设在搜索过程中页面不会被其他事务修改，因此只在需要时才加锁。这种方式提高了并发性，但需要确保在访问时页面没有被其他事务修改。*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimisticSearch(const KeyType &key, SearchMode mode, bustub::Transaction *transaction,
                                      bool &success) -> LeafPage * {
  const auto use_mode = UseMode::Read;
  // 插入模式，确保插入操作不会导致节点溢出
  // 删除模式，确保删除后的当前大小不会小于最小值，额外安全条件：如果是叶子节点，删除后的当前大小不能小于最大值减去最小值加1
  const auto is_safe_predicate = (mode == SearchMode::Insert)
      ? [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
    return cur_size_for_insert < tree_page->GetMaxSize();
  }
  : [](BPlusTreePage *tree_page, int cur_size_for_insert, int cur_size_for_delete) -> bool {
      return (cur_size_for_delete > tree_page->GetMinSize()) &&
             (cur_size_for_delete > ((tree_page->IsLeafPage() ? tree_page->GetMaxSize() - 1 : tree_page->GetMaxSize()) -
                                     tree_page->GetMinSize() + 1));
    };
  // 根据节点的类型施加写锁或读锁
  // 叶子节点施加写锁，因叶子节点可能被修改（如插入/删除键值对）。
  // 内部节点施加读锁，假设遍历时不需要修改内部节点，只需读取其结构。
  // 事务记录：将锁定的页面（转换为 Page*）加入事务的 PageSet，确保事务结束时统一释放锁或处理状态。
  const auto smart_latch = [this, transaction](BPlusTreePage *tree_page) {
    tree_page->IsLeafPage() ? ToRawPage(tree_page)->WLatch() : ToRawPage(tree_page)->RLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(ToRawPage(tree_page));
    }
  };
  auto root_id_page = UsePage(INVALID_PAGE_ID, use_mode, transaction);
  auto root_id = root_page_id_;
  success = true;
  if (root_page_id_ == INVALID_PAGE_ID) {
    DisusePage(root_id_page, use_mode);
    success = false;
    return nullptr;
  }
  auto tree_page = ToTreePage(buffer_pool_manager_->FetchPage(root_id));
  smart_latch(tree_page);
  DisusePage(root_id_page, use_mode);
  while (!tree_page->IsLeafPage()) {
    const auto size = ToInternal(tree_page)->GetSize();
    page_id_t next_page_id = ToInternal(tree_page)->ValueAt(0);
    for (int i = 0; i < size; ++i) {
      if ((i == (size - 1)) || (comparator_(ToInternal(tree_page)->KeyAt(i + 1), key) > 0)) {
        next_page_id = ToInternal(tree_page)->ValueAt(i);
        break;
      }
    }
    auto next_page = ToTreePage(buffer_pool_manager_->FetchPage(next_page_id));
    smart_latch(next_page);
    ToRawPage(tree_page)->RUnlatch();
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);
    tree_page = next_page;
  }
  if (mode != SearchMode::Find && !is_safe_predicate(tree_page, tree_page->GetSize() + 1, tree_page->GetSize())) {
    success = false;
  }
  return static_cast<LeafPage *>(tree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
/*
 * 在头页中更新/插入根页面 ID（头页位于 `include/page/header_page.h` 中，page_id = 0）。
 * 每次根页面 ID 更改时都调用此方法。
 * @parameter: insert_record 默认值为 false。如果设置为 true，则在头页中插入一条记录 <index_name, root_page_id>，而不是更新它。
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  THREAD_DEBUG_LOG("(thread %ld) update root id to be %d", DEBUG_THREAD_ID, insert_record);
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  // 更新头部
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
 // 测试脚本用例
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
// 将B+树的结构以图形形式输出到文件中，以便可视化和调试。
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
 // 用于打印 B+ 树的详细信息（树的每个节点的信息），通常用于调试和查看树的具体内容。
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * 打印B+树的详细信息（每个节点的信息），通常用于调试和查看树的具体内容。
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    // 内部节点的打印逻辑
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

// 将 KeyType 类型的键（key）转换为字符串表示形式
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyToString(const KeyType &key) const -> std::string {
#ifdef HOO_ALLOW_DEBUG_LOG
  std::stringstream buf;
  buf << key;
  return buf.str();
#else
  return std::string{};
#endif
}

// 将 ValueType 类型的值（value）转换为字符串表示形式
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ValueToString(const ValueType &value) const -> std::string {
  std::stringstream buf;
  buf << value;
  auto res = buf.str();
  res.resize(res.size() - 1);
  return res;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
