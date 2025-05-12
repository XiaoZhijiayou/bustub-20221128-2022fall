```cpp
enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
```
## 锁模式（Lock Mode）解释

在数据库管理系统中，**锁模式（Lock Mode）**用于控制并发访问的事务。下面是这五种锁模式的解释：

### 1. **SHARED 锁（共享锁）**
   - **定义**：多个事务可以同时持有共享锁。
   - **特点**：当一个事务持有共享锁时，其他事务可以读取该资源，但不能对该资源进行修改。
   - **用途**：适用于只读操作，确保多个事务能够同时读取数据，但无法修改数据。
   - **示例**：事务A对某行数据加共享锁，事务B也可以读取该数据，但无法修改。

### 2. **EXCLUSIVE 锁（排它锁）**
   - **定义**：一个事务持有排它锁时，其他事务既不能读取也不能修改该资源。
   - **特点**：排它锁是最强的锁模式，意味着资源被完全独占，其他事务无法访问。
   - **用途**：适用于需要修改数据的操作，确保没有其他事务能干扰该数据的修改。
   - **示例**：事务A对某行数据加排它锁，事务B无法读取或修改该数据。

### 3. **INTENTION_SHARED 锁（意向共享锁）**
   - **定义**：该锁表示事务希望对某资源获取共享锁，通常是对资源的父级或层次上的锁。
   - **特点**：意向共享锁本身不会阻止其他事务获取共享锁，它主要是为了表示未来可能会请求共享锁。
   - **用途**：通常用于层次结构（如树形结构）中，表明事务将来可能在某个较低层次上请求共享锁。
   - **示例**：事务A在一个表上加意向共享锁，表示它可能在表中的某些行上请求共享锁。

### 4. **INTENTION_EXCLUSIVE 锁（意向排它锁）**
   - **定义**：该锁表示事务希望对某资源获取排它锁，通常是对资源的父级或层次上的锁。
   - **特点**：意向排它锁本身不会阻止其他事务获取排它锁，它是为了表明未来可能会请求排它锁。
   - **用途**：与意向共享锁类似，通常用于层次结构中，表明事务将来可能在某个较低层次上请求排它锁。
   - **示例**：事务A在一个表上加意向排它锁，表示它可能在表中的某些行上请求排它锁。

### 5. **SHARED_INTENTION_EXCLUSIVE 锁（共享意向排它锁）**
   - **定义**：这是一种复合锁模式，表示事务希望在某个资源上加共享锁，并且可能在该资源的子资源上加排它锁。
   - **特点**：这种锁结合了共享锁和意向排它锁的特点。它允许多个事务共享访问该资源，但同时表明某些子资源可能会被单独锁定。
   - **用途**：适用于复杂的事务操作，需要共享资源的同时，也可能对某些子资源进行独占访问。
   - **示例**：事务A对某个表加共享意向排它锁，表示它可能对表中的某些行进行排它锁操作，同时允许其他事务共享访问表。

## 总结
- **共享锁（SHARED）** 允许多方读取但不允许修改。
- **排它锁（EXCLUSIVE）** 完全独占资源，阻止其他事务访问。
- **意向锁（INTENTION_SHARED 和 INTENTION_EXCLUSIVE）** 用于层次结构中，标明事务有意向在较低级别加锁，但不直接阻止其他操作。
- **共享意向排它锁（SHARED_INTENTION_EXCLUSIVE）** 是一种复合锁，允许共享访问同时标明某些子资源可能会被

#### 事务的状态
每个事务 (Transaction) 有四种可能的状态：
- GROWING
  - 事务处于“增长”阶段，仅能获取新锁，不能释放已持有的 S/X 锁。
- SHRINKING
  - 事务进入“收缩”阶段，只能释放已有的 S/X 锁，不再申请新的锁。
- COMMITTED
  - 事务已成功执行完毕，所有修改持久化；此后不再做任何锁操作。
- ABORTED
  - 事务因冲突或错误被回滚，所有未提交的修改被撤销；之后也不再申请或释放锁。

## Lock
### 1. 检查txn的状态
- 若 txn 处于 Abort/Commit 状态，抛逻辑异常，不应该有这种情况出现。
- 若 txn 处于 Shrinking 状态，则需要检查 txn 的隔离级别和当前锁请求类型：
```
REPEATABLE_READ:
   The transaction is required to take all locks.
   All locks are allowed in the GROWING state
   No locks are allowed in the SHRINKING state

READ_COMMITTED:
   The transaction is required to take all locks.
   All locks are allowed in the GROWING state
   Only IS, S locks are allowed in the SHRINKING state

READ_UNCOMMITTED:
   The transaction is required to take only IX, X locks.
   X, IX locks are allowed in the GROWING state.
   S, IS, SIX locks are never allowed
```
- 

##### 当事务处于Shrinking状态下
- 在 REPEATABLE_READ 下，造成事务终止，并抛出 LOCK_ON_SHRINKING 异常。
- 在 READ_COMMITTED 下，若为 IS/S 锁，则正常通过，否则抛 LOCK_ON_SHRINKING。
- 在 READ_UNCOMMITTED 下，若为 IX/X 锁，抛 LOCK_ON_SHRINKING，否则抛 LOCK_SHARED_ON_READ_UNCOMMITTED。
##### 当事务处于Growing状态下
- 若 txn 处于 Growing 状态，若隔离级别为 READ_UNCOMMITTED 且锁类型为 S/IS/SIX，抛 LOCK_SHARED_ON_READ_UNCOMMITTED。其余状态正常通过。

- 第一步保证了锁请求、事务状态、事务隔离级别的兼容。正常通过第一步后，可以开始尝试获取锁。

### 2. 获取table对应的lock request queue。
- 从 table_lock_map_ 中获取 table 对应的 lock request queue。注意需要对 map 加锁，并且为了提高并发性，在获取到 queue 之后立即释放 map 的锁。若 queue 不存在则创建。
### 3. 检查锁请求是否为第一次的锁升级
- 1. 首先，记得对 queue 加锁。
- 2. granted 和 waiting 的锁请求均放在同一个队列里，我们需要遍历队列查看有没有与当前事务 id（我习惯叫做 tid）相同的请求。如果存在这样的请求，则代表当前事务在此前已经得到了在此资源上的一把锁，接下来可能需要锁升级。需要注意的是，这个请求的 granted_ 一定为 true。因为假如事务此前的请求还没有被通过，事务会被阻塞在 LockManager 中，不可能再去尝试获取另一把锁。
- 3. 现在我们找到了此前已经获取的锁，开始尝试锁升级。首先，判断此前授予锁类型是否与当前请求锁类型相同。若相同，则代表是一次重复的请求，直接返回。否则进行下一步检查。
- 4. 接下来，判断当前资源上是否有另一个事务正在尝试升级（queue->upgrading_ == INVALID_TXN_ID）。若有，则终止当前事务，抛出 UPGRADE_CONFLICT 异常。因为不允许多个事务在同一资源上同时尝试锁升级。

#### 锁的升级
- 1. 可以升级吗？即我们此前的一系列判断。
- 2. 释放当前已经持有的锁，并在 queue 中标记我正在尝试升级。
- 3. 等待直到新锁被授予。
- 需要特别注意的是，在锁升级时，需要先释放此前持有的锁，把升级作为一个新的请求加入队列。之前我以为在锁升级时要一直持有此前的锁，直到能够升级时，再直接修改锁的类型。按此实现之后被一个 test case 卡到怀疑人生。
- 锁升级的步骤大概就是这样。当然，假如遍历队列后发现不存在与当前 tid 相同的请求，就代表这是一次平凡的锁请求。

### 4. 将锁请求加入请求队列
- new 一个 LockRequest，加入队列尾部。
  
### 5. 尝试获取锁
- 这是最后一步，也是最核心的一步，体现了 Lock Manager 的执行模型。首先，需要清楚条件变量的使用场景。
- 条件变量并不是某一个特定语言中的概念，而是操作系统中线程同步的一种机制。先给出条件变量经典的使用形式：
```cpp
std::unique_lock<std::mutex> lock(latch);
while (!resource) {
    cv.wait(lock);
}
```
- 条件变量与互斥锁配合使用。首先需要持有锁，并查看是否能够获取资源。这个锁与资源绑定，是用来保护资源的锁。若暂时无法获取资源，则调用条件变量的 wait 函数。调用 wait 函数后，latch 将自动释放，并且当前线程被挂起，以节省资源。这就是阻塞的过程。此外，允许有多个线程在 wait 同一个 latch。
- 当其他线程的活动使得资源状态发生改变时，需要调用条件遍历的 notify_all() 函数。即
```cpp
// do something changing the state of resource...
cv.notify_all();
```
- notify_all() 可以看作一次广播，会唤醒所有正在此条件变量上阻塞的线程。在线程被唤醒后，其仍处于 wait 函数中。在 wait 函数中尝试获取 latch。在成功获取 latch 后，退出 wait 函数，进入循环的判断条件，检查是否能获取资源。若仍不能获取资源，就继续进入 wait 阻塞，释放锁，挂起线程。若能获取资源，则退出循环。这样就实现了阻塞等待资源的模型。条件变量中的条件指的就是满足某个条件，在这里即能够获取资源。
- 理解条件变量的作用后，就可以写出如下代码：
```cpp
std::unique_lock<std::mutex> lock(queue->latch_);
while (!GrantLock(...)) {
    queue->cv_.wait(lock);
}
```