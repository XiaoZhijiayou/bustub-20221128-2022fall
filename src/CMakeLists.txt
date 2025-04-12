add_subdirectory(binder)
add_subdirectory(buffer)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(concurrency)
add_subdirectory(container)
add_subdirectory(execution)
add_subdirectory(recovery)
add_subdirectory(storage)
add_subdirectory(type)
add_subdirectory(planner)
add_subdirectory(primer)
add_subdirectory(optimizer)

add_library(bustub STATIC ${ALL_OBJECT_FILES})

## 设置了一个变量 BUSTUB_LIBS，它包含了项目中各个模块和库的目标，这些库目标会被链接到 bustub 静态库中。
set(BUSTUB_LIBS
        bustub_binder
        bustub_buffer
        bustub_catalog
        bustub_common
        bustub_concurrency
        bustub_execution
        bustub_recovery
        bustub_type
        bustub_container_hash
        bustub_container_disk_hash
        bustub_storage_disk
        bustub_storage_index
        bustub_storage_page
        bustub_storage_table
        bustub_planner
        bustub_primer
        bustub_optimizer
        )

find_package(Threads REQUIRED)

# 设置了一个包含第三方库的变量 BUSTUB_THIRDPARTY_LIBS，这些库将被链接到 bustub 库中
set(BUSTUB_THIRDPARTY_LIBS
        bustub_murmur3
        duckdb_pg_query
        fmt
        libfort::fort
        Threads::Threads
        )

# 将 BUSTUB_LIBS 和 BUSTUB_THIRDPARTY_LIBS 链接到 bustub 库中
target_link_libraries(
        bustub
        ${BUSTUB_LIBS}
        ${BUSTUB_THIRDPARTY_LIBS})

target_include_directories(
        bustub PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
