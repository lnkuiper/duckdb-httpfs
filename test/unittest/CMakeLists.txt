include_directories(${CMAKE_SOURCE_DIR}/test/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/catch)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${DUCKDB_MODULE_BASE_DIR}/third_party/httplib)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src/include)

add_executable(
  unittest_httpfs EXCLUDE_FROM_ALL
  main.cpp mock_s3_server.cpp httpfs_refresh_test.cpp s3_list_retry_test.cpp
  short_read_test.cpp)
set_target_properties(unittest_httpfs PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                 ${CMAKE_BINARY_DIR}/test)

target_link_libraries(unittest_httpfs test_helpers duckdb_static)
link_extension_libraries(unittest_httpfs "")
link_threads(unittest_httpfs "")
