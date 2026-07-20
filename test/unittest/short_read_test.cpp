#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

static void LoadHTTPFSExtension(DuckDB &db) {
	if (db.ExtensionIsLoaded("httpfs")) {
		return;
	}
	ExtensionInfo extension_info;
	ExtensionActiveLoad load_info(*db.instance, extension_info, "httpfs");
	ExtensionLoader loader(load_info);
	HttpfsExtension extension;
	extension.Load(loader);
}

static void RequireQueryOk(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(result);
	INFO((result->HasError() ? result->GetError() : string()));
	REQUIRE_FALSE(result->HasError());
}

static idx_t CountRangeGets(const vector<MockS3RequestObservation> &observations, const string &range) {
	idx_t count = 0;
	for (auto &observation : observations) {
		if (observation.method == "GET" && observation.status == 206 && observation.range == range) {
			count++;
		}
	}
	return count;
}

static void ConfigureShortReadTest(DuckDB &db, Connection &con, idx_t retries,
                                   const string &client_implementation = "curl") {
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, "SET http_retries=" + to_string(retries));
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
}

static string ReadRange(Connection &con, const string &path, idx_t offset, idx_t length) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	string buffer(length, '?');
	handle->Read(QueryContext(*con.context), &buffer[0], length, offset);
	return buffer;
}

} // namespace

TEST_CASE("HTTP range reads reject truncated response bodies", "[httpfs][short-read]") {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	SECTION("persistent short reads fail after the retry budget is exhausted") {
		static constexpr idx_t RETRIES = 3;
		MockS3ServerConfig config;
		config.object_data = string(8192, 'x');
		config.truncated_range_failures = RETRIES + 1;
		config.truncated_range_bytes = 17;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, RETRIES);

		RequireQueryOk(con, "BEGIN TRANSACTION");
		bool read_failed = false;
		string read_error;
		try {
			ReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
		} catch (std::exception &ex) {
			read_failed = true;
			read_error = ex.what();
		} catch (...) {
			read_failed = true;
			read_error = "unknown exception";
		}
		RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		INFO(read_error);
		REQUIRE(read_failed);
		REQUIRE(CountRangeGets(observations, expected_range) == RETRIES + 1);
	}

	SECTION("a transient short read is retried and replaced by the complete response") {
		MockS3ServerConfig config;
		config.object_data = string(8192, 'x');
		config.truncated_range_failures = 1;
		config.truncated_range_bytes = 17;
		auto expected = config.object_data.substr(READ_OFFSET, READ_LENGTH);
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, 1);

		RequireQueryOk(con, "BEGIN TRANSACTION");
		REQUIRE(ReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH) == expected);
		RequireQueryOk(con, "COMMIT");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		REQUIRE(CountRangeGets(observations, expected_range) == 2);
	}

	SECTION("a cleanly terminated response must still contain every requested byte") {
		MockS3ServerConfig config;
		config.object_data = string(8192, 'x');
		config.successful_short_range_responses = 1;
		config.truncated_range_bytes = 17;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, 0);

		RequireQueryOk(con, "BEGIN TRANSACTION");
		bool read_failed = false;
		string read_error;
		try {
			ReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
		} catch (std::exception &ex) {
			read_failed = true;
			read_error = ex.what();
		} catch (...) {
			read_failed = true;
			read_error = "unknown exception";
		}
		RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		INFO(read_error);
		REQUIRE(read_failed);
		REQUIRE(read_error.find("Short read for HTTP GET") != string::npos);
		REQUIRE(CountRangeGets(observations, expected_range) == 1);
	}
}

} // namespace duckdb
