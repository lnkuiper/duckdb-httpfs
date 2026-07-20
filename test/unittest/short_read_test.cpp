#include "catch.hpp"

#include "mock_s3_server.hpp"

#include "httpfs.hpp"
#include "httpfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "test_helpers.hpp"

#include <fstream>

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

static idx_t CountRequests(const vector<MockS3RequestObservation> &observations, const string &method, int status,
                           const string &range = string()) {
	idx_t count = 0;
	for (auto &observation : observations) {
		if (observation.method == method && observation.status == status && observation.range == range) {
			count++;
		}
	}
	return count;
}

static void ConfigureShortReadTest(DuckDB &db, Connection &con, idx_t retries,
                                   const string &client_implementation = "curl", bool connection_caching = false) {
	LoadHTTPFSExtension(db);
	RequireQueryOk(con, StringUtil::Format("SET httpfs_client_implementation='%s'", client_implementation));
	RequireQueryOk(con, StringUtil::Format("SET httpfs_connection_caching=%s", connection_caching ? "true" : "false"));
	RequireQueryOk(con, "SET http_retries=" + to_string(retries));
	RequireQueryOk(con, "SET http_retry_wait_ms=1");
	RequireQueryOk(con, "SET http_retry_backoff=1");
}

static string ReadRange(Connection &con, const string &path, idx_t offset, idx_t length) {
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	string buffer(length, '?');
	handle->Read(QueryContext(*con.context), &buffer[0], length, offset);
	return buffer;
}

struct ReadOutcome {
	bool failed = false;
	bool internal_error = false;
	string error;
};

static ReadOutcome TryReadRange(Connection &con, const string &path, idx_t offset, idx_t length) {
	ReadOutcome outcome;
	try {
		ReadRange(con, path, offset, length);
	} catch (std::exception &ex) {
		outcome.failed = true;
		outcome.error = ex.what();
		ErrorData error(ex);
		outcome.internal_error = error.Type() == ExceptionType::INTERNAL || error.Type() == ExceptionType::FATAL;
	} catch (...) {
		outcome.failed = true;
		outcome.error = "unknown exception";
	}
	return outcome;
}

static vector<int> RangeRequestPorts(const vector<MockS3RequestObservation> &observations, const string &range) {
	vector<int> result;
	for (auto &observation : observations) {
		if (observation.method == "GET" && observation.range == range) {
			result.push_back(observation.remote_port);
		}
	}
	return result;
}

static void RunPersistentShortRead(const string &client_implementation, bool connection_caching, idx_t retries) {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object_data = string(8192, 'x');
	config.range_behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range_behavior_requests = retries + 1;
	config.truncated_range_bytes = 17;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureShortReadTest(db, con, retries, client_implementation, connection_caching);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto outcome = TryReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
	RequireQueryOk(con, "ROLLBACK");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	INFO(outcome.error);
	REQUIRE(outcome.failed);
	REQUIRE_FALSE(outcome.internal_error);
	REQUIRE(CountRequests(observations, "GET", 206, expected_range) == retries + 1);

	if (connection_caching && retries > 0) {
		auto ports = RangeRequestPorts(observations, expected_range);
		REQUIRE(ports.size() == retries + 1);
		REQUIRE(ports.front() != ports.back());
	}
}

static void RunTransientShortRead(const string &client_implementation, bool connection_caching) {
	static constexpr idx_t READ_OFFSET = 113;
	static constexpr idx_t READ_LENGTH = 1024;
	const string expected_range = "bytes=113-1136";

	MockS3ServerConfig config;
	config.object_data = string(8192, 'x');
	config.range_behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
	config.range_behavior_requests = 1;
	config.truncated_range_bytes = 17;
	auto expected = config.object_data.substr(READ_OFFSET, READ_LENGTH);
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureShortReadTest(db, con, 1, client_implementation, connection_caching);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE(ReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH) == expected);
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "GET", 206, expected_range) == 2);

	if (connection_caching) {
		auto ports = RangeRequestPorts(observations, expected_range);
		REQUIRE(ports.size() == 2);
		REQUIRE(ports.front() != ports.back());
	}
}

class TestHTTPFileSystem : public HTTPFileSystem {
public:
	using HTTPFileSystem::RunGetRangeRequest;
	using HTTPFileSystem::TryRangeRequest;

	unique_ptr<HTTPResponse> GetRangeRequest(FileHandle &, string, HTTPHeaders, idx_t, char *, idx_t) override {
		auto response = make_uniq<HTTPResponse>(forced_status);
		response->success = forced_success;
		return response;
	}

	HTTPStatusCode forced_status = HTTPStatusCode::PartialContent_206;
	bool forced_success = false;
};

struct TestHTTPFileContext {
	TestHTTPFileContext() {
		auto params = make_uniq<HTTPFSParams>(http_util);
		handle =
		    make_uniq<HTTPFileHandle>(file_system, OpenFileInfo("http://test.invalid/object.bin"),
		                              FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO, std::move(params));
	}

	HTTPFSUtil http_util;
	TestHTTPFileSystem file_system;
	unique_ptr<HTTPFileHandle> handle;
};

static unique_ptr<HTTPResponse> RunSyntheticRange(TestHTTPFileContext &context, char *buffer, idx_t expected_length,
                                                  idx_t delivered_length) {
	const string url = "http://test.invalid/object.bin";
	return context.file_system.RunGetRangeRequest(
	    *context.handle, url, {}, context.handle->http_params, string(), true, 5, buffer, expected_length,
	    [](const HTTPResponse &) { return HTTPException("unexpected HTTP error"); },
	    [&](BaseRequest &request) {
		    auto &get_request = request.Cast<GetRequestInfo>();
		    REQUIRE(get_request.headers.GetHeaderValue("Range") ==
		            StringUtil::Format("bytes=5-%llu", static_cast<unsigned long long>(4 + expected_length)));

		    auto response = make_uniq<HTTPResponse>(HTTPStatusCode::PartialContent_206);
		    response->headers.Insert("Content-Length", to_string(expected_length));
		    REQUIRE(get_request.response_handler(*response));

		    string data(delivered_length, 'x');
		    REQUIRE(get_request.content_handler(const_data_ptr_cast(data.data()), data.size()));
		    return response;
	    });
}

static void RunFullDownloadSizeMismatch(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object_data = "AB";
	config.head_content_length = 3;
	config.range_behavior = MockS3RangeBehavior::IGNORE_RANGE;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureShortReadTest(db, con, 0, client_implementation);
	RequireQueryOk(con, "SET enable_http_metadata_cache=true");

	RequireQueryOk(con, "BEGIN TRANSACTION");
	auto initial_outcome = TryReadRange(con, server.HTTPPath(), 0, 1);

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	INFO(initial_outcome.error);
	REQUIRE(initial_outcome.failed);
	REQUIRE_FALSE(initial_outcome.internal_error);
	REQUIRE(initial_outcome.error.find("size reported by HEAD") != string::npos);
	REQUIRE(initial_outcome.error.find("full GET downloaded") != string::npos);
	REQUIRE(CountRequests(observations, "HEAD", 200) == 1);
	REQUIRE(CountRequests(observations, "GET", 200, "bytes=0-0") == 1);
	REQUIRE(CountRequests(observations, "GET", 200) == 1);

	// The full download is cached at its actual size. A later handle must report an external I/O error rather than
	// treating a bounds mismatch as a database-invalidating internal error.
	auto beyond_eof_outcome = TryReadRange(con, server.HTTPPath(), 2, 1);
	INFO(beyond_eof_outcome.error);
	REQUIRE(beyond_eof_outcome.failed);
	REQUIRE_FALSE(beyond_eof_outcome.internal_error);
	RequireQueryOk(con, "SELECT 42");

	// Clear the query-local full-file cache. The shared metadata entry must now reflect the downloaded size while
	// retaining the HEAD metadata, so reopening and reading a valid byte succeeds without trusting the stale length.
	auto http_state = HTTPState::TryGetState(*con.context);
	REQUIRE(http_state);
	http_state->Reset();
	auto &fs = FileSystem::GetFileSystem(*con.context);
	auto reopened = fs.OpenFile(server.HTTPPath(), FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_DIRECT_IO);
	REQUIRE(reopened->Cast<HTTPFileHandle>().etag == "\"httpfs-refresh-test-etag\"");
	string buffer(1, '?');
	reopened->Read(QueryContext(*con.context), &buffer[0], buffer.size(), 1);
	REQUIRE(buffer == "B");
	RequireQueryOk(con, "ROLLBACK");
}

static void RunFullDownloadFallbackControl(const string &client_implementation) {
	MockS3ServerConfig config;
	config.object_data = "ABC";
	config.range_behavior = MockS3RangeBehavior::IGNORE_RANGE;
	MockS3Server server(std::move(config));

	DuckDB db(nullptr);
	Connection con(db);
	ConfigureShortReadTest(db, con, 0, client_implementation);

	RequireQueryOk(con, "BEGIN TRANSACTION");
	REQUIRE(ReadRange(con, server.HTTPPath(), 0, 2) == "AB");
	RequireQueryOk(con, "COMMIT");

	auto observations = server.Observations();
	INFO(MockS3DescribeObservations(observations));
	REQUIRE(CountRequests(observations, "HEAD", 200) == 1);
	REQUIRE(CountRequests(observations, "GET", 200, "bytes=0-1") == 1);
	REQUIRE(CountRequests(observations, "GET", 200) == 1);
}

} // namespace

TEST_CASE("HTTP range reads reject truncated response bodies", "[httpfs][short-read]") {
	SECTION("curl persistent failure without retries") {
		RunPersistentShortRead("curl", false, 0);
	}
	SECTION("curl persistent failure exhausts retries") {
		RunPersistentShortRead("curl", false, 3);
	}
	SECTION("curl transient failure is replaced by a complete retry") {
		RunTransientShortRead("curl", false);
	}
	SECTION("curl connection caching does not preserve a failed transfer") {
		RunTransientShortRead("curl", true);
	}
	SECTION("curl connection caching still exhausts persistent failures") {
		RunPersistentShortRead("curl", true, 3);
	}
	SECTION("httplib persistent failure without retries") {
		RunPersistentShortRead("httplib", false, 0);
	}
	SECTION("httplib persistent failure exhausts retries") {
		RunPersistentShortRead("httplib", false, 3);
	}
	SECTION("httplib transient failure is replaced by a complete retry") {
		RunTransientShortRead("httplib", false);
	}

	SECTION("a cleanly terminated response must still contain every requested byte") {
		static constexpr idx_t READ_OFFSET = 113;
		static constexpr idx_t READ_LENGTH = 1024;
		const string expected_range = "bytes=113-1136";
		MockS3ServerConfig config;
		config.object_data = string(8192, 'x');
		config.range_behavior = MockS3RangeBehavior::SHORT_SUCCESS;
		config.range_behavior_requests = 1;
		config.truncated_range_bytes = 17;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, 0);

		RequireQueryOk(con, "BEGIN TRANSACTION");
		auto outcome = TryReadRange(con, server.HTTPPath(), READ_OFFSET, READ_LENGTH);
		RequireQueryOk(con, "ROLLBACK");

		auto observations = server.Observations();
		INFO(MockS3DescribeObservations(observations));
		INFO(outcome.error);
		REQUIRE(outcome.failed);
		REQUIRE(outcome.error.find("Short read for HTTP GET") != string::npos);
		REQUIRE(CountRequests(observations, "GET", 206, expected_range) == 1);
	}
}

TEST_CASE("HTTP range runner enforces response classification and exact length", "[httpfs][short-read][runner]") {
	SECTION("a successful short callback is rejected") {
		TestHTTPFileContext context;
		string buffer(8, '?');
		REQUIRE_THROWS_WITH(RunSyntheticRange(context, &buffer[0], buffer.size(), buffer.size() - 1),
		                    Catch::Matchers::Contains("Short read for HTTP GET"));
	}

	SECTION("an exact callback succeeds") {
		TestHTTPFileContext context;
		string buffer(8, '?');
		auto response = RunSyntheticRange(context, &buffer[0], buffer.size(), buffer.size());
		REQUIRE(response);
		REQUIRE(response->Success());
		REQUIRE(buffer == string(8, 'x'));
	}

	SECTION("an unsuccessful 206 is not accepted by status alone") {
		TestHTTPFileContext context;
		context.file_system.forced_status = HTTPStatusCode::PartialContent_206;
		string buffer(8, '?');
		REQUIRE_THROWS(context.file_system.TryRangeRequest(*context.handle, context.handle->path, {}, 0, &buffer[0],
		                                                   buffer.size()));
	}

	SECTION("an unsuccessful 202 is not accepted by status alone") {
		TestHTTPFileContext context;
		context.file_system.forced_status = HTTPStatusCode::Accepted_202;
		string buffer(8, '?');
		REQUIRE_THROWS(context.file_system.TryRangeRequest(*context.handle, context.handle->path, {}, 0, &buffer[0],
		                                                   buffer.size()));
	}
}

TEST_CASE("HTTP full-download fallback validates HEAD and GET lengths", "[httpfs][full-download][issue-354]") {
	SECTION("curl reports a HEAD and GET size discrepancy") {
		RunFullDownloadSizeMismatch("curl");
	}
	SECTION("httplib reports a HEAD and GET size discrepancy") {
		RunFullDownloadSizeMismatch("httplib");
	}
	SECTION("curl accepts a sufficiently large fallback body") {
		RunFullDownloadFallbackControl("curl");
	}
	SECTION("httplib accepts a sufficiently large fallback body") {
		RunFullDownloadFallbackControl("httplib");
	}
}

TEST_CASE("Parquet queries fail closed on truncated HTTP ranges", "[httpfs][short-read][parquet]") {
	auto parquet_path = TestCreatePath("httpfs_short_read.parquet");
	DuckDB fixture_db(nullptr);
	Connection fixture_con(fixture_db);
	RequireQueryOk(fixture_con,
	               StringUtil::Format("COPY (SELECT i, repeat('x', 2038) || lpad(i::VARCHAR, 10, '0') AS payload "
	                                  "FROM range(4096) t(i)) TO '%s' (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, "
	                                  "ROW_GROUP_SIZE 2048)",
	                                  parquet_path));

	std::ifstream parquet_file(parquet_path, std::ios::binary);
	REQUIRE(parquet_file.good());
	string parquet_data((std::istreambuf_iterator<char>(parquet_file)), std::istreambuf_iterator<char>());
	parquet_file.close();
	TestDeleteFile(parquet_path);
	REQUIRE_FALSE(parquet_data.empty());

	SECTION("a complete remote Parquet file returns the expected result") {
		MockS3ServerConfig config;
		config.object_key = "object.parquet";
		config.object_data = parquet_data;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, 0);
		auto result = con.Query(
		    StringUtil::Format("SELECT count(*), sum(length(payload)) FROM read_parquet('%s')", server.HTTPPath()));
		REQUIRE(result);
		INFO((result->HasError() ? result->GetError() : string()));
		REQUIRE_FALSE(result->HasError());
		REQUIRE(result->GetValue(0, 0).ToString() == "4096");
		REQUIRE(result->GetValue(1, 0).ToString() == "8388608");
	}

	SECTION("a persistently truncated remote Parquet file returns an error") {
		MockS3ServerConfig config;
		config.object_key = "object.parquet";
		config.object_data = parquet_data;
		config.range_behavior = MockS3RangeBehavior::TRUNCATE_TRANSFER;
		config.range_behavior_requests = 100;
		config.truncated_range_bytes = 17;
		MockS3Server server(std::move(config));

		DuckDB db(nullptr);
		Connection con(db);
		ConfigureShortReadTest(db, con, 1);
		auto result = con.Query(
		    StringUtil::Format("SELECT count(*), sum(length(payload)) FROM read_parquet('%s')", server.HTTPPath()));
		REQUIRE(result);
		INFO((result->HasError() ? result->GetError() : string()));
		REQUIRE(result->HasError());
	}
}

} // namespace duckdb
