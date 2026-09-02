#include "s3/s3_settings.hpp"

#include "s3/s3_auth.hpp"
#include "s3/s3_provider.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

namespace {

struct S3UploadSizing {
	static bool ScheduleCovers(uint64_t initial_part_size, uint64_t max_file_size, idx_t max_parts,
	                           idx_t growth_interval) {
		uint64_t capacity = 0;
		idx_t completed_parts = 0;
		while (completed_parts < max_parts) {
			auto part_size = PartSize(initial_part_size, growth_interval, completed_parts);
			auto parts_at_size = MinValue<idx_t>(growth_interval, max_parts - completed_parts);
			if (part_size > (max_file_size - MinValue<uint64_t>(capacity, max_file_size)) / parts_at_size) {
				return true;
			}
			capacity += part_size * parts_at_size;
			if (capacity >= max_file_size) {
				return true;
			}
			completed_parts += parts_at_size;
		}
		return false;
	}

	static idx_t PartSize(uint64_t initial_part_size, idx_t growth_interval, idx_t completed_parts) {
		auto result = initial_part_size;
		auto doubling_count = completed_parts / growth_interval;
		while (doubling_count > 0 && result < S3UploadConfig::MAX_MULTIPART_PART_SIZE) {
			result = MinValue<uint64_t>(result * 2, S3UploadConfig::MAX_MULTIPART_PART_SIZE);
			doubling_count--;
		}
		return NumericCast<idx_t>(result);
	}

	static idx_t InitialPartSize(uint64_t max_file_size, idx_t max_parts, idx_t growth_interval) {
		const auto block_size = NumericCast<uint64_t>(Storage::DEFAULT_BLOCK_SIZE + Storage::DEFAULT_BLOCK_HEADER_SIZE);
		D_ASSERT(S3UploadConfig::MIN_MULTIPART_PART_SIZE % block_size == 0);
		D_ASSERT(S3UploadConfig::MAX_MULTIPART_PART_SIZE % block_size == 0);
		auto lower = NumericCast<uint64_t>(S3UploadConfig::MIN_MULTIPART_PART_SIZE) / block_size;
		auto upper = NumericCast<uint64_t>(S3UploadConfig::MAX_MULTIPART_PART_SIZE) / block_size;
		while (lower < upper) {
			auto middle = lower + (upper - lower) / 2;
			if (ScheduleCovers(middle * block_size, max_file_size, max_parts, growth_interval)) {
				upper = middle;
			} else {
				lower = middle + 1;
			}
		}
		return NumericCast<idx_t>(lower * block_size);
	}
};

} // namespace

static void SetS3URLStyle(ClientContext &, SetScope, Value &parameter) {
	S3Provider::ParseURLStyle(StringValue::Get(parameter));
}

S3UploadConfig S3UploadConfig::Create(uint64_t max_file_size, uint64_t max_parts) {
	if (max_file_size == 0) {
		throw InvalidInputException("s3_uploader_max_filesize must be greater than zero");
	}
	if (max_parts == 0 || max_parts > MAX_MULTIPART_PARTS) {
		throw InvalidInputException("s3_uploader_max_parts_per_file must be between 1 and 10000");
	}
	const auto supported_file_size = NumericCast<uint64_t>(MAX_MULTIPART_PART_SIZE) * max_parts;
	if (max_file_size > supported_file_size) {
		throw InvalidInputException(
		    "s3_uploader_max_filesize exceeds the size supported by s3_uploader_max_parts_per_file");
	}

	S3UploadConfig result;
	result.max_file_size = max_file_size;
	result.max_parts = NumericCast<idx_t>(max_parts);
	result.growth_interval = (result.max_parts + PART_SIZE_TIERS - 1) / PART_SIZE_TIERS;
	result.initial_part_size = S3UploadSizing::InitialPartSize(max_file_size, result.max_parts, result.growth_interval);
	return result;
}

S3UploadConfig S3UploadConfig::ReadFrom(optional_ptr<FileOpener> opener) {
	uint64_t uploader_max_filesize;
	uint64_t max_parts_per_file;
	Value value;

	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_filesize", value)) {
		uploader_max_filesize = DBConfig::ParseMemoryLimit(value.GetValue<string>());
	} else {
		uploader_max_filesize = S3UploadConfig::DEFAULT_MAX_FILESIZE;
	}
	if (FileOpener::TryGetCurrentSetting(opener, "s3_uploader_max_parts_per_file", value)) {
		max_parts_per_file = value.GetValue<uint64_t>();
	} else {
		max_parts_per_file = S3UploadConfig::DEFAULT_MAX_PARTS_PER_FILE;
	}
	return Create(uploader_max_filesize, max_parts_per_file);
}

idx_t S3UploadConfig::PartSize(idx_t reserved_parts) const {
	D_ASSERT(initial_part_size >= MIN_MULTIPART_PART_SIZE);
	D_ASSERT(initial_part_size <= MAX_MULTIPART_PART_SIZE);
	D_ASSERT(growth_interval > 0);
	return S3UploadSizing::PartSize(initial_part_size, growth_interval, reserved_parts);
}

bool S3UploadConfig::HasPartCapacity(idx_t reserved_parts) const {
	return reserved_parts < max_parts;
}

void S3Settings::Register(DBConfig &config) {
	config.AddExtensionOption("s3_region", "S3 Region", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_access_key_id", "S3 Access Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_secret_access_key", "S3 Access Key", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_session_token", "S3 Session Token", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_endpoint", "S3 Endpoint", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_url_style", "S3 URL style", LogicalType::VARCHAR, Value("vhost"), SetS3URLStyle);
	config.AddExtensionOption("s3_use_ssl", "S3 use SSL", LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("s3_kms_key_id", "S3 KMS Key ID", LogicalType::VARCHAR);
	config.AddExtensionOption("s3_url_compatibility_mode", "Disable Globs and Query Parameters on S3 URLs",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("s3_requester_pays", "S3 use requester pays mode", LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption(
	    "s3_url_scheme_aliases",
	    "Additional URL schemes routed to the S3-compatible filesystem (e.g. ['oss', 'cos']). "
	    "Can only be set globally; secrets for aliased schemes need an explicit SCOPE.",
	    LogicalType::LIST(LogicalType::VARCHAR), Value::LIST(LogicalType::VARCHAR, vector<Value>()),
	    [](ClientContext &context, SetScope scope, Value &parameter) {
		    if (scope == SetScope::SESSION || scope == SetScope::LOCAL) {
			    throw InvalidInputException("s3_url_scheme_aliases can only be set globally");
		    }
		    // Validate only: routing reads the stored value from DBConfig
		    parameter = S3Provider::NormalizeSchemeAliases(parameter);
	    },
	    SetScope::GLOBAL);
	config.AddExtensionOption(
	    "s3_allow_recursive_globbing",
	    "Whether globs on S3-like storage are optimized with recursive strategy (alternative is listing)",
	    LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("s3_uploader_max_filesize", "Maximum size of an S3 upload", LogicalType::VARCHAR, "80GB");
	config.AddExtensionOption("s3_uploader_max_parts_per_file", "Maximum number of parts in an S3 multipart upload",
	                          LogicalType::UBIGINT, Value::UBIGINT(10000));
	config.AddExtensionOption("s3_version_id_pinning", "Pin S3 reads to a specific object version for consistency",
	                          LogicalType::BOOLEAN, Value(false));
	config.AddExtensionOption("merge_http_secret_into_s3_request", "Merges HTTP secret parameters into S3 requests",
	                          LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("httpfs_enable_credential_refresh", "Enable credential refresh for HTTPFS S3 secrets",
	                          LogicalType::BOOLEAN, Value(true));
	config.AddExtensionOption("enable_global_s3_configuration",
	                          "Automatically fetch AWS credentials from environment variables.", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
}

void S3Settings::Initialize(DBConfig &config) {
	auto provider = make_uniq<AWSEnvironmentCredentialsProvider>(config);
	provider->SetAll();
}

} // namespace duckdb
