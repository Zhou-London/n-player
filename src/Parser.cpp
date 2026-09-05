#include "Parser.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <fmt/core.h>

#include "ParquetWriter.h"

namespace nplayer {

namespace {

// Bounds-checked little-endian reader over the metadata bytes.
class cursor {
 public:
  explicit cursor(std::span<const std::byte> data) : data_(data) {}

  template <typename T>
  T read() {
    static_assert(std::is_trivially_copyable_v<T>);
    need(sizeof(T));
    T value;
    std::memcpy(&value, data_.data() + pos_, sizeof(T));
    pos_ += sizeof(T);
    return value;
  }

  // Reads a fixed-width field and returns the text before its first NUL.
  std::string cstr(std::size_t width) {
    need(width);
    const char* begin = reinterpret_cast<const char*>(data_.data() + pos_);
    const std::size_t len = static_cast<std::size_t>(std::find(begin, begin + width, '\0') - begin);
    pos_ += width;
    return std::string(begin, len);
  }

  void skip(std::size_t n) {
    need(n);
    pos_ += n;
  }

 private:
  void need(std::size_t n) const;

  std::span<const std::byte> data_;
  std::size_t pos_ = 0;
};

}  // namespace

// Parser

Parser::Parser(const std::filesystem::path& file) : path_(file) {
  file_ = std::fopen(file.c_str(), "rb");
  if (file_ == nullptr) fail(fmt::format("cannot open: {}", std::strerror(errno)));
  dstream_ = ZSTD_createDStream();
  if (dstream_ == nullptr) fail("ZSTD_createDStream failed");
  const std::size_t init = ZSTD_initDStream(dstream_);
  if (ZSTD_isError(init)) fail(ZSTD_getErrorName(init));
  in_.resize(ZSTD_DStreamInSize());
  out_chunk_ = ZSTD_DStreamOutSize();
  read_metadata();
}

Parser::~Parser() {
  ZSTD_freeDStream(dstream_);
  if (file_ != nullptr) std::fclose(file_);
}

void Parser::fail(const std::string& what) const {
  throw std::runtime_error(fmt::format("{}: {}", path_.string(), what));
}

bool Parser::fill() {
  while (true) {
    if (in_buf_.pos == in_buf_.size) {
      const std::size_t n = std::fread(in_.data(), 1, in_.size(), file_);
      if (n == 0) {
        if (std::ferror(file_)) fail("read error");
        if (in_frame_) fail("truncated zstd stream");
        return false;
      }
      in_buf_ = {in_.data(), n, 0};
    }
    const std::size_t old = buf_.size();
    buf_.resize(old + out_chunk_);
    ZSTD_outBuffer out{buf_.data() + old, out_chunk_, 0};
    const std::size_t ret = ZSTD_decompressStream(dstream_, &out, &in_buf_);
    if (ZSTD_isError(ret)) fail(fmt::format("zstd: {}", ZSTD_getErrorName(ret)));
    in_frame_ = ret != 0;
    buf_.resize(old + out.pos);
    if (out.pos > 0) return true;
  }
}

void Parser::read_metadata() {
  while (buf_.size() < dbn::prelude_len) {
    if (!fill()) fail("file shorter than the DBN prelude");
  }
  if (std::memcmp(buf_.data(), "DBN", 3) != 0) fail("missing DBN magic");
  const auto version = std::to_integer<std::uint8_t>(buf_[3]);
  if (version < 1 || version > dbn::max_version) {
    fail(fmt::format("DBN version {} is outside 1..{}", version, dbn::max_version));
  }
  std::uint32_t length;
  std::memcpy(&length, buf_.data() + 4, sizeof(length));
  if (length < dbn::fixed_len) fail("metadata length shorter than the fixed header");
  while (buf_.size() < dbn::prelude_len + length) {
    if (!fill()) fail("file ends inside the metadata header");
  }

  cursor c(std::span<const std::byte>(buf_.data() + dbn::prelude_len, length));
  dbn::metadata md;
  md.version = version;
  md.dataset = c.cstr(dbn::dataset_cstr_len);
  const auto raw_schema = c.read<std::uint16_t>();
  if (raw_schema != dbn::null_schema) md.schema = static_cast<dbn::schema>(raw_schema);
  md.start = c.read<std::uint64_t>();
  const auto end = c.read<std::uint64_t>();
  if (end != dbn::undef_timestamp && end != 0) md.end = end;
  const auto limit = c.read<std::uint64_t>();
  if (limit != 0) md.limit = limit;
  if (version == 1) c.skip(sizeof(std::uint64_t));  // record_count, dropped in v2
  const auto stype_in = c.read<std::uint8_t>();
  if (stype_in != dbn::null_stype) md.stype_in = static_cast<dbn::stype>(stype_in);
  md.stype_out = static_cast<dbn::stype>(c.read<std::uint8_t>());
  md.ts_out = c.read<std::uint8_t>() != 0;
  if (version == 1) {
    md.symbol_cstr_len = dbn::symbol_cstr_len_v1;
    c.skip(dbn::reserved_len_v1);
  } else {
    md.symbol_cstr_len = c.read<std::uint16_t>();
    c.skip(dbn::reserved_len);
  }
  if (md.symbol_cstr_len == 0) fail("metadata declares a zero symbol width");
  const auto schema_definition_len = c.read<std::uint32_t>();
  if (schema_definition_len != 0) fail("schema definitions are unsupported");

  auto read_symbols = [&](std::vector<std::string>& out) {
    const auto count = c.read<std::uint32_t>();
    out.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) out.push_back(c.cstr(md.symbol_cstr_len));
  };
  read_symbols(md.symbols);
  read_symbols(md.partial);
  read_symbols(md.not_found);
  const auto mapping_count = c.read<std::uint32_t>();
  md.mappings.reserve(mapping_count);
  for (std::uint32_t i = 0; i < mapping_count; ++i) {
    dbn::symbol_mapping m;
    m.raw_symbol = c.cstr(md.symbol_cstr_len);
    const auto interval_count = c.read<std::uint32_t>();
    m.intervals.reserve(interval_count);
    for (std::uint32_t j = 0; j < interval_count; ++j) {
      dbn::mapping_interval iv;
      iv.start_date = c.read<std::uint32_t>();
      iv.end_date = c.read<std::uint32_t>();
      iv.symbol = c.cstr(md.symbol_cstr_len);
      m.intervals.push_back(std::move(iv));
    }
    md.mappings.push_back(std::move(m));
  }

  metadata_ = std::move(md);
  pos_ = dbn::prelude_len + length;
}

std::uint64_t Parser::for_each_record(
    const std::function<void(std::span<const std::byte>)>& fn, std::uint64_t limit) {
  std::uint64_t count = 0;
  while (true) {
    while (pos_ < buf_.size()) {
      const std::size_t len = std::to_integer<std::size_t>(buf_[pos_]) * 4;
      if (len < sizeof(dbn::record_header)) {
        fail(fmt::format("record {} declares {} bytes, shorter than a header", count, len));
      }
      if (pos_ + len > buf_.size()) break;  // record continues in the next chunk
      fn(std::span<const std::byte>(buf_.data() + pos_, len));
      pos_ += len;
      ++count;
      if (limit != 0 && count == limit) return count;
    }
    buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(pos_));
    pos_ = 0;
    if (!fill()) {
      if (!buf_.empty()) fail("file ends inside a record");
      return count;
    }
  }
}

void cursor::need(std::size_t n) const {
  if (pos_ + n > data_.size()) {
    throw std::runtime_error(fmt::format("metadata header ends early at byte {}", pos_));
  }
}

// Conversion

convert_result convert_file(const std::filesystem::path& in, const std::filesystem::path& out,
                            const options& opt) {
  const auto t0 = std::chrono::steady_clock::now();
  Parser parser(in);
  const dbn::metadata& md = parser.metadata();
  if (!md.schema) {
    throw std::runtime_error(fmt::format("{}: file mixes schemas, which Parquet cannot hold",
                                         in.string()));
  }

  std::filesystem::create_directories(out.parent_path());
  const std::filesystem::path tmp = out.string() + ".tmp";
  convert_result result;
  try {
    ParquetWriter writer(tmp, md, opt.batch_rows);
    result.records =
        parser.for_each_record([&](std::span<const std::byte> r) { writer.append(r); }, opt.limit);
    result.rows = writer.close();
  } catch (...) {
    std::error_code ignored;
    std::filesystem::remove(tmp, ignored);
    throw;
  }
  std::filesystem::rename(tmp, out);
  result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  return result;
}

int convert_tree(const options& opt) {
  namespace fs = std::filesystem;
  if (!fs::is_directory(opt.input)) {
    throw std::runtime_error(fmt::format("input is not a directory: {}", opt.input.string()));
  }
  std::vector<fs::path> files;
  for (const auto& entry : fs::recursive_directory_iterator(opt.input)) {
    if (entry.is_regular_file() && entry.path().filename().string().ends_with(".dbn.zst")) {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  if (files.empty()) {
    fmt::print("no .dbn.zst files under {}\n", opt.input.string());
    return 0;
  }

  std::atomic<std::size_t> next{0};
  std::atomic<int> failed{0};
  std::mutex log;
  auto worker = [&] {
    while (true) {
      const std::size_t i = next.fetch_add(1);
      if (i >= files.size()) return;
      const fs::path& in = files[i];
      std::string name = in.filename().string();
      name.resize(name.size() - std::strlen(".dbn.zst"));
      const fs::path out =
          opt.output / fs::relative(in, opt.input).parent_path() / (name + ".parquet");
      try {
        const convert_result r = convert_file(in, out, opt);
        const double mib = static_cast<double>(fs::file_size(in)) / (1024.0 * 1024.0);
        std::lock_guard lock(log);
        fmt::print("{} -> {}: {} records, {} rows, {:.1f}s ({:.1f} MiB/s compressed)\n",
                   in.string(), out.string(), r.records, r.rows, r.seconds,
                   r.seconds > 0 ? mib / r.seconds : 0.0);
      } catch (const std::exception& e) {
        failed.fetch_add(1);
        std::lock_guard lock(log);
        fmt::print(stderr, "failed: {}\n", e.what());
      }
    }
  };

  const unsigned jobs = std::max(1u, std::min<unsigned>(opt.jobs, files.size()));
  std::vector<std::thread> threads;
  for (unsigned i = 1; i < jobs; ++i) threads.emplace_back(worker);
  worker();
  for (auto& t : threads) t.join();
  return failed.load();
}

}  // namespace nplayer
