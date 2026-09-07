#include "ParquetWriter.h"

#include <cassert>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/util/bit_util.h>
#include <fmt/core.h>
#include <parquet/properties.h>

// Columns use the Iceberg primitive types int32, int64, and string only.
// Iceberg has no unsigned or 8/16-bit integers, and its timestamp type holds
// microseconds. DBN fields widen as follows:
//
//   u8, u16     -> int32
//   u32         -> int64
//   u64         -> int64, two's-complement reinterpretation
//   timestamps  -> int64, Unix-epoch nanoseconds
//
// A reader recovers nanosecond timestamps with a plain cast.

namespace nplayer {

namespace {

using namespace std::chrono_literals;

// Poll interval of the two queue ends when the queue is full or empty.
constexpr auto queue_wait = 200us;

// Finished batches the queue holds before append() blocks.
constexpr std::size_t queue_depth = 4;

// Column-buffer bytes per row of the widest schema, mbo.
constexpr std::uint64_t row_bytes = 80;

void check(const arrow::Status& st) {
  if (!st.ok()) throw std::runtime_error(st.ToString());
}

template <typename T>
T unwrap(arrow::Result<T> result) {
  check(result.status());
  return std::move(result).ValueUnsafe();
}

// Column of `Builder` values written straight into the builder's reserved
// buffer. reserve() takes the buffer pointer, set() stores one value, and
// finish() advances the builder over the rows written. The pointer is valid
// only between reserve() and finish(); every reserve() takes it again.
template <typename Builder>
class value_column {
 public:
  using value_type = typename Builder::value_type;

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(builder_.Reserve(n));
    base_ = builder_.GetMutableValue(builder_.length());
    return arrow::Status::OK();
  }

  void set(std::int64_t i, value_type v) { base_[i] = v; }

  arrow::Result<std::shared_ptr<arrow::Array>> finish(std::int64_t n) {
    builder_.UnsafeAdvance(n);
    base_ = nullptr;
    return builder_.Finish();
  }

 private:
  Builder builder_;
  value_type* base_ = nullptr;
};

// value_column with a validity bitmap. The bitmap starts all valid; set()
// clears a bit for a null, so the common valid case costs one store.
template <typename Builder>
class nullable_column {
 public:
  using value_type = typename Builder::value_type;

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(builder_.Reserve(n));
    base_ = builder_.GetMutableValue(builder_.length());
    bitmap_.assign(static_cast<std::size_t>(arrow::bit_util::BytesForBits(n)), 0xFF);
    nulls_ = 0;
    return arrow::Status::OK();
  }

  void set(std::int64_t i, value_type v, bool valid) {
    base_[i] = v;
    if (!valid) [[unlikely]] {
      arrow::bit_util::ClearBit(bitmap_.data(), i);
      ++nulls_;
    }
  }

  arrow::Result<std::shared_ptr<arrow::Array>> finish(std::int64_t n) {
    if (nulls_ == 0) {
      builder_.UnsafeAdvance(n);
    } else {
      builder_.UnsafeAdvance(n, bitmap_.data(), 0);
    }
    base_ = nullptr;
    return builder_.Finish();
  }

 private:
  Builder builder_;
  value_type* base_ = nullptr;
  std::vector<std::uint8_t> bitmap_;
  std::int64_t nulls_ = 0;
};

// One-character utf8 column for `action` and `side`. Every value is one
// byte, so the offsets are 0, 1, 2, ... and one offsets buffer, built once,
// serves every batch as a slice. The values buffer is the bytes themselves.
class char_column {
 public:
  arrow::Status reserve(std::int64_t n) {
    if (capacity_ < n) {
      ARROW_ASSIGN_OR_RAISE(auto offsets, arrow::AllocateBuffer((n + 1) * kOffset));
      auto* o = reinterpret_cast<std::int32_t*>(offsets->mutable_data());
      for (std::int64_t i = 0; i <= n; ++i) o[i] = static_cast<std::int32_t>(i);
      offsets_ = std::move(offsets);
      capacity_ = n;
    }
    ARROW_ASSIGN_OR_RAISE(values_, arrow::AllocateBuffer(n));
    base_ = values_->mutable_data();
    return arrow::Status::OK();
  }

  void set(std::int64_t i, char c) { base_[i] = static_cast<std::uint8_t>(c); }

  arrow::Result<std::shared_ptr<arrow::Array>> finish(std::int64_t n) {
    auto data = arrow::ArrayData::Make(
        arrow::utf8(), n,
        {nullptr, arrow::SliceBuffer(offsets_, 0, (n + 1) * kOffset), std::move(values_)}, 0);
    base_ = nullptr;
    return arrow::MakeArray(data);
  }

 private:
  static constexpr std::int64_t kOffset = sizeof(std::int32_t);
  std::shared_ptr<arrow::Buffer> offsets_;
  std::int64_t capacity_ = 0;
  std::shared_ptr<arrow::Buffer> values_;
  std::uint8_t* base_ = nullptr;
};

using ts_column = nullable_column<arrow::Int64Builder>;

// Stores a DBN timestamp as Unix-epoch nanoseconds; the undefined sentinel
// becomes null.
void set_ts(ts_column& c, std::int64_t i, std::uint64_t ts) {
  c.set(i, static_cast<std::int64_t>(ts), ts != dbn::undef_timestamp);
}

void set_price(nullable_column<arrow::Int64Builder>& c, std::int64_t i, std::int64_t price) {
  c.set(i, price, price != dbn::undef_price);
}

void set_size(nullable_column<arrow::Int64Builder>& c, std::int64_t i, std::uint32_t size) {
  c.set(i, static_cast<std::int64_t>(size), size != dbn::undef_order_size);
}

// Stores a u64 order id as int64. An id at or above 2^63 comes out negative.
// A cast back to unsigned restores it. CME ids stay far below that.
void set_order_id(value_column<arrow::Int64Builder>& c, std::int64_t i, std::uint64_t id) {
  c.set(i, static_cast<std::int64_t>(id));
}

// Reads the record struct `T` from the front of a framed record, plus the
// trailing ts_out when present. Throws on a short record.
template <typename T>
T load(std::span<const std::byte> record, bool ts_out, std::uint64_t& out_ts) {
  const std::size_t want = sizeof(T) + (ts_out ? dbn::ts_out_len : 0);
  if (record.size() < want) {
    throw std::runtime_error(fmt::format("record of rtype 0x{:02x} is {} bytes, expected {}",
                                         std::to_integer<unsigned>(record[1]), record.size(),
                                         want));
  }
  T msg;
  std::memcpy(&msg, record.data(), sizeof(T));
  if (ts_out) std::memcpy(&out_ts, record.data() + sizeof(T), dbn::ts_out_len);
  return msg;
}

// Columns shared by every record type, in Databento's DataFrame order.
struct header_columns {
  ts_column ts_recv;
  ts_column ts_event;
  value_column<arrow::Int32Builder> rtype;
  value_column<arrow::Int32Builder> publisher_id;
  value_column<arrow::Int64Builder> instrument_id;

  static void fields(std::vector<std::shared_ptr<arrow::Field>>& f) {
    f.push_back(arrow::field("ts_recv", arrow::int64()));
    f.push_back(arrow::field("ts_event", arrow::int64()));
    f.push_back(arrow::field("rtype", arrow::int32(), false));
    f.push_back(arrow::field("publisher_id", arrow::int32(), false));
    f.push_back(arrow::field("instrument_id", arrow::int64(), false));
  }

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(ts_recv.reserve(n));
    ARROW_RETURN_NOT_OK(ts_event.reserve(n));
    ARROW_RETURN_NOT_OK(rtype.reserve(n));
    ARROW_RETURN_NOT_OK(publisher_id.reserve(n));
    return instrument_id.reserve(n);
  }

  void set(std::int64_t i, const dbn::record_header& hd, std::uint64_t recv) {
    set_ts(ts_recv, i, recv);
    set_ts(ts_event, i, hd.ts_event);
    rtype.set(i, hd.rtype);
    publisher_id.set(i, hd.publisher_id);
    instrument_id.set(i, hd.instrument_id);
  }

  arrow::Status finish(std::int64_t n, std::vector<std::shared_ptr<arrow::Array>>& out) {
    ARROW_ASSIGN_OR_RAISE(auto a0, ts_recv.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto a1, ts_event.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto a2, rtype.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto a3, publisher_id.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto a4, instrument_id.finish(n));
    out.insert(out.end(), {a0, a1, a2, a3, a4});
    return arrow::Status::OK();
  }
};

// Trailing columns every book record ends with, plus the optional ts_out.
struct tail_columns {
  bool ts_out;
  value_column<arrow::Int32Builder> ts_in_delta;
  value_column<arrow::Int64Builder> sequence;
  ts_column ts_out_col;

  explicit tail_columns(bool with_ts_out) : ts_out(with_ts_out) {}

  void fields(std::vector<std::shared_ptr<arrow::Field>>& f) const {
    f.push_back(arrow::field("ts_in_delta", arrow::int32(), false));
    f.push_back(arrow::field("sequence", arrow::int64(), false));
    if (ts_out) f.push_back(arrow::field("ts_out", arrow::int64()));
  }

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(ts_in_delta.reserve(n));
    ARROW_RETURN_NOT_OK(sequence.reserve(n));
    if (ts_out) ARROW_RETURN_NOT_OK(ts_out_col.reserve(n));
    return arrow::Status::OK();
  }

  void set(std::int64_t i, std::int32_t delta, std::uint32_t seq, std::uint64_t out_ts) {
    ts_in_delta.set(i, delta);
    sequence.set(i, static_cast<std::int64_t>(seq));
    if (ts_out) set_ts(ts_out_col, i, out_ts);
  }

  arrow::Status finish(std::int64_t n, std::vector<std::shared_ptr<arrow::Array>>& out) {
    ARROW_ASSIGN_OR_RAISE(auto a0, ts_in_delta.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto a1, sequence.finish(n));
    out.insert(out.end(), {a0, a1});
    if (ts_out) {
      ARROW_ASSIGN_OR_RAISE(auto a2, ts_out_col.finish(n));
      out.push_back(a2);
    }
    return arrow::Status::OK();
  }
};

class mbo_builder final : public batch_builder {
 public:
  explicit mbo_builder(bool ts_out) : tail_(ts_out) {
    std::vector<std::shared_ptr<arrow::Field>> f;
    header_columns::fields(f);
    f.push_back(arrow::field("action", arrow::utf8(), false));
    f.push_back(arrow::field("side", arrow::utf8(), false));
    f.push_back(arrow::field("price", arrow::int64()));
    f.push_back(arrow::field("size", arrow::int64()));
    f.push_back(arrow::field("channel_id", arrow::int32(), false));
    f.push_back(arrow::field("order_id", arrow::int64(), false));
    f.push_back(arrow::field("flags", arrow::int32(), false));
    tail_.fields(f);
    schema_ = arrow::schema(std::move(f));
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status reserve(std::int64_t n) override {
    ARROW_RETURN_NOT_OK(hd_.reserve(n));
    ARROW_RETURN_NOT_OK(action_.reserve(n));
    ARROW_RETURN_NOT_OK(side_.reserve(n));
    ARROW_RETURN_NOT_OK(price_.reserve(n));
    ARROW_RETURN_NOT_OK(size_.reserve(n));
    ARROW_RETURN_NOT_OK(channel_id_.reserve(n));
    ARROW_RETURN_NOT_OK(order_id_.reserve(n));
    ARROW_RETURN_NOT_OK(flags_.reserve(n));
    ARROW_RETURN_NOT_OK(tail_.reserve(n));
    capacity_ = n;
    return arrow::Status::OK();
  }

  bool append(std::span<const std::byte> record) override {
    if (std::to_integer<std::uint8_t>(record[1]) != dbn::rtype::mbo) return false;
    assert(rows_ < capacity_);
    std::uint64_t out_ts = dbn::undef_timestamp;
    const auto m = load<dbn::mbo_msg>(record, tail_.ts_out, out_ts);
    const std::int64_t i = rows_;
    hd_.set(i, m.hd, m.ts_recv);
    action_.set(i, m.action);
    side_.set(i, m.side);
    set_price(price_, i, m.price);
    set_size(size_, i, m.size);
    channel_id_.set(i, m.channel_id);
    set_order_id(order_id_, i, m.order_id);
    flags_.set(i, m.flags);
    tail_.set(i, m.ts_in_delta, m.sequence, out_ts);
    ++rows_;
    return true;
  }

  std::int64_t rows() const override { return rows_; }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish() override {
    const std::int64_t n = rows_;
    std::vector<std::shared_ptr<arrow::Array>> a;
    ARROW_RETURN_NOT_OK(hd_.finish(n, a));
    ARROW_ASSIGN_OR_RAISE(auto action, action_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto side, side_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto price, price_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto size, size_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto channel_id, channel_id_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto order_id, order_id_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto flags, flags_.finish(n));
    a.insert(a.end(), {action, side, price, size, channel_id, order_id, flags});
    ARROW_RETURN_NOT_OK(tail_.finish(n, a));
    rows_ = 0;
    capacity_ = 0;
    return arrow::RecordBatch::Make(schema_, n, std::move(a));
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  header_columns hd_;
  char_column action_;
  char_column side_;
  nullable_column<arrow::Int64Builder> price_;
  nullable_column<arrow::Int64Builder> size_;
  value_column<arrow::Int32Builder> channel_id_;
  value_column<arrow::Int64Builder> order_id_;
  value_column<arrow::Int32Builder> flags_;
  tail_columns tail_;
  std::int64_t rows_ = 0;
  std::int64_t capacity_ = 0;
};

class trades_builder final : public batch_builder {
 public:
  explicit trades_builder(bool ts_out) : tail_(ts_out) {
    std::vector<std::shared_ptr<arrow::Field>> f;
    header_columns::fields(f);
    f.push_back(arrow::field("action", arrow::utf8(), false));
    f.push_back(arrow::field("side", arrow::utf8(), false));
    f.push_back(arrow::field("depth", arrow::int32(), false));
    f.push_back(arrow::field("price", arrow::int64()));
    f.push_back(arrow::field("size", arrow::int64()));
    f.push_back(arrow::field("flags", arrow::int32(), false));
    tail_.fields(f);
    schema_ = arrow::schema(std::move(f));
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status reserve(std::int64_t n) override {
    ARROW_RETURN_NOT_OK(hd_.reserve(n));
    ARROW_RETURN_NOT_OK(action_.reserve(n));
    ARROW_RETURN_NOT_OK(side_.reserve(n));
    ARROW_RETURN_NOT_OK(depth_.reserve(n));
    ARROW_RETURN_NOT_OK(price_.reserve(n));
    ARROW_RETURN_NOT_OK(size_.reserve(n));
    ARROW_RETURN_NOT_OK(flags_.reserve(n));
    ARROW_RETURN_NOT_OK(tail_.reserve(n));
    capacity_ = n;
    return arrow::Status::OK();
  }

  bool append(std::span<const std::byte> record) override {
    if (std::to_integer<std::uint8_t>(record[1]) != dbn::rtype::mbp_0) return false;
    assert(rows_ < capacity_);
    std::uint64_t out_ts = dbn::undef_timestamp;
    const auto m = load<dbn::trade_msg>(record, tail_.ts_out, out_ts);
    const std::int64_t i = rows_;
    hd_.set(i, m.hd, m.ts_recv);
    action_.set(i, m.action);
    side_.set(i, m.side);
    depth_.set(i, m.depth);
    set_price(price_, i, m.price);
    set_size(size_, i, m.size);
    flags_.set(i, m.flags);
    tail_.set(i, m.ts_in_delta, m.sequence, out_ts);
    ++rows_;
    return true;
  }

  std::int64_t rows() const override { return rows_; }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish() override {
    const std::int64_t n = rows_;
    std::vector<std::shared_ptr<arrow::Array>> a;
    ARROW_RETURN_NOT_OK(hd_.finish(n, a));
    ARROW_ASSIGN_OR_RAISE(auto action, action_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto side, side_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto depth, depth_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto price, price_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto size, size_.finish(n));
    ARROW_ASSIGN_OR_RAISE(auto flags, flags_.finish(n));
    a.insert(a.end(), {action, side, depth, price, size, flags});
    ARROW_RETURN_NOT_OK(tail_.finish(n, a));
    rows_ = 0;
    capacity_ = 0;
    return arrow::RecordBatch::Make(schema_, n, std::move(a));
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  header_columns hd_;
  char_column action_;
  char_column side_;
  value_column<arrow::Int32Builder> depth_;
  nullable_column<arrow::Int64Builder> price_;
  nullable_column<arrow::Int64Builder> size_;
  value_column<arrow::Int32Builder> flags_;
  tail_columns tail_;
  std::int64_t rows_ = 0;
  std::int64_t capacity_ = 0;
};

// File-level key-value metadata: the DBN header, so a reader can tell what
// query produced the file and how to read `price`.
std::shared_ptr<arrow::KeyValueMetadata> file_metadata(const dbn::metadata& md) {
  auto join = [](const std::vector<std::string>& v) {
    std::string s;
    for (const auto& x : v) {
      if (!s.empty()) s += ',';
      s += x;
    }
    return s;
  };
  // Mappings serialize as raw_symbol=symbol@start-end, semicolon separated,
  // with the dates as YYYYMMDD and the end exclusive.
  std::string mappings;
  for (const auto& m : md.mappings) {
    for (const auto& iv : m.intervals) {
      if (!mappings.empty()) mappings += ';';
      mappings += fmt::format("{}={}@{}-{}", m.raw_symbol, iv.symbol, iv.start_date, iv.end_date);
    }
  }
  std::vector<std::string> keys{"dbn.version", "dbn.dataset", "dbn.schema", "dbn.start",
                                "dbn.end",     "dbn.stype_in", "dbn.stype_out", "dbn.symbols",
                                "dbn.partial", "dbn.not_found", "dbn.mappings", "price_scale",
                                "timestamp_unit"};
  std::vector<std::string> values{
      std::to_string(md.version),
      md.dataset,
      md.schema ? std::string(dbn::schema_name(*md.schema)) : "",
      std::to_string(md.start),
      md.end ? std::to_string(*md.end) : "",
      md.stype_in ? std::string(dbn::stype_name(*md.stype_in)) : "",
      std::string(dbn::stype_name(md.stype_out)),
      join(md.symbols),
      join(md.partial),
      join(md.not_found),
      mappings,
      std::to_string(dbn::price_scale),
      "ns"};
  return arrow::KeyValueMetadata::Make(std::move(keys), std::move(values));
}

// Parquet encoding: zstd everywhere; the monotone timestamp and sequence
// columns delta-packed instead of dictionary-encoded, which the dictionary
// would fall back from anyway on a million distinct values per row group.
std::shared_ptr<parquet::WriterProperties> writer_properties(std::int64_t batch_rows) {
  parquet::WriterProperties::Builder b;
  b.compression(parquet::Compression::ZSTD)
      ->max_row_group_length(batch_rows)
      ->version(parquet::ParquetVersion::PARQUET_2_6);
  for (const char* col : {"ts_recv", "ts_event", "sequence", "ts_out"}) {
    b.disable_dictionary(col)->encoding(col, parquet::Encoding::DELTA_BINARY_PACKED);
  }
  b.disable_dictionary("order_id");
  return b.build();
}

}  // namespace

std::uint64_t ParquetWriter::peak_memory(std::int64_t batch_rows) {
  // One batch under construction, queue_depth waiting, one being encoded;
  // then the zstd window, the file buffers, and the Parquet page buffers.
  return static_cast<std::uint64_t>(batch_rows) * row_bytes * (queue_depth + 2) +
         (std::uint64_t{64} << 20);
}

std::unique_ptr<batch_builder> make_batch_builder(dbn::schema schema, bool ts_out) {
  switch (schema) {
    case dbn::schema::mbo: return std::make_unique<mbo_builder>(ts_out);
    case dbn::schema::trades: return std::make_unique<trades_builder>(ts_out);
    default:
      throw std::runtime_error(
          fmt::format("schema {} is unsupported; mbo and trades can be written",
                      dbn::schema_name(schema)));
  }
}

// ParquetWriter

ParquetWriter::ParquetWriter(const std::filesystem::path& path, const dbn::metadata& md,
                             std::int64_t batch_rows)
    : batch_rows_(batch_rows),
      builder_(make_batch_builder(md.schema.value(), md.ts_out)),
      queue_(queue_depth) {
  if (batch_rows <= 0) throw std::runtime_error("batch_rows must be positive");
  sink_ = unwrap(arrow::io::FileOutputStream::Open(path.string()));
  const auto schema = builder_->schema()->WithMetadata(file_metadata(md));
  // use_threads encodes the columns of a batch in parallel on Arrow's CPU
  // thread pool, which every ParquetWriter in the process shares.
  auto arrow_props =
      parquet::ArrowWriterProperties::Builder().store_schema()->set_use_threads(true)->build();
  writer_ = unwrap(parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), sink_,
                                                    writer_properties(batch_rows), arrow_props));
  check(builder_->reserve(batch_rows_));
  thread_ = std::thread([this] { writer_loop(); });
}

ParquetWriter::~ParquetWriter() {
  if (thread_.joinable()) {
    enqueue(nullptr);
    thread_.join();
  }
}

void ParquetWriter::append(std::span<const std::byte> record) {
  if (failed_.load(std::memory_order_acquire)) check(thread_status_);
  if (!builder_->append(record)) {
    ++skipped_;
    return;
  }
  if (builder_->rows() >= batch_rows_) flush();
}

void ParquetWriter::flush() {
  if (builder_->rows() == 0) return;
  enqueue(unwrap(builder_->finish()));
  check(builder_->reserve(batch_rows_));
}

void ParquetWriter::enqueue(std::shared_ptr<arrow::RecordBatch> batch) {
  while (!queue_.try_push(std::move(batch))) std::this_thread::sleep_for(queue_wait);
}

// Writer thread. After a failure it keeps draining so the producer never
// blocks on a full queue; the failure surfaces on the producer's next call.
void ParquetWriter::writer_loop() {
  while (true) {
    auto item = queue_.try_pop();
    if (!item) {
      std::this_thread::sleep_for(queue_wait);
      continue;
    }
    if (*item == nullptr) return;
    if (failed_.load(std::memory_order_relaxed)) continue;
    const arrow::Status st = writer_->WriteRecordBatch(**item);
    if (!st.ok()) {
      thread_status_ = st;
      failed_.store(true, std::memory_order_release);
      continue;
    }
    written_ += static_cast<std::uint64_t>((*item)->num_rows());
  }
}

std::uint64_t ParquetWriter::close() {
  if (closed_) return written_;
  closed_ = true;
  flush();
  enqueue(nullptr);
  thread_.join();
  if (failed_.load(std::memory_order_acquire)) check(thread_status_);
  check(writer_->Close());
  check(sink_->Close());
  return written_;
}

}  // namespace nplayer
