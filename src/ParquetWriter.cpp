#include "ParquetWriter.h"

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <parquet/properties.h>

namespace nplayer {

namespace {

using namespace std::chrono_literals;

// Poll interval of the two queue ends when the queue is full or empty.
constexpr auto queue_wait = 200us;

void check(const arrow::Status& st) {
  if (!st.ok()) throw std::runtime_error(st.ToString());
}

template <typename T>
T unwrap(arrow::Result<T> result) {
  check(result.status());
  return std::move(result).ValueUnsafe();
}

std::shared_ptr<arrow::DataType> ts_ns() { return arrow::timestamp(arrow::TimeUnit::NANO, "UTC"); }

// Appends a DBN timestamp; the undefined sentinel becomes null.
void append_ts(arrow::TimestampBuilder& b, std::uint64_t ts) {
  if (ts == dbn::undef_timestamp) {
    b.UnsafeAppendNull();
  } else {
    b.UnsafeAppend(static_cast<std::int64_t>(ts));
  }
}

void append_price(arrow::Int64Builder& b, std::int64_t price) {
  if (price == dbn::undef_price) {
    b.UnsafeAppendNull();
  } else {
    b.UnsafeAppend(price);
  }
}

void append_size(arrow::UInt32Builder& b, std::uint32_t size) {
  if (size == dbn::undef_order_size) {
    b.UnsafeAppendNull();
  } else {
    b.UnsafeAppend(size);
  }
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
  arrow::TimestampBuilder ts_recv{ts_ns(), arrow::default_memory_pool()};
  arrow::TimestampBuilder ts_event{ts_ns(), arrow::default_memory_pool()};
  arrow::UInt8Builder rtype;
  arrow::UInt16Builder publisher_id;
  arrow::UInt32Builder instrument_id;

  static void fields(std::vector<std::shared_ptr<arrow::Field>>& f) {
    f.push_back(arrow::field("ts_recv", ts_ns()));
    f.push_back(arrow::field("ts_event", ts_ns()));
    f.push_back(arrow::field("rtype", arrow::uint8(), false));
    f.push_back(arrow::field("publisher_id", arrow::uint16(), false));
    f.push_back(arrow::field("instrument_id", arrow::uint32(), false));
  }

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(ts_recv.Reserve(n));
    ARROW_RETURN_NOT_OK(ts_event.Reserve(n));
    ARROW_RETURN_NOT_OK(rtype.Reserve(n));
    ARROW_RETURN_NOT_OK(publisher_id.Reserve(n));
    return instrument_id.Reserve(n);
  }

  void append(const dbn::record_header& hd, std::uint64_t recv) {
    append_ts(ts_recv, recv);
    append_ts(ts_event, hd.ts_event);
    rtype.UnsafeAppend(hd.rtype);
    publisher_id.UnsafeAppend(hd.publisher_id);
    instrument_id.UnsafeAppend(hd.instrument_id);
  }

  arrow::Status finish(std::vector<std::shared_ptr<arrow::Array>>& out) {
    ARROW_ASSIGN_OR_RAISE(auto a0, ts_recv.Finish());
    ARROW_ASSIGN_OR_RAISE(auto a1, ts_event.Finish());
    ARROW_ASSIGN_OR_RAISE(auto a2, rtype.Finish());
    ARROW_ASSIGN_OR_RAISE(auto a3, publisher_id.Finish());
    ARROW_ASSIGN_OR_RAISE(auto a4, instrument_id.Finish());
    out.insert(out.end(), {a0, a1, a2, a3, a4});
    return arrow::Status::OK();
  }
};

// Trailing columns every book record ends with, plus the optional ts_out.
struct tail_columns {
  bool ts_out;
  arrow::Int32Builder ts_in_delta;
  arrow::UInt32Builder sequence;
  arrow::TimestampBuilder ts_out_col{ts_ns(), arrow::default_memory_pool()};

  explicit tail_columns(bool with_ts_out) : ts_out(with_ts_out) {}

  void fields(std::vector<std::shared_ptr<arrow::Field>>& f) const {
    f.push_back(arrow::field("ts_in_delta", arrow::int32(), false));
    f.push_back(arrow::field("sequence", arrow::uint32(), false));
    if (ts_out) f.push_back(arrow::field("ts_out", ts_ns()));
  }

  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(ts_in_delta.Reserve(n));
    ARROW_RETURN_NOT_OK(sequence.Reserve(n));
    if (ts_out) ARROW_RETURN_NOT_OK(ts_out_col.Reserve(n));
    return arrow::Status::OK();
  }

  void append(std::int32_t delta, std::uint32_t seq, std::uint64_t out_ts) {
    ts_in_delta.UnsafeAppend(delta);
    sequence.UnsafeAppend(seq);
    if (ts_out) append_ts(ts_out_col, out_ts);
  }

  arrow::Status finish(std::vector<std::shared_ptr<arrow::Array>>& out) {
    ARROW_ASSIGN_OR_RAISE(auto a0, ts_in_delta.Finish());
    ARROW_ASSIGN_OR_RAISE(auto a1, sequence.Finish());
    out.insert(out.end(), {a0, a1});
    if (ts_out) {
      ARROW_ASSIGN_OR_RAISE(auto a2, ts_out_col.Finish());
      out.push_back(a2);
    }
    return arrow::Status::OK();
  }
};

// One-character string column for `action` and `side`.
class char_column {
 public:
  arrow::Status reserve(std::int64_t n) {
    ARROW_RETURN_NOT_OK(b_.Reserve(n));
    return b_.ReserveData(n);
  }
  void append(char c) { b_.UnsafeAppend(&c, 1); }
  arrow::Result<std::shared_ptr<arrow::Array>> finish() { return b_.Finish(); }

 private:
  arrow::StringBuilder b_;
};

class mbo_builder final : public batch_builder {
 public:
  explicit mbo_builder(bool ts_out) : tail_(ts_out) {
    std::vector<std::shared_ptr<arrow::Field>> f;
    header_columns::fields(f);
    f.push_back(arrow::field("action", arrow::utf8(), false));
    f.push_back(arrow::field("side", arrow::utf8(), false));
    f.push_back(arrow::field("price", arrow::int64()));
    f.push_back(arrow::field("size", arrow::uint32()));
    f.push_back(arrow::field("channel_id", arrow::uint8(), false));
    f.push_back(arrow::field("order_id", arrow::uint64(), false));
    f.push_back(arrow::field("flags", arrow::uint8(), false));
    tail_.fields(f);
    schema_ = arrow::schema(std::move(f));
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status reserve(std::int64_t n) override {
    ARROW_RETURN_NOT_OK(hd_.reserve(n));
    ARROW_RETURN_NOT_OK(action_.reserve(n));
    ARROW_RETURN_NOT_OK(side_.reserve(n));
    ARROW_RETURN_NOT_OK(price_.Reserve(n));
    ARROW_RETURN_NOT_OK(size_.Reserve(n));
    ARROW_RETURN_NOT_OK(channel_id_.Reserve(n));
    ARROW_RETURN_NOT_OK(order_id_.Reserve(n));
    ARROW_RETURN_NOT_OK(flags_.Reserve(n));
    return tail_.reserve(n);
  }

  bool append(std::span<const std::byte> record) override {
    if (std::to_integer<std::uint8_t>(record[1]) != dbn::rtype::mbo) return false;
    std::uint64_t out_ts = dbn::undef_timestamp;
    const auto m = load<dbn::mbo_msg>(record, tail_.ts_out, out_ts);
    hd_.append(m.hd, m.ts_recv);
    action_.append(m.action);
    side_.append(m.side);
    append_price(price_, m.price);
    append_size(size_, m.size);
    channel_id_.UnsafeAppend(m.channel_id);
    order_id_.UnsafeAppend(m.order_id);
    flags_.UnsafeAppend(m.flags);
    tail_.append(m.ts_in_delta, m.sequence, out_ts);
    ++rows_;
    return true;
  }

  std::int64_t rows() const override { return rows_; }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish() override {
    std::vector<std::shared_ptr<arrow::Array>> a;
    ARROW_RETURN_NOT_OK(hd_.finish(a));
    ARROW_ASSIGN_OR_RAISE(auto action, action_.finish());
    ARROW_ASSIGN_OR_RAISE(auto side, side_.finish());
    ARROW_ASSIGN_OR_RAISE(auto price, price_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto size, size_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto channel_id, channel_id_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto order_id, order_id_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto flags, flags_.Finish());
    a.insert(a.end(), {action, side, price, size, channel_id, order_id, flags});
    ARROW_RETURN_NOT_OK(tail_.finish(a));
    auto batch = arrow::RecordBatch::Make(schema_, rows_, std::move(a));
    rows_ = 0;
    return batch;
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  header_columns hd_;
  char_column action_;
  char_column side_;
  arrow::Int64Builder price_;
  arrow::UInt32Builder size_;
  arrow::UInt8Builder channel_id_;
  arrow::UInt64Builder order_id_;
  arrow::UInt8Builder flags_;
  tail_columns tail_;
  std::int64_t rows_ = 0;
};

class trades_builder final : public batch_builder {
 public:
  explicit trades_builder(bool ts_out) : tail_(ts_out) {
    std::vector<std::shared_ptr<arrow::Field>> f;
    header_columns::fields(f);
    f.push_back(arrow::field("action", arrow::utf8(), false));
    f.push_back(arrow::field("side", arrow::utf8(), false));
    f.push_back(arrow::field("depth", arrow::uint8(), false));
    f.push_back(arrow::field("price", arrow::int64()));
    f.push_back(arrow::field("size", arrow::uint32()));
    f.push_back(arrow::field("flags", arrow::uint8(), false));
    tail_.fields(f);
    schema_ = arrow::schema(std::move(f));
  }

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status reserve(std::int64_t n) override {
    ARROW_RETURN_NOT_OK(hd_.reserve(n));
    ARROW_RETURN_NOT_OK(action_.reserve(n));
    ARROW_RETURN_NOT_OK(side_.reserve(n));
    ARROW_RETURN_NOT_OK(depth_.Reserve(n));
    ARROW_RETURN_NOT_OK(price_.Reserve(n));
    ARROW_RETURN_NOT_OK(size_.Reserve(n));
    ARROW_RETURN_NOT_OK(flags_.Reserve(n));
    return tail_.reserve(n);
  }

  bool append(std::span<const std::byte> record) override {
    if (std::to_integer<std::uint8_t>(record[1]) != dbn::rtype::mbp_0) return false;
    std::uint64_t out_ts = dbn::undef_timestamp;
    const auto m = load<dbn::trade_msg>(record, tail_.ts_out, out_ts);
    hd_.append(m.hd, m.ts_recv);
    action_.append(m.action);
    side_.append(m.side);
    depth_.UnsafeAppend(m.depth);
    append_price(price_, m.price);
    append_size(size_, m.size);
    flags_.UnsafeAppend(m.flags);
    tail_.append(m.ts_in_delta, m.sequence, out_ts);
    ++rows_;
    return true;
  }

  std::int64_t rows() const override { return rows_; }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish() override {
    std::vector<std::shared_ptr<arrow::Array>> a;
    ARROW_RETURN_NOT_OK(hd_.finish(a));
    ARROW_ASSIGN_OR_RAISE(auto action, action_.finish());
    ARROW_ASSIGN_OR_RAISE(auto side, side_.finish());
    ARROW_ASSIGN_OR_RAISE(auto depth, depth_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto price, price_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto size, size_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto flags, flags_.Finish());
    a.insert(a.end(), {action, side, depth, price, size, flags});
    ARROW_RETURN_NOT_OK(tail_.finish(a));
    auto batch = arrow::RecordBatch::Make(schema_, rows_, std::move(a));
    rows_ = 0;
    return batch;
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  header_columns hd_;
  char_column action_;
  char_column side_;
  arrow::UInt8Builder depth_;
  arrow::Int64Builder price_;
  arrow::UInt32Builder size_;
  arrow::UInt8Builder flags_;
  tail_columns tail_;
  std::int64_t rows_ = 0;
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
                                "dbn.partial", "dbn.not_found", "dbn.mappings", "price_scale"};
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
      std::to_string(dbn::price_scale)};
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
      queue_(4) {
  if (batch_rows <= 0) throw std::runtime_error("batch_rows must be positive");
  sink_ = unwrap(arrow::io::FileOutputStream::Open(path.string()));
  const auto schema = builder_->schema()->WithMetadata(file_metadata(md));
  auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
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
