#pragma once

// Encapsulation of Apache Arrow

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <thread>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <nlib/single_queue.h>
#include <parquet/arrow/writer.h>

#include "common.h"

namespace nplayer {

// Column builders for one DBN schema. Collects records into an Arrow
// RecordBatch: reserve(), then append() up to that many rows, then finish().
// Appending past the reservation is undefined; the columns write straight
// into reserved buffers without a capacity check.
class batch_builder {
 public:
  virtual ~batch_builder() = default;

  virtual std::shared_ptr<arrow::Schema> schema() const = 0;

  // Reserves room for `rows` more appends. finish() releases the reservation.
  virtual arrow::Status reserve(std::int64_t rows) = 0;

  // Appends one framed record. Returns false, appending nothing, when the
  // record's rtype belongs to another schema. Throws on a short record.
  virtual bool append(std::span<const std::byte> record) = 0;

  virtual std::int64_t rows() const = 0;

  // Returns the appended rows as one batch and resets the builder.
  virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> finish() = 0;
};

// Returns the builder for `schema`; throws std::runtime_error for a schema
// this tool cannot write. `ts_out` adds the trailing ts_out column.
std::unique_ptr<batch_builder> make_batch_builder(dbn::schema schema, bool ts_out);

// Writes DBN records of one schema to one Parquet file. append() fills a
// batch on the calling thread; a full batch goes through a bounded SPSC queue
// to a writer thread, so decoding and Parquet encoding overlap. The writer
// thread encodes and compresses the columns of a batch in parallel on Arrow's
// CPU thread pool. close() must be called to finish the file; a writer that
// is destroyed without close() leaves the file incomplete.
//
// Thread contract: append() and close() from one thread only.
class ParquetWriter {
 public:
  ParquetWriter(const std::filesystem::path& path, const dbn::metadata& md,
                std::int64_t batch_rows);
  ~ParquetWriter();

  ParquetWriter(const ParquetWriter&) = delete;
  ParquetWriter& operator=(const ParquetWriter&) = delete;

  // Appends one framed record. Throws if the writer thread has failed.
  void append(std::span<const std::byte> record);

  // Flushes the open batch, writes the footer, and closes the file. Rethrows
  // any writer-thread failure. Returns the rows written.
  std::uint64_t close();

  std::uint64_t rows_skipped() const { return skipped_; }

  // Rough peak memory of converting one file at `batch_rows`: the batch being
  // built, the batches queued and encoding, and the decoder's buffers.
  static std::uint64_t peak_memory(std::int64_t batch_rows);

 private:
  void flush();  // hands the open batch to the writer thread
  void enqueue(std::shared_ptr<arrow::RecordBatch> batch);
  void writer_loop();

  std::int64_t batch_rows_;
  std::unique_ptr<batch_builder> builder_;
  std::shared_ptr<arrow::io::FileOutputStream> sink_;
  std::unique_ptr<parquet::arrow::FileWriter> writer_;

  nlib::single_queue<std::shared_ptr<arrow::RecordBatch>> queue_;
  std::thread thread_;
  std::atomic<bool> failed_{false};
  arrow::Status thread_status_;  // written by the writer thread before failed_ is set
  std::uint64_t written_ = 0;    // writer thread only, until join
  std::uint64_t skipped_ = 0;
  bool closed_ = false;
};

}  // namespace nplayer
