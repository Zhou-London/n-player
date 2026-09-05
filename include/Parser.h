#pragma once

// Read data from 'input', parse it and convert to parquet, then output to 'output'

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <span>
#include <vector>

#include <zstd.h>

#include "common.h"

namespace nplayer {

// Streaming decoder of one zstd-compressed DBN file. Decompresses in chunks
// and yields records one at a time, so memory stays near one zstd window
// whatever the file size. Throws std::runtime_error on any malformed input.
class Parser {
 public:
  // Opens `file` and decodes its metadata header.
  explicit Parser(const std::filesystem::path& file);
  ~Parser();

  Parser(const Parser&) = delete;
  Parser& operator=(const Parser&) = delete;

  const dbn::metadata& metadata() const { return metadata_; }

  // Yields every record in file order as one complete framed record: the
  // header, the fields, and ts_out when the metadata says records carry one.
  // The span is valid only during the call. Stops after `limit` records when
  // limit is nonzero. Returns the number of records yielded.
  std::uint64_t for_each_record(const std::function<void(std::span<const std::byte>)>& fn,
                                std::uint64_t limit = 0);

 private:
  // Decompresses more of the stream onto the end of buf_. Returns false once
  // the stream is exhausted.
  bool fill();
  void read_metadata();
  [[noreturn]] void fail(const std::string& what) const;

  std::filesystem::path path_;
  std::FILE* file_ = nullptr;
  ZSTD_DStream* dstream_ = nullptr;
  std::vector<char> in_;  // compressed bytes read from file_
  ZSTD_inBuffer in_buf_{nullptr, 0, 0};
  std::size_t out_chunk_ = 0;  // decompressed bytes requested per fill()
  bool in_frame_ = false;  // a zstd frame is open, so end of file means truncation
  std::vector<std::byte> buf_;  // decompressed bytes; [pos_, size) is unconsumed
  std::size_t pos_ = 0;
  dbn::metadata metadata_;
};

// Converts one .dbn.zst file into the Parquet file at `out`. Writes to a
// temporary file beside `out` and renames it on success, so `out` never holds
// a partial file. Throws on failure.
convert_result convert_file(const std::filesystem::path& in, const std::filesystem::path& out,
                            const options& opt);

// Converts every *.dbn.zst under opt.input, mirroring the directory tree under
// opt.output with the extension replaced by .parquet. Files that fail are
// reported and skipped. Returns the number of failed files.
int convert_tree(const options& opt);

}  // namespace nplayer
