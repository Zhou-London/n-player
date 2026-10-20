#pragma once

// Backplays historical data through ZMQ. Receives an Arrow IPC stream over a
// TCP connection, buffers the record batches in an nlib::single_queue, and
// encodes them into nlib::order / nlib::trade records for ZMQ to publish.
// The ZMQ side is the next step; the player thread drops the batches for now.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include <arrow/api.h>
#include <nlib/single_queue.h>

#include "common.h"

namespace nplayer {

// Outcome of playing one stream.
struct backplay_result {
  std::shared_ptr<arrow::Schema> schema;  // null when the sender closed before its schema
  std::uint64_t batches = 0;
  std::uint64_t rows = 0;
  double seconds = 0;  // from the first byte received to end of stream
};

// Serves one Arrow IPC stream from a TCP connection. By default run()
// connects to the sender at host:port; with `listen` the constructor binds
// host:port and run() accepts one connection. run() decodes the stream on
// the calling thread and prints a line on connection and every ten million
// rows. Each decoded batch goes through a bounded SPSC queue to
// the player thread, which drops the batch after counting its rows. The
// stream ends at the IPC end-of-stream marker or when the sender closes at a
// message boundary; a close inside a message is an error.
//
// Thread contract: run() from one thread, once.
class Backplayer {
 public:
  // With `listen`, binds the socket. Throws std::runtime_error when the host
  // does not resolve or the bind fails.
  explicit Backplayer(const backplay_options& opt);
  ~Backplayer();

  Backplayer(const Backplayer&) = delete;
  Backplayer& operator=(const Backplayer&) = delete;

  // Connects to the sender, or accepts one connection, serves the stream, and
  // returns the counts. Throws std::runtime_error on a socket failure or a
  // malformed stream.
  backplay_result run();

 private:
  class listener;  // receives the decoder's callbacks and calls enqueue()

  // Binds and listens on host:port. Returns the listening descriptor.
  int listen_socket() const;
  // Connects to host:port. Returns the connected descriptor.
  int connect_socket() const;
  // Reads the stream from `fd` into the decoder until end of stream. `t0` is
  // the connection time, for the progress lines.
  void decode(int fd, backplay_result& result, std::chrono::steady_clock::time_point t0);
  void enqueue(std::shared_ptr<arrow::RecordBatch> batch);
  void player_loop();

  backplay_options opt_;
  int listen_fd_ = -1;
  nlib::single_queue<std::shared_ptr<arrow::RecordBatch>> queue_;
  std::thread thread_;
  std::uint64_t played_rows_ = 0;  // player thread only, until join
  std::uint64_t played_batches_ = 0;
};

}  // namespace nplayer
