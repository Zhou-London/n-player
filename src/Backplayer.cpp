#include "Backplayer.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <arrow/ipc/reader.h>
#include <fmt/core.h>

namespace nplayer {

namespace {

using namespace std::chrono_literals;

// Poll interval of the two queue ends when the queue is full or empty.
constexpr auto queue_wait = 200us;

// Rows between progress lines.
constexpr std::uint64_t log_every_rows = 10'000'000;

double seconds_since(std::chrono::steady_clock::time_point t0) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}

void check(const arrow::Status& st) {
  if (!st.ok()) throw std::runtime_error(st.ToString());
}

template <typename T>
T unwrap(arrow::Result<T> result) {
  check(result.status());
  return std::move(result).ValueUnsafe();
}

[[noreturn]] void fail_errno(const std::string& what) {
  throw std::runtime_error(fmt::format("{}: {}", what, std::strerror(errno)));
}

// Closes a descriptor when the scope ends.
struct fd_guard {
  int fd;
  ~fd_guard() {
    if (fd >= 0) ::close(fd);
  }
};

struct addrinfo_free {
  void operator()(addrinfo* p) const { ::freeaddrinfo(p); }
};

std::string endpoint(const backplay_options& opt) {
  return fmt::format("{}:{}", opt.host, opt.port);
}

// Numeric ip:port of the peer of `fd`, or "?" when the socket has none.
std::string peer_name(int fd) {
  sockaddr_storage addr{};
  socklen_t len = sizeof(addr);
  if (::getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) return "?";
  char host[NI_MAXHOST];
  char port[NI_MAXSERV];
  if (::getnameinfo(reinterpret_cast<sockaddr*>(&addr), len, host, sizeof(host), port,
                    sizeof(port), NI_NUMERICHOST | NI_NUMERICSERV) != 0)
    return "?";
  return fmt::format("{}:{}", host, port);
}

// Resolves host:port to IPv4 stream addresses. `passive` asks for addresses
// to bind rather than to connect to. Throws when the host does not resolve.
std::unique_ptr<addrinfo, addrinfo_free> resolve(const backplay_options& opt, bool passive) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = passive ? AI_PASSIVE : 0;
  addrinfo* out = nullptr;
  const int rc = ::getaddrinfo(opt.host.c_str(), std::to_string(opt.port).c_str(), &hints, &out);
  if (rc != 0)
    throw std::runtime_error(fmt::format("resolve {}: {}", endpoint(opt), ::gai_strerror(rc)));
  return std::unique_ptr<addrinfo, addrinfo_free>(out);
}

// Reads exactly `size` bytes into `dst`. Returns the bytes read: `size`, or
// fewer when the peer closed first.
std::int64_t read_fully(int fd, std::uint8_t* dst, std::int64_t size) {
  std::int64_t done = 0;
  while (done < size) {
    const ssize_t n = ::read(fd, dst + done, static_cast<std::size_t>(size - done));
    if (n > 0) {
      done += n;
    } else if (n == 0) {
      break;
    } else if (errno != EINTR) {
      fail_errno("read from socket");
    }
  }
  return done;
}

}  // namespace

// Decoder callbacks. The decoder runs on the run() thread, so every callback
// is on the queue's producer side. Prints a progress line each time the row
// count passes a multiple of log_every_rows.
class Backplayer::listener : public arrow::ipc::Listener {
 public:
  listener(Backplayer& owner, backplay_result& result, std::chrono::steady_clock::time_point t0)
      : owner_(owner), result_(result), t0_(t0) {}

  bool eos() const { return eos_; }

  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema) override {
    result_.schema = std::move(schema);
    return arrow::Status::OK();
  }

  arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) override {
    ++result_.batches;
    result_.rows += static_cast<std::uint64_t>(batch->num_rows());
    if (result_.rows >= next_log_) {
      fmt::print("{} rows, {} batches, {:.1f}s\n", result_.rows, result_.batches,
                 seconds_since(t0_));
      next_log_ = (result_.rows / log_every_rows + 1) * log_every_rows;
    }
    owner_.enqueue(std::move(batch));
    return arrow::Status::OK();
  }

  arrow::Status OnEOS() override {
    eos_ = true;
    return arrow::Status::OK();
  }

 private:
  Backplayer& owner_;
  backplay_result& result_;
  std::chrono::steady_clock::time_point t0_;
  std::uint64_t next_log_ = log_every_rows;
  bool eos_ = false;
};

Backplayer::Backplayer(const backplay_options& opt) : opt_(opt), queue_(opt.queue_depth) {
  if (opt.queue_depth == 0) throw std::runtime_error("queue_depth must be positive");
  if (opt_.listen) listen_fd_ = listen_socket();
}

Backplayer::~Backplayer() {
  if (thread_.joinable()) {
    enqueue(nullptr);
    thread_.join();
  }
  if (listen_fd_ >= 0) ::close(listen_fd_);
}

// SO_REUSEADDR lets a rerun bind while the previous connection sits in
// TIME_WAIT.
int Backplayer::listen_socket() const {
  const auto ai = resolve(opt_, true);
  fd_guard fd{::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol)};
  if (fd.fd < 0) fail_errno("socket");
  const int one = 1;
  if (::setsockopt(fd.fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
    fail_errno("setsockopt SO_REUSEADDR");
  if (::bind(fd.fd, ai->ai_addr, ai->ai_addrlen) < 0) fail_errno("bind " + endpoint(opt_));
  if (::listen(fd.fd, 1) < 0) fail_errno("listen");
  return std::exchange(fd.fd, -1);
}

// Tries every resolved address in order; reports the last failure.
int Backplayer::connect_socket() const {
  const auto list = resolve(opt_, false);
  int last_errno = 0;
  for (const addrinfo* ai = list.get(); ai != nullptr; ai = ai->ai_next) {
    fd_guard fd{::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol)};
    if (fd.fd < 0) {
      last_errno = errno;
      continue;
    }
    if (::connect(fd.fd, ai->ai_addr, ai->ai_addrlen) == 0) return std::exchange(fd.fd, -1);
    last_errno = errno;
  }
  errno = last_errno;
  fail_errno("connect " + endpoint(opt_));
}

backplay_result Backplayer::run() {
  if (thread_.joinable()) throw std::runtime_error("run() called twice");
  backplay_result result;
  thread_ = std::thread([this] { player_loop(); });

  fd_guard conn{opt_.listen ? ::accept(listen_fd_, nullptr, nullptr) : connect_socket()};
  if (conn.fd < 0) fail_errno("accept");
  fmt::print("{} {}\n", opt_.listen ? "connection from" : "connected to", peer_name(conn.fd));
  const auto t0 = std::chrono::steady_clock::now();
  decode(conn.fd, result, t0);
  result.seconds = seconds_since(t0);

  enqueue(nullptr);
  thread_.join();
  return result;
}

// Feeds the decoder exactly the bytes its next step needs, so no byte is
// copied into the decoder's own buffer. A batch's buffers alias the memory
// read here. A step can need zero bytes: a message with an empty body, such
// as the schema, still takes one Consume() call to advance the decoder.
void Backplayer::decode(int fd, backplay_result& result,
                        std::chrono::steady_clock::time_point t0) {
  auto callbacks = std::make_shared<listener>(*this, result, t0);
  arrow::ipc::StreamDecoder decoder(callbacks);
  while (!callbacks->eos()) {
    const std::int64_t need = decoder.next_required_size();
    std::shared_ptr<arrow::Buffer> buf = unwrap(arrow::AllocateBuffer(need));
    const std::int64_t got = read_fully(fd, buf->mutable_data(), need);
    if (got == 0 && need > 0) return;  // closed at a message boundary
    if (got < need)
      throw std::runtime_error(
          fmt::format("stream truncated: sender closed {} bytes into a {}-byte read", got, need));
    check(decoder.Consume(std::move(buf)));
  }
}

void Backplayer::enqueue(std::shared_ptr<arrow::RecordBatch> batch) {
  while (!queue_.try_push(std::move(batch))) std::this_thread::sleep_for(queue_wait);
}

// Player thread. Pops each batch, counts its rows, and drops it. Encoding
// into nlib records and publishing over ZMQ go here next.
void Backplayer::player_loop() {
  while (true) {
    auto item = queue_.try_pop();
    if (!item) {
      std::this_thread::sleep_for(queue_wait);
      continue;
    }
    if (*item == nullptr) return;
    ++played_batches_;
    played_rows_ += static_cast<std::uint64_t>((*item)->num_rows());
  }
}

}  // namespace nplayer
