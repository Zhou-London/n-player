// nplayer: command-line front end. Parses the arguments with CLI11 and calls
// the internal interface of the chosen subcommand.

#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include "Backplayer.h"
#include "DbnParser.h"
#include "common.h"

namespace {

// Registers `convert`: every *.dbn.zst under --input becomes a Parquet file
// under --output. Returns the exit code through `rc` when run.
void add_convert(CLI::App& app, nplayer::options& opt, int& rc) {
  CLI::App* cmd = app.add_subcommand(
      "convert",
      "Convert every *.dbn.zst under the input directory to Parquet, mirroring the tree "
      "under the output directory. Other files are ignored; the output keeps the input "
      "name with .dbn.zst replaced by .parquet.");
  cmd->add_option("-i,--input", opt.input, "Directory searched recursively")
      ->capture_default_str()
      ->check(CLI::ExistingDirectory);
  cmd->add_option("-o,--output", opt.output, "Directory written to")->capture_default_str();
  cmd->add_option("-l,--limit", opt.limit, "Convert only the first N records of each file")
      ->type_name("N");
  cmd->add_option("-b,--batch-rows", opt.batch_rows, "Rows per Parquet row group")
      ->type_name("N")
      ->capture_default_str()
      ->check(CLI::Range(std::int64_t{1}, std::numeric_limits<std::int64_t>::max()));
  cmd->add_option("-j,--jobs", opt.jobs,
                  "Files converted at the same time; 0 picks a count from the CPU cores and "
                  "the available memory")
      ->type_name("N")
      ->capture_default_str();
  cmd->callback([&] { rc = nplayer::convert_tree(opt) == 0 ? 0 : 1; });
}

// Registers `backplay`: connects to a sender (or listens for one), receives
// one Arrow IPC stream over TCP, and reports what arrived.
void add_backplay(CLI::App& app, nplayer::backplay_options& opt, int& rc) {
  CLI::App* cmd = app.add_subcommand(
      "backplay",
      "Connect to the sender over TCP, receive one Arrow IPC stream, and play it back. "
      "The command returns when the sender ends the stream.");
  cmd->add_option("-H,--host", opt.host, "Sender to connect to; with --listen, the address to bind")
      ->capture_default_str();
  cmd->add_option("-p,--port", opt.port, "TCP port")->type_name("N")->capture_default_str();
  cmd->add_flag("-l,--listen", opt.listen, "Listen on host:port and wait for the sender to connect");
  cmd->add_option("-q,--queue-depth", opt.queue_depth,
                  "Record batches buffered between the socket reader and the player")
      ->type_name("N")
      ->capture_default_str()
      ->check(CLI::Range(std::size_t{1}, std::numeric_limits<std::size_t>::max()));
  cmd->callback([&] {
    nplayer::Backplayer player(opt);
    fmt::print("{} {}:{}\n", opt.listen ? "listening on" : "connecting to", opt.host, opt.port);
    const nplayer::backplay_result r = player.run();
    fmt::print("received {} batches, {} rows in {:.3f} s\n", r.batches, r.rows, r.seconds);
    if (r.schema) fmt::print("schema:\n{}\n", r.schema->ToString());
    rc = 0;
  });
}

}  // namespace

int main(int argc, char** argv) {
  CLI::App app{"Databento DBN tools"};
  app.require_subcommand(1);

  nplayer::options opt;
  int rc = 0;
  add_convert(app, opt, rc);
  nplayer::backplay_options backplay_opt;
  add_backplay(app, backplay_opt, rc);

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);  // prints help or the usage error; 0 for --help
  } catch (const std::exception& e) {
    fmt::print(stderr, "error: {}\n", e.what());
    return 1;
  }
  return rc;
}
