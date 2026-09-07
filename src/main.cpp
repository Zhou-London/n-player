// nplayer: command-line front end. Parses the arguments with CLI11 and calls
// the internal interface of the chosen subcommand.

#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include "Parser.h"
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

}  // namespace

int main(int argc, char** argv) {
  CLI::App app{"Databento DBN tools"};
  app.require_subcommand(1);

  nplayer::options opt;
  int rc = 0;
  add_convert(app, opt, rc);

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
