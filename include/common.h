#pragma once

// Project-specific structures: the Databento Binary Encoding (DBN) wire layout
// this tool decodes, and the settings of one conversion run.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace nplayer {

// DBN as the dbn crate (github.com/databento/dbn) defines it, versions 1 to 3.
// A file is a metadata header followed by fixed-layout little-endian records.
// Every record starts with `record_header`, whose first byte holds the record
// length in 32-bit words.
namespace dbn {

inline constexpr std::uint8_t max_version = 3;

// Metadata header: "DBN", version u8, then the u32 byte length of the rest.
inline constexpr std::size_t prelude_len = 8;
inline constexpr std::size_t dataset_cstr_len = 16;
inline constexpr std::size_t fixed_len = 100;  // bytes before the variable-length symbol lists
inline constexpr std::size_t reserved_len_v1 = 47;
inline constexpr std::size_t reserved_len = 53;  // v2 and v3
inline constexpr std::size_t symbol_cstr_len_v1 = 22;  // v2+ store the length in the header

inline constexpr std::uint16_t null_schema = 0xFFFF;  // file mixes schemas
inline constexpr std::uint8_t null_stype = 0xFF;

inline constexpr std::uint64_t undef_timestamp = std::numeric_limits<std::uint64_t>::max();
inline constexpr std::int64_t undef_price = std::numeric_limits<std::int64_t>::max();
inline constexpr std::uint32_t undef_order_size = std::numeric_limits<std::uint32_t>::max();

// Prices are fixed point: one unit is 1e-9 of the quote currency.
inline constexpr std::int64_t price_scale = 1'000'000'000;

enum class schema : std::uint16_t {
  mbo = 0,
  mbp_1 = 1,
  mbp_10 = 2,
  tbbo = 3,
  trades = 4,
  ohlcv_1s = 5,
  ohlcv_1m = 6,
  ohlcv_1h = 7,
  ohlcv_1d = 8,
  definition = 9,
  statistics = 10,
  status = 11,
  imbalance = 12,
  ohlcv_eod = 13,
  cmbp_1 = 14,
  cbbo_1s = 15,
  cbbo_1m = 16,
  tcbbo = 17,
  bbo_1s = 18,
  bbo_1m = 19,
};

// Symbology type of the request (`stype_in`) and of the records (`stype_out`).
enum class stype : std::uint8_t {
  instrument_id = 0,
  raw_symbol = 1,
  smart = 2,
  continuous = 3,
  parent = 4,
  nasdaq_symbol = 5,
  cms_symbol = 6,
  isin = 7,
  us_code = 8,
  bbg_comp_id = 9,
  bbg_comp_ticker = 10,
  figi = 11,
  figi_ticker = 12,
  listing_id = 13,
  issuer_id = 14,
  security_id = 15,
};

// Record type byte of `record_header`. 0x00 to 0x0F are MBP records whose
// value is the book depth; trades are MBP with depth 0.
namespace rtype {
inline constexpr std::uint8_t mbp_0 = 0x00;
inline constexpr std::uint8_t mbp_1 = 0x01;
inline constexpr std::uint8_t mbp_10 = 0x0A;
inline constexpr std::uint8_t status = 0x12;
inline constexpr std::uint8_t instrument_def = 0x13;
inline constexpr std::uint8_t imbalance = 0x14;
inline constexpr std::uint8_t error = 0x15;
inline constexpr std::uint8_t symbol_mapping = 0x16;
inline constexpr std::uint8_t system = 0x17;
inline constexpr std::uint8_t statistics = 0x18;
inline constexpr std::uint8_t mbo = 0xA0;
}  // namespace rtype

// `action` byte of a book record: 'A' add, 'C' cancel, 'M' modify, 'R' clear
// the book, 'T' trade, 'F' fill, 'N' none. `side`: 'A' ask, 'B' bid, 'N' none.

// `flags` bits of a record.
namespace flags {
inline constexpr std::uint8_t last = 1 << 7;        // last record of the event
inline constexpr std::uint8_t tob = 1 << 6;         // top-of-book record
inline constexpr std::uint8_t snapshot = 1 << 5;    // from a book snapshot, not the live feed
inline constexpr std::uint8_t mbp = 1 << 4;         // aggregated price level
inline constexpr std::uint8_t bad_ts_recv = 1 << 3;
inline constexpr std::uint8_t maybe_bad_book = 1 << 2;
inline constexpr std::uint8_t publisher_specific = 1 << 1;
}  // namespace flags

struct record_header {
  std::uint8_t length;  // record length in 32-bit words, ts_out included
  std::uint8_t rtype;
  std::uint16_t publisher_id;
  std::uint32_t instrument_id;
  std::uint64_t ts_event;  // matching-engine time, Unix-epoch nanoseconds
};

// Market-by-order record: one order event. Schema `mbo`, rtype 0xA0.
struct mbo_msg {
  record_header hd;
  std::uint64_t order_id;
  std::int64_t price;  // 1/price_scale units; undef_price when absent
  std::uint32_t size;  // undef_order_size when absent
  std::uint8_t flags;
  std::uint8_t channel_id;
  char action;
  char side;
  std::uint64_t ts_recv;  // capture-server receive time, Unix-epoch nanoseconds
  std::int32_t ts_in_delta;  // ts_recv minus the gateway send time, nanoseconds
  std::uint32_t sequence;  // venue sequence number
};

// Trade record. Schema `trades`, rtype 0x00.
struct trade_msg {
  record_header hd;
  std::int64_t price;
  std::uint32_t size;
  char action;  // always 'T'
  char side;    // aggressor side
  std::uint8_t flags;
  std::uint8_t depth;  // book level the trade hit
  std::uint64_t ts_recv;
  std::int32_t ts_in_delta;
  std::uint32_t sequence;
};

// Records are read by memcpy from the stream, so their layout must match the
// wire byte for byte.
static_assert(std::is_trivially_copyable_v<record_header> && sizeof(record_header) == 16);
static_assert(std::is_trivially_copyable_v<mbo_msg> && sizeof(mbo_msg) == 56);
static_assert(std::is_trivially_copyable_v<trade_msg> && sizeof(trade_msg) == 48);

// A record with `ts_out` carries the live-gateway send time after its fields.
inline constexpr std::size_t ts_out_len = sizeof(std::uint64_t);

// Dates are YYYYMMDD integers, as in the header.
struct mapping_interval {
  std::uint32_t start_date;
  std::uint32_t end_date;  // exclusive
  std::string symbol;
};

// Maps one requested symbol to the symbols the records use, per date range.
struct symbol_mapping {
  std::string raw_symbol;
  std::vector<mapping_interval> intervals;
};

// Decoded metadata header of one DBN file.
struct metadata {
  std::uint8_t version = 0;
  std::string dataset;
  // Empty when the file mixes schemas. Qualified: the member name hides the enum.
  std::optional<dbn::schema> schema;
  std::uint64_t start = 0;  // query range, Unix-epoch nanoseconds
  std::optional<std::uint64_t> end;
  std::optional<std::uint64_t> limit;  // record cap of the query
  std::optional<stype> stype_in;
  stype stype_out = stype::instrument_id;
  bool ts_out = false;  // every record carries a trailing ts_out
  std::size_t symbol_cstr_len = 0;
  std::vector<std::string> symbols;
  std::vector<std::string> partial;
  std::vector<std::string> not_found;
  std::vector<symbol_mapping> mappings;
};

constexpr std::string_view schema_name(schema s) {
  switch (s) {
    case schema::mbo: return "mbo";
    case schema::mbp_1: return "mbp-1";
    case schema::mbp_10: return "mbp-10";
    case schema::tbbo: return "tbbo";
    case schema::trades: return "trades";
    case schema::ohlcv_1s: return "ohlcv-1s";
    case schema::ohlcv_1m: return "ohlcv-1m";
    case schema::ohlcv_1h: return "ohlcv-1h";
    case schema::ohlcv_1d: return "ohlcv-1d";
    case schema::definition: return "definition";
    case schema::statistics: return "statistics";
    case schema::status: return "status";
    case schema::imbalance: return "imbalance";
    case schema::ohlcv_eod: return "ohlcv-eod";
    case schema::cmbp_1: return "cmbp-1";
    case schema::cbbo_1s: return "cbbo-1s";
    case schema::cbbo_1m: return "cbbo-1m";
    case schema::tcbbo: return "tcbbo";
    case schema::bbo_1s: return "bbo-1s";
    case schema::bbo_1m: return "bbo-1m";
  }
  return "unknown";
}

constexpr std::string_view stype_name(stype s) {
  switch (s) {
    case stype::instrument_id: return "instrument_id";
    case stype::raw_symbol: return "raw_symbol";
    case stype::smart: return "smart";
    case stype::continuous: return "continuous";
    case stype::parent: return "parent";
    case stype::nasdaq_symbol: return "nasdaq_symbol";
    case stype::cms_symbol: return "cms_symbol";
    case stype::isin: return "isin";
    case stype::us_code: return "us_code";
    case stype::bbg_comp_id: return "bbg_comp_id";
    case stype::bbg_comp_ticker: return "bbg_comp_ticker";
    case stype::figi: return "figi";
    case stype::figi_ticker: return "figi_ticker";
    case stype::listing_id: return "listing_id";
    case stype::issuer_id: return "issuer_id";
    case stype::security_id: return "security_id";
  }
  return "unknown";
}

}  // namespace dbn

// Settings of one conversion run, as parsed from the command line.
struct options {
  std::filesystem::path input = "data/input";   // searched recursively for *.dbn.zst
  std::filesystem::path output = "data/output"; // mirrors the input tree
  std::uint64_t limit = 0;         // records to convert per file; 0 converts all
  std::int64_t batch_rows = 1 << 20;  // rows per Arrow batch and Parquet row group
  unsigned jobs = 1;               // files converted at the same time
};

// Outcome of converting one file.
struct convert_result {
  std::uint64_t records = 0;  // records decoded
  std::uint64_t rows = 0;     // rows written; lower when records of other schemas were skipped
  double seconds = 0;
};

}  // namespace nplayer
