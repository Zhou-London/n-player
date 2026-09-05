<img src="https://capsule-render.vercel.app/api?type=waving&height=400&text=N-Player&fontAlign=80&fontAlignY=40&color=gradient" />

<p align="center">
  <img alt="Version 0.1.0" src="https://img.shields.io/badge/version-0.1.0-blue" />
  <img alt="C++23" src="https://img.shields.io/badge/C%2B%2B-23-00599C?logo=cplusplus&logoColor=white" />
  <img alt="CMake 3.28+" src="https://img.shields.io/badge/CMake-3.28%2B-064F8C?logo=cmake&logoColor=white" />
  <img alt="Apache Arrow / Parquet" src="https://img.shields.io/badge/Apache-Arrow%20%2F%20Parquet-1F6FEB" />
  <img alt="Depends on nlib" src="https://img.shields.io/badge/submodule-nlib-4c1?logo=git&logoColor=white" />
  <img alt="Depends on CLI11" src="https://img.shields.io/badge/submodule-CLI11-4c1?logo=git&logoColor=white" />
</p>

Project of NowQuant.

Converts Databento Binary Encoding files (`*.dbn.zst`) to Parquet.

The tool walks `data/input`, decodes every `.dbn.zst` it finds, and writes a
Parquet file with the same name under `data/output`, mirroring the directory
tree. Other files (`metadata.json`, `manifest.json`, ...) are ignored.

```
data/input/GLBX-.../glbx-mdp3-20260804.mbo.dbn.zst
  -> data/output/GLBX-.../glbx-mdp3-20260804.mbo.parquet
```

## Build and run

Everything builds and runs in the `dev` container image, which carries GCC 13,
CMake, Apache Arrow / Parquet 25, zstd, and fmt. The repository is mounted at
`/work`.

```bash
git submodule update --init   # third_party/nlib, third_party/CLI11
make build                    # cmake configure + build into build/
make run                      # nplayer convert: data/input -> data/output
make run ARGS="convert --limit 1000000 --jobs 2"
```

`make shell` opens a shell in the container. Without make:

```bash
docker run --rm -v "$PWD:/work" -w /work dev \
  bash -c 'cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build build && ./build/nplayer convert'
```

## Commands

The command line is parsed by [CLI11](https://github.com/CLIUtils/CLI11)
(header-only, submodule in `third_party/CLI11`); `nplayer --help` and
`nplayer <command> --help` list the options with their defaults.

### `convert`

```
-i, --input DIR       directory searched recursively (default: data/input)
-o, --output DIR      directory written to (default: data/output)
-l, --limit N         convert only the first N records of each file
-b, --batch-rows N    rows per Parquet row group (default: 1048576)
-j, --jobs N          files converted at the same time (default: 1)
```

Each file is written to `<name>.parquet.tmp` and renamed on success, so an
existing `.parquet` is either complete or absent. A file that fails is
reported on stderr and skipped; the exit code is 1 if any file failed.

## Output

One row per DBN record, in file order, columns in Databento's DataFrame
order. Prices stay fixed point (`int64`, 1e-9 of the quote unit); timestamps
are `timestamp[ns, UTC]`. Undefined sentinels (price `INT64_MAX`, size
`UINT32_MAX`, timestamp `UINT64_MAX`) become nulls.

| Schema | rtype | Columns |
|---|---|---|
| `mbo` | `0xA0` | ts_recv, ts_event, rtype, publisher_id, instrument_id, action, side, price, size, channel_id, order_id, flags, ts_in_delta, sequence |
| `trades` | `0x00` | ts_recv, ts_event, rtype, publisher_id, instrument_id, action, side, depth, price, size, flags, ts_in_delta, sequence |

Files whose metadata carries `ts_out` get a trailing `ts_out` column. Other
schemas are rejected with an error naming the schema.

The Parquet key-value metadata holds the DBN header: `dbn.version`,
`dbn.dataset`, `dbn.schema`, `dbn.start`, `dbn.end`, `dbn.stype_in`,
`dbn.stype_out`, `dbn.symbols`, `dbn.partial`, `dbn.not_found`,
`dbn.mappings` (`raw_symbol=symbol@YYYYMMDD-YYYYMMDD;...`, end exclusive), and
`price_scale`.

Encoding: zstd compression, one row group per `--batch-rows`, timestamps and
`sequence` delta-packed, everything else dictionary-encoded.

## Layout

```
include/common.h          DBN wire layout (header, mbo, trades records), metadata, options
include/Parser.h          streaming zstd + DBN decoder; convert_file / convert_tree
include/ParquetWriter.h   Arrow batch builders and the threaded Parquet writer
third_party/nlib/         git submodule: nlib (single_queue feeds the writer thread)
third_party/CLI11/        git submodule: CLI11, header-only command-line parser
src/main.cpp              CLI11 subcommands, each calling an internal interface
data/                     gitignored: input/, output/, backplay/
```

Decoding and Parquet encoding run on two threads joined by an
`nlib::single_queue` of Arrow record batches, so zstd decompression of the
input overlaps with zstd compression of the output.

## DBN format

Decoded from the layout in the [dbn crate](https://github.com/databento/dbn):
an 8-byte prelude (`DBN`, version, u32 length), a fixed 100-byte header
(dataset, schema, start, end, limit, stype_in, stype_out, ts_out,
symbol_cstr_len), then the symbol lists and mappings, then records. Every
record starts with a 16-byte header whose first byte is the record length in
32-bit words. Versions 1 to 3 are accepted.
