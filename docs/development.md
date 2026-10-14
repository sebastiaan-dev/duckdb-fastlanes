# Building against DuckDB 1.5.6

## Dependencies

- DuckDB submodule: `v1.5.6` (`069cc9f9b5b`)
- extension-ci-tools submodule: `v1.5.6` (`3dc5c2a2754`)
- FastLanes: `sebastiaan-dev/fastlanes-patched` at
  `7f91afdbd916d24ab5b39a990fbdf7d9157c0f69`, plus
  [`patches/fastlanes-build.patch`](../patches/fastlanes-build.patch),
  [`patches/fastlanes-branch-consolidation.patch`](../patches/fastlanes-branch-consolidation.patch),
  [`patches/fastlanes-ci.patch`](../patches/fastlanes-ci.patch),
  [`patches/fastlanes-native-reader.patch`](../patches/fastlanes-native-reader.patch),
  [`patches/fastlanes-portability.patch`](../patches/fastlanes-portability.patch), and
  [`patches/fastlanes-operand-safety.patch`](../patches/fastlanes-operand-safety.patch).

Use Clang (13 or newer), CMake (3.22 or newer), Ninja, Git and Python 3.
The pinned FastLanes version requires C++20 and Clang; GCC is not supported by
this patch stack. Local validation was on macOS arm64 with Apple Clang 21.
The CI matrix covers Linux, Windows and both macOS architectures. WASM is not
validated by this migration.

## Reproducible build

```sh
git submodule update --init --recursive
CC=clang CXX=clang++ CMAKE_BUILD_PARALLEL_LEVEL=10 make release GEN=ninja
make test
```

CMake downloads a SHA-256-verified archive of the pinned FastLanes revision and
applies the checked-in patch series. It does not follow a moving `dev` branch.
The build patch respects the embedding project's compiler/toolchain and fixes the
`TypedStats<std::string>` null-pointer construction caught by current Clang.
The consolidation patch restores writer option forwarding and safe deferred
positional I/O; see [the branch audit](branch-consolidation.md).
The CI patch makes writer option forwarding compile with Clang 18, supplies the
required `<utility>` include, and uses the supported Windows environment API.
The native-reader patch restores FSST/FSST12 scratch allocation, retains the
connection's table after serialization, avoids duplicate vector padding, and
adds native round-trip regressions. The portability patch supplies explicit
includes for static analysis, initializes nested statistics, selects physical
codecs without losing DATE/TIMESTAMP/DECIMAL metadata, and supports Windows CRT
descriptors and clang-cl. Windows serializes each 64-bit seek/read
sequence per file; POSIX keeps concurrent `pread` calls. The operand-safety patch
handles omitted FlatBuffers operand vectors, uses secure Windows descriptor
opening, and adds native ALP/ALP-RD round-trip checks.

CI builds and tests Linux x86-64/arm64, macOS x86-64/arm64, and Windows x86-64
explicitly with Clang. Linux builds run in manylinux 2.28 containers; the generic
DuckDB distribution workflow selects incompatible GCC/MSVC toolchains for this
library. Release-tag metadata is verified before building. Formatting uses
clang-format 11 and static analysis uses clang-tidy 18; neither check is disabled.
On Windows/Clang, `cmake/patch_duckdb.cmake` also removes `DUCKDB_API` from two
**deleted** DuckDB 1.5.6 overloads: they have no exported symbols, and Clang
rejects the `dllexport` annotation. This idempotent source-only fix leaves the
DuckDB submodule revision and runtime ABI unchanged.

Outputs:

- CLI with FastLanes linked in: `build/release/duckdb`
- Loadable extension: `build/release/extension/fastlanes/fastlanes.duckdb_extension`
- Test runner: `build/release/test/unittest`

The loadable extension must be loaded into **DuckDB 1.5.6**, not another DuckDB
version. Locally built extensions are unsigned, so use `duckdb -unsigned` when
loading the binary into a separate CLI.

## Paired Worktrunk workspaces

The repositories and worktrees are siblings under `~/repos`:

| Repository | Worktree | Branch |
| --- | --- | --- |
| `FastLanes` | `FastLanes.fix-duckdb-1.5.6` | `fix/duckdb-1.5.6` |
| `duckdb-fastlanes` | `duckdb-fastlanes.fix-duckdb-1.5.6` | `fix/duckdb-1.5.6` |

```sh
wt -C ~/repos/FastLanes switch fix/duckdb-1.5.6
wt -C ~/repos/duckdb-fastlanes switch fix/duckdb-1.5.6

cd ~/repos/duckdb-fastlanes.fix-duckdb-1.5.6
CC=clang CXX=clang++ CMAKE_BUILD_PARALLEL_LEVEL=10 make release GEN=ninja \
  FASTLANES_SOURCE_DIR=../FastLanes.fix-duckdb-1.5.6
make test
```

`FASTLANES_SOURCE_DIR` maps to CMake's `FETCHCONTENT_SOURCE_DIR_FLS_REPO`.
With this override, CMake uses the worktree **as-is** and does not download or
patch it. The FastLanes worktree already contains the same fixes and features.
To return to the downloaded source in an existing build directory, pass
`EXT_FLAGS=-DFETCHCONTENT_SOURCE_DIR_FLS_REPO=` (CMake caches this override).

## Patch provenance

The older branches in `sebastiaan-dev/FastLanes` contain the lambda writer,
parallel/out-of-order row-group writer, CMake overrides and positional-read
experiments. The extension's current main branch already depends on a newer,
consolidated stack in the separate `fastlanes-patched` repository:

- `6c48943`: projection support
- `55c5e40`: minimum statistics metadata
- `01aa52a`: preserve DATE schema information
- `5d60b2e`, `24732f1`: owned spans and vector removal
- `4cddc20`, `8d69b15`, `8981180`: multithreaded/positional I/O and buffer pooling
- `f13af16`: writer and larger row groups
- `7a87f06`, `64b0341`, `7f91afd`: table-API integration and buffer-pool completion

The FastLanes worktree starts from that consolidated revision, imported via the
`patched` remote; its `origin` still points to `sebastiaan-dev/FastLanes`, and
`upstream` points to `cwida/FastLanes`. This preserves the extension's required
patches without mixing in incompatible intermediate experiments. It is **not**
a rebase onto the latest `cwida/FastLanes/dev`. Subsequent consolidation restores
useful missing work from `update` and `task/ifstream-to-pread` and reconciles
superseded branch histories; see [the branch audit](branch-consolidation.md).

## Migration and tests

The 1.5.6 adaptation uses `AsyncResult` for scans and reusable dictionaries for
string materialization. Dictionary selection buffers are owned per output
chunk. Per-file filter/decoder state is reset when MultiFileFunction reuses a
worker for a different reader, while filter references keep their reader alive.
It also fixes reader counters/bind-info access, string dictionary offsets,
and adds the missing BOOLEAN bitmap materializer exposed by the existing tests.

`make test` runs the existing single-/multi-threaded type tests and portable
regressions for projections, count-only scans, filters, partial vectors, virtual
row numbers, multiple files, union-by-name, string dictionaries, BOOLEAN values,
and ordered/unordered parallel COPY over multiple row groups.

Validation on macOS arm64:

- Full Release build and all **175 assertions in 4 test cases** pass with the
  paired FastLanes worktree and independently with the downloaded/patched source.
- The SQL regressions passed 20 consecutive runs after the multi-file state fix.
- Loading into the official DuckDB 1.5.6 CLI succeeds; a 200,003-row parallel
  round trip and BOOLEAN filter also pass.

This is build/API compatibility and regression coverage, not a claim of complete
format support. Existing limitations such as general NULL/nested-type support
and full logical-type preservation remain outside this migration.
