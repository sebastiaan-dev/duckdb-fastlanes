# Branch consolidation audit

The consolidation target is `fix/duckdb-1.5.6`. The repository origin now uses
`git@github.com:sebastiaan-dev/duckdb-fastlanes.git` locally. Public submodule and
FetchContent download URLs are intentionally unchanged.

| Branch | Original tip | Decision |
| --- | --- | --- |
| `dev` | `57c0a4f` | Its entire `src/` tree matches origin/main. Main includes this work through squash commit `1be8b5f` (#43); the only remaining differences are obsolete CMake comments and README content. Reconcile history without replacing the 1.5.6 code. |
| `bench` | `d7408c6` | Ancestor of dev. Its benchmark scripts were deliberately removed in `472b6bf`, and the FSST changes after bench are already present. Do not resurrect the removed harness. |
| `main` | `c6b6bcd` | Retain the default branch and existing main worktree unchanged. |

A history-only merge of dev makes both dev and bench ancestors of the current
branch, while preserving the tested source tree. This is deliberate reconciliation
of squash-merged work, not a replacement of the current implementation with the
old branch contents.

## FastLanes feature additions

The paired fork audit identified two useful missing features:

- Forward Connection footer, sample size and forced encoding options into the
  writer, including the WithSampleSize builder method (`ad34adf` from update).
  Keep the declared physical types when an encoding is forced, rather than
  narrowing the column before applying its codec.
- Defer read-only file opening while retaining thread-safe positional reads
  (adapted from task/ifstream-to-pread). Also handle I/O failure and EOF in Release
  instead of relying on disabled assertions.

They are included in `patches/fastlanes-branch-consolidation.patch`, on top of
`fastlanes-build.patch`. CMake fingerprints the patch series so existing downloaded
source builds run the patch step again when it changes. The patch helper accepts
an already-applied patch, including upgrading the previous build-patch-only source.
Local FastLanes overrides remain untouched by patch application.

The fork's `docs/branch-consolidation.md` contains the complete branch audit and
the six standalone regression cases. Build those through this extension with:

```sh
CMAKE_BUILD_PARALLEL_LEVEL=8 make release GEN=ninja \
  FASTLANES_SOURCE_DIR=../FastLanes.fix-duckdb-1.5.6 \
  EXT_FLAGS=-DFLS_BUILD_STANDALONE_TESTS=ON
build/release/_deps/fls_repo-build/test/standalone/fastlanes_integration_test
make test
```

## Recovery and deletion status

Original branch tips are preserved locally and on GitHub as annotated tags under
`archive/branch-consolidation/origin/<branch>`. The previous integration commit is
`archive/branch-consolidation/current`. A verified full Git bundle is stored at
`~/repos/duckdb-fastlanes/.git/branch-archives/pre-consolidation.bundle`.

The consolidated `fix/duckdb-1.5.6` branch and all archive tags are published.
After verifying the remote archives, `dev` and `bench` were deleted using an
atomic push with exact-tip leases. Local remote-tracking refs were pruned.
`main` and the repository default branch remain unchanged. There were no
redundant local feature branches to delete, only main and the integration
worktree branch.
