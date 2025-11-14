from __future__ import annotations

import argparse
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "benchmark" / "data" / "tpch"
TPCH_QUERY_DIR = REPO_ROOT / "duckdb" / "extension" / "tpch" / "dbgen" / "queries"
BUILD_DIR = REPO_ROOT / "build" / "release"
DUCKDB_BINARY = BUILD_DIR / "duckdb"
FASTLANES_EXTENSION: Path = (
    REPO_ROOT
    / "build"
    / "release"
    / "extension"
    / "fastlanes"
    / "fastlanes.duckdb_extension"
)
VORTEX_EXTENSION = (
    REPO_ROOT
    / "build"
    / "duckdb-vortex"
    / "build"
    / "release"
    / "extension"
    / "vortex"
    / "vortex.duckdb_extension"
)
TPCH_TABLES: Sequence[str] = (
    "lineitem",
    "orders",
    "customer",
    "nation",
    "region",
    "part",
    "partsupp",
    "supplier",
)
WARMUP_RUNS = 2
TPCH_QUERY_COUNT = 22
DUCKDB_CLI_FLAGS: Sequence[str] = ("-unsigned", "-csv", "-noheader", "-bail")


def resolve_duckdb_binary() -> Path:
    if not DUCKDB_BINARY.exists():
        raise FileNotFoundError(
            f"DuckDB binary not found at {DUCKDB_BINARY}. Build the project with 'make' first."
        )
    if not DUCKDB_BINARY.is_file():
        raise FileNotFoundError(
            f"Expected DuckDB binary at {DUCKDB_BINARY}, but found something else."
        )
    return DUCKDB_BINARY


def sql_literal(value: str | Path) -> str:
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


def run_duckdb_script(
    statements: Sequence[str], database: Optional[Path]
) -> subprocess.CompletedProcess:
    binary = resolve_duckdb_binary()
    args: List[str] = [str(binary)]
    if database is not None:
        if not database.exists():
            raise FileNotFoundError(f"DuckDB database not found at {database}")
        args.append(str(database))
        args.extend(DUCKDB_CLI_FLAGS)
        args.append("-readonly")
    else:
        args.append(":memory:")
        args.extend(DUCKDB_CLI_FLAGS)
    script = "\n".join(statements)
    try:
        return subprocess.run(
            args,
            input=script,
            text=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stdout = exc.stdout.strip()
        stderr = exc.stderr.strip()
        details = stderr or stdout or "unknown error"
        raise RuntimeError(f"DuckDB command failed: {details}") from exc


def extract_result_and_runtime(output: str) -> tuple[str, float]:
    result_lines: List[str] = []
    runtime: float | None = None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("Run Time (s):"):
            match = re.search(r"Run Time \(s\):\s*real\s+([0-9.]+)", line)
            if not match:
                raise RuntimeError(f"Failed to parse runtime from: {line}")
            runtime = float(match.group(1))
        else:
            result_lines.append(line)
    if not result_lines:
        raise RuntimeError("DuckDB did not return a result value")
    if runtime is None:
        raise RuntimeError("DuckDB did not report a runtime (enable .timer?)")
    return "\n".join(result_lines), runtime


def fastlanes_load_statement() -> str:
    return f"LOAD {sql_literal(FASTLANES_EXTENSION)};"


def vortex_load_statement() -> str:
    return f"LOAD {sql_literal(VORTEX_EXTENSION)};"


def base_session_statements(threads: int) -> List[str]:
    return [
        f"PRAGMA threads={threads};",
        "PRAGMA disable_object_cache;",
        "SET enable_external_file_cache = FALSE;",
    ]


@dataclass(frozen=True)
class ScaleInfo:
    original: str
    normalized: str
    slug: str

    @property
    def dataset_name(self) -> str:
        return f"tpch_sf{self.slug}_rg65536"


def parse_scale(value: str) -> ScaleInfo:
    raw = value.replace("_", ".")
    try:
        numeric = Decimal(raw)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"Invalid scale factor '{value}'") from exc
    if numeric <= 0:
        raise argparse.ArgumentTypeError("Scale factor must be positive")
    normalized = format(numeric.normalize(), "f")
    slug = normalized.replace(".", "_")
    return ScaleInfo(original=value, normalized=normalized, slug=slug)


def parse_query_id(value: str) -> int:
    cleaned = value.strip()
    if not cleaned:
        raise argparse.ArgumentTypeError("Query id cannot be empty")
    if cleaned.lower().startswith("q"):
        cleaned = cleaned[1:]
    try:
        query_id = int(cleaned, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid query id '{value}'") from exc
    if not 1 <= query_id <= TPCH_QUERY_COUNT:
        raise argparse.ArgumentTypeError(
            f"Query id must be between 1 and {TPCH_QUERY_COUNT}"
        )
    return query_id


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a full TPC-H query against DuckDB, Parquet, FastLanes, or Vortex data."
    )
    parser.add_argument(
        "--query",
        type=parse_query_id,
        required=True,
        help="TPC-H query id (1-22). You can also use forms like q1 or Q01.",
    )
    parser.add_argument(
        "--format",
        choices=("duckdb", "parquet", "fls", "vortex"),
        required=True,
        help="Storage format to query",
    )
    parser.add_argument(
        "--scale",
        default="1",
        type=parse_scale,
        help="TPC-H scale factor (e.g. 0.01, 1, 10). Defaults to 1.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of times to run the query (default: 5)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of threads to use (default: 1)",
    )
    return parser.parse_args(argv)


def load_tpch_query(query_id: int) -> str:
    query_path = TPCH_QUERY_DIR / f"q{query_id:02d}.sql"
    if not query_path.exists():
        raise FileNotFoundError(
            f"Query file {query_path} does not exist. "
            "Have you initialized submodules?"
        )
    text = query_path.read_text(encoding="utf-8").strip()
    if not text:
        raise RuntimeError(f"Query file {query_path} is empty")
    return text


def dataset_file_path(format_name: str, scale: ScaleInfo, table: str) -> Path:
    extension = {"parquet": ".parquet", "fls": ".fls", "vortex": ".vortex"}[format_name]
    table_path = DATA_ROOT / format_name / scale.dataset_name / f"{table}{extension}"
    if not table_path.exists():
        raise FileNotFoundError(f"Data file not found for table '{table}': {table_path}")
    return table_path


def table_view_statement(
    table: str, reader: str, table_path: Path, temp: bool = True
) -> str:
    qualifier = "TEMPORARY " if temp else ""
    return (
        f"CREATE OR REPLACE {qualifier}VIEW {table} AS "
        f"SELECT * FROM {reader}({sql_literal(table_path)});"
    )


def format_preparation_statements(
    format_name: str, scale: ScaleInfo
) -> tuple[Optional[Path], Sequence[str]]:
    if format_name == "duckdb":
        db_path = DATA_ROOT / "duckdb" / f"{scale.dataset_name}.duckdb"
        if not db_path.exists():
            raise FileNotFoundError(f"DuckDB database not found at {db_path}")
        return db_path, ()

    if format_name not in ("parquet", "fls", "vortex"):
        raise ValueError(f"Unsupported format '{format_name}'")

    reader = {
        "parquet": "read_parquet",
        "fls": "read_fls",
        "vortex": "read_vortex",
    }[format_name]
    statements: List[str] = []
    if format_name == "fls":
        statements.append(fastlanes_load_statement())
    elif format_name == "vortex":
        statements.append(vortex_load_statement())

    for table in TPCH_TABLES:
        table_path = dataset_file_path(format_name, scale, table)
        statements.append(table_view_statement(table, reader, table_path))
    return None, statements


def execute_runs(
    query_sql: str,
    runs: int,
    database: Optional[Path],
    extra_statements: Sequence[str],
    threads: int,
) -> None:
    if runs <= 0:
        raise ValueError("Number of runs must be positive")

    statements = list(base_session_statements(threads))
    statements.extend(extra_statements)
    warmup_script = [*statements, query_sql]
    timed_script = [*statements, ".timer on", query_sql, ".timer off"]

    for _ in range(WARMUP_RUNS):
        run_duckdb_script(warmup_script, database)

    durations: List[float] = []
    last_result = ""
    for run_idx in range(1, runs + 1):
        process = run_duckdb_script(timed_script, database)
        last_result, elapsed = extract_result_and_runtime(process.stdout)
        durations.append(elapsed)
        print(f"Run {run_idx}: elapsed={elapsed:.4f}s")

    print("Last result:")
    print(last_result)
    print(
        f"Completed {runs} runs. "
        f"mean={statistics.mean(durations):.4f}s "
        f"median={statistics.median(durations):.4f}s "
        f"min={min(durations):.4f}s "
        f"max={max(durations):.4f}s"
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    scale: ScaleInfo = args.scale
    runs: int = args.runs
    query_id: int = args.query
    threads: int = args.threads
    query_sql = load_tpch_query(query_id)
    database, extra_statements = format_preparation_statements(args.format, scale)
    execute_runs(query_sql, runs, database, extra_statements, threads)


if __name__ == "__main__":
    try:
        main()
    except (
        RuntimeError,
        FileNotFoundError,
        ValueError,
        argparse.ArgumentTypeError,
    ) as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
