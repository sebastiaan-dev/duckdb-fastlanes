#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from textwrap import dedent

from tpch_utils import TPCH_QUERY_NUMBERS, TPCH_TABLES, parse_scale_list

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BENCHMARK_ROOT = PROJECT_ROOT / "benchmark" / "queries" / "tpch"
ANSWERS_ROOT = PROJECT_ROOT / "duckdb" / "extension" / "tpch" / "dbgen" / "answers"

FORMAT_CONFIGS = {
    "fls": {
        "require": "fastlanes",
        "load": True,
        "load_line": "create view {table} as from read_fls('./benchmark/data/generated/fls/{dataset_name}/{table}.fls');",
        "run_block": "run duckdb/extension/tpch/dbgen/queries/q${QUERY_NUMBER_PADDED}.sql",
    },
    "parquet": {
        "require": "parquet",
        "load": True,
        "load_line": "create view {table} as from parquet_scan('./benchmark/data/generated/parquet/{dataset_name}/{table}.parquet');",
        "run_block": "run duckdb/extension/tpch/dbgen/queries/q${QUERY_NUMBER_PADDED}.sql",
    },
    "duckdb": {
        "load": True,
        "load_lines": [
            "attach './benchmark/data/generated/duckdb/{dataset_name}.duckdb' as tpch_db;",
        ],
        "load_table_template": "create or replace view {table} as select * from tpch_db.{table};",
        "run_block": "run duckdb/extension/tpch/dbgen/queries/q${QUERY_NUMBER_PADDED}.sql",
        "cleanup_lines": [
            "use memory;",
        ],
        "cleanup_finalize_lines": [
            "detach tpch_db;",
        ],
    },
}

def build_load_sql(scale, fmt_config) -> str:
    lines: list[str] = []
    if "load_lines" in fmt_config:
        lines.extend(
            line.format(dataset_name=scale.dataset_name)
            for line in fmt_config["load_lines"]
        )
    if "load_line" in fmt_config:
        line_template = fmt_config["load_line"]
        lines.extend(
            line_template.format(table=table, dataset_name=scale.dataset_name)
            for table in TPCH_TABLES
        )
    if "load_table_template" in fmt_config:
        table_template = fmt_config["load_table_template"]
        lines.extend(
            table_template.format(table=table, dataset_name=scale.dataset_name)
            for table in TPCH_TABLES
        )
    if "load_lines_after" in fmt_config:
        lines.extend(
            line.format(dataset_name=scale.dataset_name)
            for line in fmt_config["load_lines_after"]
        )
    return "\n".join(lines) + ("\n" if lines else "")


def build_template(scale, fmt_name, fmt_config, answers_available: bool) -> str:
    require_clause = fmt_config.get("require")

    run_block_template = fmt_config["run_block"].strip("\n")
    if "{dataset_name}" in run_block_template:
        run_block = run_block_template.replace("{dataset_name}", scale.dataset_name)
    else:
        run_block = run_block_template

    if answers_available:
        result_line = f"result duckdb/extension/tpch/dbgen/answers/{scale.result_dir}/q${{QUERY_NUMBER_PADDED}}.csv"
    else:
        result_line = "# result files not available for this scale factor"

    lines = [
        "# name: ${FILE_PATH}",
        "# description: ${DESCRIPTION}",
        f"# group: [{scale.group_tag}]",
        "",
        "name Q${QUERY_NUMBER_PADDED}",
        "group tpch",
        f"subgroup {scale.group_tag}",
        "",
    ]

    if require_clause:
        lines.append(f"require {require_clause}")
        lines.append("")

    if fmt_config["load"]:
        lines.append(f"load benchmark/queries/tpch/{scale.group_tag}/{fmt_name}/load.sql")
        lines.append("")

    lines.extend(run_block.splitlines())
    lines.append("")
    cleanup_section: list[str] = []
    # if "cleanup_lines" in fmt_config:
    #     cleanup_section.extend(
    #         line.format(dataset_name=scale.dataset_name)
    #         for line in fmt_config["cleanup_lines"]
    #     )
    # if "cleanup_finalize_lines" in fmt_config:
    #     cleanup_section.extend(
    #         line.format(dataset_name=scale.dataset_name)
    #         for line in fmt_config["cleanup_finalize_lines"]
    #     )

    if cleanup_section:
        lines.append("cleanup")
        lines.extend(cleanup_section)
        lines.append("")

    lines.append(result_line)

    return "\n".join(lines) + "\n"


def build_query_file(scale, fmt_name, query_number) -> str:
    query_number_padded = f"{query_number:02d}"
    path = f"benchmark/queries/tpch/{scale.group_tag}/{fmt_name}/q{query_number_padded}.benchmark"
    description = f"Run query {query_number_padded} from the TPC-H benchmark"
    template_path = f"benchmark/queries/tpch/{scale.group_tag}/{fmt_name}/tpch_{scale.group_tag}.benchmark.in"
    content = dedent(
        f"""
        # name: {path}
        # description: {description}
        # group: [{scale.group_tag}-{fmt_name}]

        template {template_path}
        QUERY_NUMBER={query_number}
        QUERY_NUMBER_PADDED={query_number_padded}
        """
    ).strip()
    return "\n".join(line.rstrip() for line in content.splitlines()) + "\n"


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def generate_scale(scale, formats: list[str]) -> None:
    answers_dir = ANSWERS_ROOT / scale.result_dir
    answers_available = answers_dir.is_dir()

    for fmt_name in formats:
        fmt_config = FORMAT_CONFIGS[fmt_name]
        scale_dir = BENCHMARK_ROOT / scale.group_tag / fmt_name
        if scale_dir.exists():
            shutil.rmtree(scale_dir)
        template_path = scale_dir / f"tpch_{scale.group_tag}.benchmark.in"
        template_content = build_template(scale, fmt_name, fmt_config, answers_available)
        write_file(template_path, template_content)

        if fmt_config["load"]:
            load_path = scale_dir / "load.sql"
            load_content = build_load_sql(scale, fmt_config)
            write_file(load_path, load_content)

        for query_number in TPCH_QUERY_NUMBERS:
            query_file = scale_dir / f"q{query_number:02d}.benchmark"
            query_content = build_query_file(scale, fmt_name, query_number)
            write_file(query_file, query_content)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate benchmark specification files for TPC-H scale factors"
    )
    parser.add_argument(
        "--scale-factors",
        "-s",
        nargs="+",
        help="List of scale factors to generate benchmarks for (e.g. 0.01 1 10)",
    )
    parser.add_argument(
        "--sf",
        dest="scale_factors_repeat",
        action="append",
        help="Scale factor to generate benchmarks for (can be repeated)",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=sorted(FORMAT_CONFIGS.keys()),
        help="Subset of formats to generate (defaults to all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    factors: list[str] = []
    if args.scale_factors:
        factors.extend(args.scale_factors)
    if args.scale_factors_repeat:
        factors.extend(args.scale_factors_repeat)

    scales = parse_scale_list(factors or None, default=["1"])
    formats = args.formats or list(FORMAT_CONFIGS.keys())

    for scale in scales:
        generate_scale(scale, formats)


if __name__ == "__main__":
    main()
