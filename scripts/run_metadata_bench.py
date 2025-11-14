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
BUILD_DIR = REPO_ROOT / "build" / "release"
DUCKDB_BINARY = BUILD_DIR / "duckdb"
DEFAULT_TABLE = "lineitem"
DEFAULT_COLUMN = "l_extendedprice"
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
WARMUP_RUNS = 2
DUCKDB_CLI_FLAGS: Sequence[str] = ("-unsigned", "-csv", "-noheader", "-bail")
