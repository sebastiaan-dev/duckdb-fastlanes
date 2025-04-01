import csv
import random
import string
import os
import json


def double_rand_synth(num_cols):
    return [round(random.uniform(0.0, 100.0), 2) for _ in range(num_cols)]


def string_rand_synth(num_cols):
    pool_size = 3
    pool = [
        "".join(random.choices(string.ascii_letters, k=10)) for _ in range(pool_size)
    ]

    return ["".join(random.choice(pool)) for _ in range(num_cols)]


def string_const(num_cols):
    return ["".join("iufanejwkfe") for _ in range(num_cols)]


def int_bool_rand_synth(num_cols):
    return [random.randint(0, 1) for _ in range(num_cols)]


def generate_csv(file_path, num_rows, num_cols, func):
    """
    Generates a CSV file with the specified number of rows and columns.
    Each cell contains a random double value between 0.0 and 100.0.

    :param file_name: Name of the CSV file to create.
    :param num_rows: Number of rows to generate.
    :param num_cols: Number of columns in each row.
    """
    if os.path.exists(f"{file_path}/data.csv"):
        print(f"CSV file {file_path}/data.csv already exists. Skipping generation.")
        return

    os.makedirs(file_path, exist_ok=True)

    with open(f"{file_path}/data.csv", mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        for _ in range(num_rows):
            # Create a row with random doubles rounded to 2 decimal places.
            row = func(num_cols)
            writer.writerow(row)

    print(
        f"CSV file {benchmark['path']} with {num_rows} rows and {num_cols} columns has been generated."
    )


def generate_json_schema(file_path, num_cols, type):
    """
    Generates a JSON schema file with the specified number of columns.
    The schema file contains a "columns" key with a list of column names."
    """
    schema = {
        "columns": [{"name": f"COLUMN_{i}", "type": type} for i in range(num_cols)]
    }

    with open(f"{file_path}/schema.json", mode="w") as schemafile:
        schemafile.write(json.dumps(schema))

    print(f"JSON schema file with {num_cols} columns has been generated.")


if __name__ == "__main__":
    base_path = "/Users/sebastiaan/Documents/university/thesis/duckdb-fastlanes/benchmarks/data/source"
    benchmarks = [
        # {"path": "dbl_rand_synth", "func": double_rand_synth, "type": "FLS_DBL"},
        {"path": "multi_str_fsst", "func": string_rand_synth, "type": "FLS_STR"},
        # {"path": "str_const", "func": string_const, "type": "FLS_STR"},
        # {"path": "int_bool_rand_synth", "func": int_bool_rand_synth, "type": "FLS_I08"},
        # {"path": "int_bool_rand_synth", "func": int_bool_rand_synth, "type": "FLS_I08"},
    ]

    num_rows = 65536  # You can change this to generate more or fewer rows.
    num_cols = 20  # Dynamic number of columns.

    for benchmark in benchmarks:
        generate_csv(
            f"{base_path}/{benchmark['path']}", num_rows, num_cols, benchmark["func"]
        )
        generate_json_schema(
            f"{base_path}/{benchmark['path']}", num_cols, benchmark["type"]
        )
