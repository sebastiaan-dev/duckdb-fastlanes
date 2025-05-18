import csv
from enum import Enum
import json
import os
import random
import string
from decimal import Decimal


class ColumnType(Enum):
    INT64 = "FLS_I64"
    BIGINT = "FLS_I64"  # alias for INT64
    INT32 = "FLS_I32"
    INT16 = "FLS_I16"
    INT8 = "FLS_I08"
    UINT8 = "FLS_U08"
    DOUBLE = "FLS_DBL"
    double = "FLS_DBL"  # alias for DOUBLE
    FLS_STR = "FLS_STR"
    string = "FLS_STR"  # alias for FLS_STR
    STR = "STR"
    varchar = "STR"  # alias for STR
    VARCHAR = "STR"  # alias for STR
    LIST = "LIST"
    STRUCT = "STRUCT"
    map = "MAP"
    MAP = "MAP"


def gen_uint8():
    return random.randint(0, 255)


def gen_int8():
    return random.randint(-128, 127)


def gen_int16():
    return random.randint(-32768, 32767)


def gen_int32():
    return random.randint(-2147483648, 2147483647)


def gen_int64():
    return random.randint(-9223372036854775808, 9223372036854775807)


def gen_dbl():
    return random.uniform(-1000, 1000)


def gen_str():
    return "".join(random.choices(string.ascii_letters, k=10))


def gen_grid(n_rows, n_cols, gen_func):
    return [[gen_func() for _ in range(n_cols)] for _ in range(n_rows)]


#
# dec_uncompressed_opr
#


def gen_dec_uncompressed_opr_uint8(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_uint8), ColumnType.UINT8


def gen_dec_uncompressed_opr_int8(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_int8), ColumnType.INT8


def gen_dec_uncompressed_opr_int16(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_int16), ColumnType.INT16


def gen_dec_uncompressed_opr_int32(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_int32), ColumnType.INT32


def gen_dec_uncompressed_opr_int64(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_int64), ColumnType.INT64


def gen_dec_uncompressed_opr_dbl(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_dbl), ColumnType.DOUBLE


def gen_dec_uncompressed_opr_str(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, gen_str), ColumnType.FLS_STR


#
# dec_unffor_opr
#


def gen_grid_unffor(n_rows, n_cols, gen_func, max_value):
    data = [[0 for _ in range(n_cols)] for _ in range(n_rows)]

    for i in range(n_rows):
        for j in range(n_cols):
            if i == 0:
                data[i][j] = gen_func()
            else:
                data[i][j] = data[i - 1][j] + random.randint(1, 3)

            if data[i][j] > max_value:
                data[i][j] = gen_func()

    return data


def gen_dec_unffor_opr_uint8(n_rows, n_cols):
    return gen_grid_unffor(n_rows, n_cols, gen_uint8, 255), ColumnType.UINT8


def gen_dec_unffor_opr_int8(n_rows, n_cols):
    return gen_grid_unffor(n_rows, n_cols, gen_int8, 127), ColumnType.INT8


def gen_dec_unffor_opr_int16(n_rows, n_cols):
    return gen_grid_unffor(n_rows, n_cols, gen_int16, 32767), ColumnType.INT16


def gen_dec_unffor_opr_int32(n_rows, n_cols):
    return gen_grid_unffor(n_rows, n_cols, gen_int32, 2147483647), ColumnType.INT32


def gen_dec_unffor_opr_int64(n_rows, n_cols):
    return (
        gen_grid_unffor(n_rows, n_cols, gen_int64, 9223372036854775807),
        ColumnType.INT64,
    )


#
# dec_alp_opr
#


def gen_dec_alp_opr_dbl(n_rows, n_cols):
    data = [[0 for _ in range(n_cols)] for _ in range(n_rows)]

    for i in range(n_rows):
        for j in range(n_cols):
            data[i][j] = Decimal(str(random.uniform(0, 100))) / 100

    return data, ColumnType.DOUBLE


def gen_dec_alp_rd_opr_dbl(n_rows, n_cols):
    data = [[random.random() for _ in range(n_cols)] for _ in range(n_rows)]
    return data, ColumnType.DOUBLE


#
# dec_constant_opr
#


def gen_dec_constant_opr_uint8(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 1), ColumnType.UINT8


def gen_dec_constant_opr_int8(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 1), ColumnType.INT8


def gen_dec_constant_opr_int16(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 1000), ColumnType.INT16


def gen_dec_constant_opr_int32(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 327670), ColumnType.INT32


def gen_dec_constant_opr_int64(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 92233720368), ColumnType.INT64


def gen_dec_constant_opr_dbl(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: 1000.0), ColumnType.DOUBLE


def gen_dec_constant_opr_str(n_rows, n_cols):
    return gen_grid(n_rows, n_cols, lambda: "0xDEADBEEF"), ColumnType.FLS_STR


#
# dec_fsst_opr
#


def gen_dec_fsst_opr_str(n_rows, n_cols):
    """
    Targeting FSST DELTA encoding.

    Data requirements:
    - Random string values
    """
    base = "_123_prefix_"
    data = []
    for i in range(n_rows):
        row = []
        # Creating strings with an incremental number to provide delta pattern.
        for j in range(n_cols):
            row.append(f"{base}{i}_{j}")
        data.append(row)
    return data, ColumnType.FLS_STR


#
# dec_fsst12_opr
#


def gen_dec_fsst12_opr_str(n_rows, n_cols):
    """
    Targeting FSST12 DELTA encoding.

    Data requirements:
    - Random string values
    """
    pool_size = 100
    pool = [
        "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
        for _ in range(pool_size)
    ]

    data = [[random.choice(pool) for _ in range(n_cols)] for _ in range(n_rows)]

    return data, ColumnType.FLS_STR


#
# dec_null_opr
#


def gen_grid_null(n_rows, n_cols, gen_func):
    data = [[None for _ in range(n_cols)] for _ in range(n_rows)]

    for i in range(n_rows):
        for j in range(n_cols):
            if random.random() < 0.1:
                data[i][j] = gen_func()

    return data


def gen_dec_null_opr_int16(n_rows, n_cols):
    return gen_grid_null(n_rows, n_cols, gen_int16), ColumnType.INT16


def gen_dec_null_opr_int32(n_rows, n_cols):
    return gen_grid_null(n_rows, n_cols, gen_int32), ColumnType.INT32


def gen_dec_null_opr_dbl(n_rows, n_cols):
    return gen_grid_null(n_rows, n_cols, gen_dbl), ColumnType.DOUBLE


# TODO
# def gen_dec_slpatch_opr_i16(n_rows, n_cols):
#     """
#     Generates a synthetic vector for dec_slpatch_opr encoding.

#     Data requirements:
#     - A vector that is predominantly constant (e.g., 95% of values are the same)
#     - Outliers (exceptions) are generated in the remaining 5% of positions.
#     - A separate selection vector is produced to store the positions and values of outliers.
#     """
#     # Choose a base constant value for the majority of the data.
#     base_value = random.randint(1, 10)
#     constant_probability = 0.95
#     data = []

#     for _ in range(n_rows):
#         for _ in range(n_cols):
#             if random.random() < constant_probability:
#                 data.append(base_value)
#             else:
#                 data.append(random.randint(10, 100))

#     return data, ColumnType.UINT8

#
# dec_frequency_opr
#


def gen_grid_freq(n_rows, n_cols, default_val, gen_func):
    default_probability = 0.95
    data = [[default_val for _ in range(n_cols)] for _ in range(n_rows)]

    for i in range(n_rows):
        for j in range(n_cols):
            if not random.random() < default_probability:
                data[i][j] = gen_func()

    return data


def gen_dec_frequency_opr_int8(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, 0, gen_int8), ColumnType.INT8


def gen_dec_frequency_opr_int16(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, 0, gen_int16), ColumnType.INT16


def gen_dec_frequency_opr_int32(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, 0, gen_int32), ColumnType.INT32


def gen_dec_frequency_opr_int64(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, 0, gen_int64), ColumnType.INT64


def gen_dec_frequency_opr_dbl(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, 0, gen_dbl), ColumnType.DOUBLE


def gen_dec_frequency_opr_str(n_rows, n_cols):
    return gen_grid_freq(n_rows, n_cols, "0xDEADBEEF", gen_str), ColumnType.FLS_STR


#
# dec_cross_rle_opr
#


def gen_grid_cross_rle(n_rows, n_cols, gen_func):
    total_elements = n_rows * n_cols
    data = []
    i = 0

    while i < total_elements:
        constant_value = gen_func()
        run_length = random.randint(100, 500)
        run_length = min(run_length, total_elements - i)

        data.extend([constant_value] * run_length)
        i += run_length

    # Reshape flat list into a 2D list (table)
    table = [data[i * n_cols : (i + 1) * n_cols] for i in range(n_rows)]
    return table


def gen_dec_cross_rle_opr_int8(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_int8), ColumnType.INT8


def gen_dec_cross_rle_opr_int16(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_int16), ColumnType.INT16


def gen_dec_cross_rle_opr_int32(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_int32), ColumnType.INT32


def gen_dec_cross_rle_opr_int64(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_int64), ColumnType.INT64


def gen_dec_cross_rle_opr_dbl(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_dbl), ColumnType.DOUBLE


def gen_dec_cross_rle_opr_str(n_rows, n_cols):
    return gen_grid_cross_rle(n_rows, n_cols, gen_str), ColumnType.FLS_STR


def generate_all_encodings():
    """
    Generates synthetic datasets for all supported encodings.

    Returns:
    - A dictionary with encoding names as keys and synthetic data as values.
    """
    n_rows = 65536
    n_cols = 1

    return {
        "dec_uncompressed_opr_uint8": gen_dec_uncompressed_opr_uint8(n_rows, n_cols),
        "dec_uncompressed_opr_int8": gen_dec_uncompressed_opr_int8(n_rows, n_cols),
        "dec_uncompressed_opr_int16": gen_dec_uncompressed_opr_int16(n_rows, n_cols),
        "dec_uncompressed_opr_int32": gen_dec_uncompressed_opr_int32(n_rows, n_cols),
        "dec_uncompressed_opr_int64": gen_dec_uncompressed_opr_int64(n_rows, n_cols),
        "dec_uncompressed_opr_dbl": gen_dec_uncompressed_opr_dbl(n_rows, n_cols),
        "dec_uncompressed_opr_str": gen_dec_uncompressed_opr_str(n_rows, n_cols),
        ########################################
        "dec_unffor_opr_uint8": gen_dec_unffor_opr_uint8(n_rows, n_cols),
        "dec_unffor_opr_int8": gen_dec_unffor_opr_int8(n_rows, n_cols),
        "dec_unffor_opr_int16": gen_dec_unffor_opr_int16(n_rows, n_cols),
        "dec_unffor_opr_int32": gen_dec_unffor_opr_int32(n_rows, n_cols),
        "dec_unffor_opr_int64": gen_dec_unffor_opr_int64(n_rows, n_cols),
        ########################################
        "dec_alp_opr_dbl": gen_dec_alp_opr_dbl(n_rows, n_cols),
        "dec_alp_rd_opr_dbl": gen_dec_alp_rd_opr_dbl(n_rows, n_cols),
        ########################################
        "dec_constant_opr_uint8": gen_dec_constant_opr_uint8(n_rows, n_cols),
        "dec_constant_opr_int8": gen_dec_constant_opr_int8(n_rows, n_cols),
        "dec_constant_opr_int16": gen_dec_constant_opr_int16(n_rows, n_cols),
        "dec_constant_opr_int32": gen_dec_constant_opr_int32(n_rows, n_cols),
        "dec_constant_opr_int64": gen_dec_constant_opr_int64(n_rows, n_cols),
        "dec_constant_opr_dbl": gen_dec_constant_opr_dbl(n_rows, n_cols),
        "dec_constant_opr_str": gen_dec_constant_opr_str(n_rows, n_cols),
        ########################################
        "dec_fsst_opr_str": gen_dec_fsst_opr_str(n_rows, n_cols),
        "dec_fsst12_opr_str": gen_dec_fsst12_opr_str(n_rows, n_cols),
        ########################################
        "dec_null_opr_int16": gen_dec_null_opr_int16(n_rows, n_cols),
        "dec_null_opr_int32": gen_dec_null_opr_int32(n_rows, n_cols),
        "dec_null_opr_dbl": gen_dec_null_opr_dbl(n_rows, n_cols),
        ########################################
        # "dec_slpatch_opr_i16": gen_dec_slpatch_opr_i16(n_rows, n_cols),
        ########################################
        "dec_frequency_opr_int8": gen_dec_frequency_opr_int8(n_rows, n_cols),
        "dec_frequency_opr_int16": gen_dec_frequency_opr_int16(n_rows, n_cols),
        "dec_frequency_opr_int32": gen_dec_frequency_opr_int32(n_rows, n_cols),
        "dec_frequency_opr_int64": gen_dec_frequency_opr_int64(n_rows, n_cols),
        "dec_frequency_opr_dbl": gen_dec_frequency_opr_dbl(n_rows, n_cols),
        "dec_frequency_opr_str": gen_dec_frequency_opr_str(n_rows, n_cols),
        ########################################
        "dec_cross_rle_opr_int8": gen_dec_cross_rle_opr_int8(n_rows, n_cols),
        "dec_cross_rle_opr_int16": gen_dec_cross_rle_opr_int16(n_rows, n_cols),
        "dec_cross_rle_opr_int32": gen_dec_cross_rle_opr_int32(n_rows, n_cols),
        "dec_cross_rle_opr_int64": gen_dec_cross_rle_opr_int64(n_rows, n_cols),
        "dec_cross_rle_opr_dbl": gen_dec_cross_rle_opr_dbl(n_rows, n_cols),
        "dec_cross_rle_opr_str": gen_dec_cross_rle_opr_str(n_rows, n_cols),
    }


def write_to_csv(data, file_path):
    """
    Writes the given data to a CSV file.

    :param data: Data to write to the CSV file.
    :param file_path: Path to the CSV file.
    """

    with open(file_path, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="|")
        for row in data:
            writer.writerow(row)

    print(f"CSV file {file_path} has been generated.")


def write_to_json_schema(types, file_path):
    """
    Generates a JSON schema file with the specified number of columns.
    The schema file contains a "columns" key with a list of column names.

    :param types: Types of the columns in the schema.
    :param file_path: Path to the JSON schema file.
    """
    schema = {
        "columns": [
            {"name": f"COLUMN_{i}", "type": col_type}
            for i, col_type in enumerate(types)
        ]
    }

    with open(file_path, mode="w") as schemafile:
        schemafile.write(json.dumps(schema))


def write_all_encodings_to_csv():
    """
    Generates synthetic datasets for all supported encodings and writes them into a single CSV file.
    """
    encodings = generate_all_encodings()

    for name, (data, col_type) in encodings.items():
        path = f"data/source/{name}"
        os.makedirs(path, exist_ok=True)
        write_to_csv(data, f"{path}/data.csv")
        write_to_json_schema([col_type.value], f"{path}/schema.json")


if __name__ == "__main__":
    write_all_encodings_to_csv()
    print("All synthetic data and schema files have been generated.")
