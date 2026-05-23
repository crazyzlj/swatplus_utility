import csv
import os
from typing import List


def build_output_columns() -> List[str]:
    """Build the target output column order."""
    cols = [
        "OBJECTID", "MUID", "SEQN", "SNAM", "S5ID", "CMPPCT",
        "NLAYERS", "HYDGRP", "SOL_ZMX", "ANION_EXCL", "SOL_CRK", "TEXTURE"
    ]

    # Layer-wise fields excluding SOL_CALi and SOL_PHi
    layer_prefix_fields = [
        "SOL_Z", "SOL_BD", "SOL_AWC", "SOL_K", "SOL_CBN",
        "CLAY", "SILT", "SAND", "ROCK", "SOL_ALB", "USLE_K", "SOL_EC"
    ]

    for i in range(1, 11):
        for field in layer_prefix_fields:
            cols.append(f"{field}{i}")

    for i in range(1, 11):
        cols.append(f"SOL_CAL{i}")

    for i in range(1, 11):
        cols.append(f"SOL_PH{i}")

    return cols


def reorder_usersoil_csv(input_csv: str, output_csv: str) -> None:
    """
    Reorder usersoil CSV columns so that all SOL_CALi and SOL_PHi fields
    are moved to the end. Rename SNAME to SNAM in output if needed.
    """
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    target_columns = build_output_columns()

    with open(input_csv, "r", newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        if reader.fieldnames is None:
            raise ValueError("Input CSV has no header row.")

        input_columns = reader.fieldnames[:]

        # Accept SNAME in input, but output as SNAM
        normalized_input_columns = [("SNAM" if c == "SNAME" else c) for c in input_columns]

        missing = [c for c in target_columns if c not in normalized_input_columns]
        extra = [c for c in normalized_input_columns if c not in target_columns]

        if missing:
            raise ValueError(
                "Input CSV is missing required columns:\n" + ", ".join(missing)
            )

        if extra:
            print("Warning: the following extra columns exist in input and will be ignored:")
            print(", ".join(extra))

        with open(output_csv, "w", newline="", encoding="utf-8-sig") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=target_columns)
            writer.writeheader()

            for row in reader:
                # Rename SNAME -> SNAM in row dict if present
                if "SNAME" in row and "SNAM" not in row:
                    row["SNAM"] = row.pop("SNAME")

                out_row = {col: row.get(col, "") for col in target_columns}
                writer.writerow(out_row)


if __name__ == "__main__":
    input_csv = r"D:\data_m\manitowoc\soil\soils_manitowoc0421.csv"
    output_csv = r"D:\data_m\manitowoc\soil\soils_manitowoc0422putAllCalPHatTheEnd.csv"

    reorder_usersoil_csv(input_csv, output_csv)
    print(f"Done. Output written to: {output_csv}")