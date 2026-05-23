#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate channel initialization records for a SWAT+ project database.

Workflow
--------
1. Read channel index records from chandeg_con.
2. For each channel, locate the weather station tmp file via weather_sta_cli.
3. Read Tmax/Tmin on the simulation start date and compute init_tmp.
4. Write flo as a fraction (e.g. 0.2), not as m3.
5. Upsert one record into om_water_ini.
6. Upsert one record into initial_cha.
7. Update channel_lte_cha.init_id.

Notes
-----
- According to the SWAT+ source code:
    tot_stor(ich)%flo = om_init_water(iom_ini)%flo * chd * chw * chl * 1000
  so om_water_ini.flo should be a dimensionless fraction rather than m3.
- hyd_sed_lte_cha.len is assumed to be in km.
- wd is treated as top width at bankfull.
- side_slp is retained for diagnostic trapezoid volume calculation only.
- All other om_water_ini fields are initialized to 0.0 for now.
"""

from __future__ import annotations

import os
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional


LOGGER = logging.getLogger(__name__)


@dataclass
class ChannelRecord:
    chandeg_id: int
    chandeg_name: str
    lcha_id: int
    lcha_name: str
    hyd_id: int
    wst_id: Optional[int]
    tmp_file: Optional[str]
    wd: float
    dp: float
    length_km: float
    side_slp: float


OM_WATER_ZERO_FIELDS = {
    "sed": 0.0,
    "orgn": 0.0,
    "sedp": 0.0,
    "no3": 0.0,
    "solp": 0.0,
    "chl_a": 0.0,
    "nh3": 0.0,
    "no2": 0.0,
    "cbn_bod": 0.0,
    "dis_ox": 0.0,
    "san": 0.0,
    "sil": 0.0,
    "cla": 0.0,
    "sag": 0.0,
    "lag": 0.0,
    "grv": 0.0,
    "c": 0.0,
}


def parse_start_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid --start-date: {date_str!r}, expected YYYY-MM-DD") from exc


def get_julian_day(d: date) -> int:
    return d.timetuple().tm_yday


def format_channel_suffix(channel_id: int, channel_name: str) -> str:
    """
    Use trailing digits in channel name if available, otherwise fall back to id.
    Ensures at least 3 digits.
    """
    match = re.search(r"(\d+)$", channel_name)
    if match:
        return match.group(1).zfill(3)
    return f"{channel_id:03d}"


def calculate_init_tmp(
    txtinout_dir: Path,
    tmp_filename: str,
    start_date: date,
) -> float:
    """
    Read the station temperature file and compute initial water temperature
    from the specified simulation start date:
        init_tmp = (tmax + tmin) / 2

    Expected file structure:
        line 1: file name
        line 2: header
        line 3: metadata
        line 4+: year, julian_day, tmax, tmin
    """
    tmp_path = txtinout_dir / tmp_filename
    if not tmp_path.exists():
        raise FileNotFoundError(f"Temperature file not found: {tmp_path}")

    target_year = start_date.year
    target_jday = get_julian_day(start_date)

    with tmp_path.open("r", encoding="utf-8", errors="ignore") as f:
        lines = [line.strip() for line in f if line.strip()]

    if len(lines) < 4:
        raise ValueError(f"Temperature file format is invalid or too short: {tmp_path}")

    for lineno, line in enumerate(lines[3:], start=4):
        parts = line.split()
        if len(parts) < 4:
            LOGGER.debug("Skipping malformed line %s in %s: %s", lineno, tmp_path, line)
            continue

        try:
            year = int(float(parts[0]))
            jday = int(float(parts[1]))
            tmax = float(parts[2])
            tmin = float(parts[3])
        except ValueError as exc:
            raise ValueError(
                f"Failed to parse line {lineno} in {tmp_path}: {line}"
            ) from exc

        if year == target_year and jday == target_jday:
            return (tmax + tmin) / 2.0

    raise ValueError(
        f"No temperature record found for {start_date.isoformat()} "
        f"(year={target_year}, jday={target_jday}) in file: {tmp_path}"
    )


def calculate_init_flo_fraction(storage_ratio: float = 0.2) -> float:
    """
    Return the value written into om_water_ini.flo.

    Based on SWAT+ source code, om_water_ini.flo is interpreted as
    a dimensionless fraction, not an absolute storage volume in m3.
    """
    if not (0.0 <= storage_ratio <= 1.0):
        raise ValueError(f"storage_ratio must be between 0 and 1, got {storage_ratio}")
    return float(storage_ratio)


def calculate_model_rect_bankfull_volume_m3(
    wd: float,
    dp: float,
    length_km: float,
) -> float:
    """
    Diagnostic volume estimate consistent with the source-code-style geometry:
        volume = dp * wd * len * 1000

    This is NOT written to om_water_ini.flo.
    """
    if wd <= 0:
        raise ValueError(f"Invalid wd: {wd}")
    if dp <= 0:
        raise ValueError(f"Invalid dp: {dp}")
    if length_km <= 0:
        raise ValueError(f"Invalid len (km): {length_km}")

    return dp * wd * length_km * 1000.0


def calculate_trapezoid_bankfull_volume_m3(
    wd: float,
    dp: float,
    length_km: float,
    side_slp: float,
) -> float:
    """
    Diagnostic trapezoid-based bankfull volume estimate using side_slp.

    Bottom width:
        b = wd - 2 * side_slp * dp

    If b >= 0:
        A = dp * (b + wd) / 2
    Else:
        A = wd * dp / 2  (triangular fallback)

    This is NOT written to om_water_ini.flo.
    """
    if wd <= 0:
        raise ValueError(f"Invalid wd: {wd}")
    if dp <= 0:
        raise ValueError(f"Invalid dp: {dp}")
    if length_km <= 0:
        raise ValueError(f"Invalid len (km): {length_km}")
    if side_slp < 0:
        raise ValueError(f"Invalid side_slp: {side_slp}")

    bottom_width = wd - 2.0 * side_slp * dp

    if bottom_width >= 0:
        bankfull_area = dp * (bottom_width + wd) / 2.0
    else:
        LOGGER.warning(
            "Computed bottom width < 0 (wd=%.4f, dp=%.4f, side_slp=%.4f). "
            "Using triangular fallback.",
            wd, dp, side_slp,
        )
        bankfull_area = wd * dp / 2.0

    if bankfull_area <= 0:
        raise ValueError(
            f"Computed non-positive bankfull area: {bankfull_area} "
            f"(wd={wd}, dp={dp}, side_slp={side_slp})"
        )

    return bankfull_area * length_km * 1000.0


def connect_db(sqlite_db: Path) -> sqlite3.Connection:
    if not sqlite_db.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_db}")

    conn = sqlite3.connect(str(sqlite_db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def fetch_channel_records(conn: sqlite3.Connection) -> list[ChannelRecord]:
    """
    Read all channels together with:
    - chandeg_con.wst_id
    - weather_sta_cli.tmp
    - channel_lte_cha.hyd_id
    - hyd_sed_lte_cha.{wd, dp, len, side_slp}
    """
    sql = """
    SELECT
        c.id AS chandeg_id,
        c.name AS chandeg_name,
        c.lcha_id AS lcha_id,
        l.name AS lcha_name,
        l.hyd_id AS hyd_id,
        c.wst_id AS wst_id,
        w.tmp AS tmp_file,
        h.wd AS wd,
        h.dp AS dp,
        h.len AS length_km,
        h.side_slp AS side_slp
    FROM chandeg_con c
    JOIN channel_lte_cha l
        ON l.id = c.lcha_id
    JOIN hyd_sed_lte_cha h
        ON h.id = l.hyd_id
    LEFT JOIN weather_sta_cli w
        ON w.id = c.wst_id
    ORDER BY c.id;
    """

    rows = conn.execute(sql).fetchall()
    records: list[ChannelRecord] = []
    for row in rows:
        records.append(
            ChannelRecord(
                chandeg_id=row["chandeg_id"],
                chandeg_name=row["chandeg_name"],
                lcha_id=row["lcha_id"],
                lcha_name=row["lcha_name"],
                hyd_id=row["hyd_id"],
                wst_id=row["wst_id"],
                tmp_file=row["tmp_file"],
                wd=float(row["wd"]),
                dp=float(row["dp"]),
                length_km=float(row["length_km"]),
                side_slp=float(row["side_slp"]),
            )
        )
    return records


def get_existing_id_by_name(
    conn: sqlite3.Connection,
    table_name: str,
    name: str,
) -> Optional[int]:
    sql = f"SELECT id FROM {table_name} WHERE name = ?"
    row = conn.execute(sql, (name,)).fetchone()
    return None if row is None else int(row["id"])


def upsert_om_water_ini(
    conn: sqlite3.Connection,
    name: str,
    flo: float,
    tmp: float,
) -> int:
    existing_id = get_existing_id_by_name(conn, "om_water_ini", name)

    payload = {
        "name": name,
        "flo": flo,
        "tmp": tmp,
        **OM_WATER_ZERO_FIELDS,
    }

    if existing_id is None:
        sql = """
        INSERT INTO om_water_ini (
            name, flo, sed, orgn, sedp, no3, solp, chl_a, nh3, no2,
            cbn_bod, dis_ox, san, sil, cla, sag, lag, grv, tmp, c
        ) VALUES (
            :name, :flo, :sed, :orgn, :sedp, :no3, :solp, :chl_a, :nh3, :no2,
            :cbn_bod, :dis_ox, :san, :sil, :cla, :sag, :lag, :grv, :tmp, :c
        )
        """
        cur = conn.execute(sql, payload)
        return int(cur.lastrowid)

    sql = """
    UPDATE om_water_ini
    SET
        flo = :flo,
        sed = :sed,
        orgn = :orgn,
        sedp = :sedp,
        no3 = :no3,
        solp = :solp,
        chl_a = :chl_a,
        nh3 = :nh3,
        no2 = :no2,
        cbn_bod = :cbn_bod,
        dis_ox = :dis_ox,
        san = :san,
        sil = :sil,
        cla = :cla,
        sag = :sag,
        lag = :lag,
        grv = :grv,
        tmp = :tmp,
        c = :c
    WHERE name = :name
    """
    conn.execute(sql, payload)
    return existing_id


def upsert_initial_cha(
    conn: sqlite3.Connection,
    name: str,
    org_min_id: int,
) -> int:
    existing_id = get_existing_id_by_name(conn, "initial_cha", name)

    if existing_id is None:
        sql = """
        INSERT INTO initial_cha (
            name, org_min_id, pest_id, path_id, hmet_id, salt_id, salt_cs_id, description
        ) VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL)
        """
        cur = conn.execute(sql, (name, org_min_id))
        return int(cur.lastrowid)

    sql = """
    UPDATE initial_cha
    SET
        org_min_id = ?,
        pest_id = NULL,
        path_id = NULL,
        hmet_id = NULL,
        salt_id = NULL,
        salt_cs_id = NULL,
        description = NULL
    WHERE name = ?
    """
    conn.execute(sql, (org_min_id, name))
    return existing_id


def update_channel_init_id(
    conn: sqlite3.Connection,
    lcha_id: int,
    init_id: int,
) -> None:
    sql = "UPDATE channel_lte_cha SET init_id = ? WHERE id = ?"
    conn.execute(sql, (init_id, lcha_id))


def generate_initialization(
    sqlite_db: Path,
    txtinout_dir: Path,
    start_date: date,
    storage_ratio: float,
) -> None:
    tmp_cache: Dict[str, float] = {}

    conn = connect_db(sqlite_db)
    try:
        records = fetch_channel_records(conn)
        LOGGER.info("Found %d channels in chandeg_con.", len(records))

        if not records:
            LOGGER.warning("No channel records found. Nothing to do.")
            return

        processed = 0

        with conn:
            for rec in records:
                if rec.wst_id is None:
                    raise ValueError(
                        f"Channel {rec.chandeg_name} (id={rec.chandeg_id}) has NULL wst_id."
                    )
                if not rec.tmp_file:
                    raise ValueError(
                        f"Channel {rec.chandeg_name} (id={rec.chandeg_id}) has no tmp file "
                        f"in weather_sta_cli for wst_id={rec.wst_id}."
                    )

                if rec.tmp_file not in tmp_cache:
                    tmp_cache[rec.tmp_file] = calculate_init_tmp(
                        txtinout_dir=txtinout_dir,
                        tmp_filename=rec.tmp_file,
                        start_date=start_date,
                    )
                init_tmp = tmp_cache[rec.tmp_file]

                flo_fraction = calculate_init_flo_fraction(storage_ratio=storage_ratio)

                # Diagnostic only: estimated actual initial storage in m3
                model_rect_bankfull_m3 = calculate_model_rect_bankfull_volume_m3(
                    wd=rec.wd,
                    dp=rec.dp,
                    length_km=rec.length_km,
                )
                model_rect_init_m3 = flo_fraction * model_rect_bankfull_m3

                trap_bankfull_m3 = calculate_trapezoid_bankfull_volume_m3(
                    wd=rec.wd,
                    dp=rec.dp,
                    length_km=rec.length_km,
                    side_slp=rec.side_slp,
                )
                trap_init_m3 = flo_fraction * trap_bankfull_m3

                suffix = format_channel_suffix(rec.chandeg_id, rec.chandeg_name)
                om_name = f"ominitcha{suffix}"
                init_name = f"initcha{suffix}"

                om_water_ini_id = upsert_om_water_ini(
                    conn=conn,
                    name=om_name,
                    flo=flo_fraction,
                    tmp=init_tmp,
                )

                initial_cha_id = upsert_initial_cha(
                    conn=conn,
                    name=init_name,
                    org_min_id=om_water_ini_id,
                )

                update_channel_init_id(
                    conn=conn,
                    lcha_id=rec.lcha_id,
                    init_id=initial_cha_id,
                )

                processed += 1
                LOGGER.info(
                    "Processed %s -> om_water_ini=%s (id=%d), initial_cha=%s (id=%d), "
                    "flo_fraction=%.4f, tmp=%.3f C, "
                    "model_rect_init_m3=%.3f, trap_init_m3=%.3f",
                    rec.chandeg_name,
                    om_name,
                    om_water_ini_id,
                    init_name,
                    initial_cha_id,
                    flo_fraction,
                    init_tmp,
                    model_rect_init_m3,
                    trap_init_m3,
                )

        LOGGER.info("Done. Processed %d channels.", processed)

    finally:
        conn.close()


def main() -> None:
    log_level = 'INFO'
    storage_ratio = 0.2
    start_date = '2002-01-01'
    txtinout_dir = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut'
    sqlite_db = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\manitowoc_test30mv5.sqlite'

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not os.path.exists(txtinout_dir):
        raise FileNotFoundError(f"TxtInOut directory not found: {txtinout_dir}")
    if not os.path.isdir(txtinout_dir):
        raise NotADirectoryError(f"TxtInOut path is not a directory: {txtinout_dir}")
    txtinout_dir = Path(txtinout_dir)
    sqlite_db = Path(sqlite_db)

    generate_initialization(
        sqlite_db=sqlite_db,
        txtinout_dir=txtinout_dir,
        start_date=parse_start_date(start_date),
        storage_ratio=storage_ratio,
    )


if __name__ == "__main__":
    main()