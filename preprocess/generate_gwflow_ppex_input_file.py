import pandas as pd
import numpy as np
from datetime import datetime
import calendar
import os


def generate_gwflow_ppex(
        summary_excel,
        grid_csv,
        output_ppex,
        valid_wells_csv,
        sim_start_date_str,
        sim_end_date_str,
        scale_factor=1.0
):
    print(">>> 1. 初始化参数与读取数据...")
    sim_start = datetime.strptime(sim_start_date_str, "%Y/%m/%d")
    sim_end = datetime.strptime(sim_end_date_str, "%Y/%m/%d")
    total_sim_days = (sim_end - sim_start).days + 1

    if total_sim_days <= 0:
        print("错误：模拟结束时间必须晚于开始时间！")
        return

    # 用户确认的转换系数
    GAL_TO_M3 = 0.00378541178
    months_keys = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov',
                   'Dec']

    # 读取 Grid ID 映射表
    grid_df = pd.read_csv(grid_csv)
    # 创建 WI_Unique -> Grid ID 的映射字典
    grid_map = pd.Series(
            grid_df['Id'].values,
            index=grid_df['WI_Unique'].astype(str).str.strip().str.upper()
    ).to_dict()

    df_summary = pd.read_excel(summary_excel)

    # 建立每口井的月度时间序列字典
    well_monthly_records = {}

    for _, row in df_summary.iterrows():
        unique_id_year = str(row['UniqueID-Year'])
        if '-' not in unique_id_year: continue

        wi_unique, year_str = unique_id_year.rsplit('-', 1)
        wi_unique = wi_unique.strip().upper()
        year = int(year_str)

        if wi_unique not in grid_map: continue

        if wi_unique not in well_monthly_records:
            well_monthly_records[wi_unique] = []

        for month_idx, month_name in enumerate(months_keys, start=1):
            val_gallons = row.get(month_name, np.nan)
            if pd.notna(val_gallons) and val_gallons > 0:
                well_monthly_records[wi_unique].append({
                    'year': year,
                    'month': month_idx,
                    'gallons': float(val_gallons)
                })

    print(">>> 2. 执行质量守恒分配并聚合至网格单元...")
    # grid_daily_m3: {grid_id: array_of_daily_rates}
    grid_daily_m3 = {}
    # grid_unique_ids: {grid_id: set_of_well_ids}
    grid_unique_ids = {}
    valid_wi_uniques = set()

    for wi_unique, records in well_monthly_records.items():
        grid_id = grid_map[wi_unique]
        records = sorted(records, key=lambda x: (x['year'], x['month']))

        # 记录该网格包含哪些井ID
        if grid_id not in grid_unique_ids:
            grid_unique_ids[grid_id] = set()
        grid_unique_ids[grid_id].add(wi_unique)

        # 智能块聚合：将连续且加仑数相同的月份缝合
        blocks = []
        if records:
            current_block = [records[0]]
            for rec in records[1:]:
                prev = current_block[-1]
                is_contiguous = (rec['year'] == prev['year'] and rec['month'] == prev[
                    'month'] + 1) or \
                                (rec['year'] == prev['year'] + 1 and prev['month'] == 12 and rec[
                                    'month'] == 1)
                is_same_gallons = abs(rec['gallons'] - prev['gallons']) < 1e-3

                if is_contiguous and is_same_gallons:
                    current_block.append(rec)
                else:
                    blocks.append(current_block)
                    current_block = [rec]
            blocks.append(current_block)

        for block in blocks:
            first_rec, last_rec = block[0], block[-1]
            start_date = datetime(first_rec['year'], first_rec['month'], 1)
            end_date = datetime(last_rec['year'], last_rec['month'],
                                calendar.monthrange(last_rec['year'], last_rec['month'])[1])

            # 均摊计算：(总和 / 总天数) 保证质量平衡
            total_gallons = sum(r['gallons'] for r in block)
            total_days = (end_date - start_date).days + 1
            daily_rate_m3 = (total_gallons * GAL_TO_M3 * scale_factor) / total_days

            overlap_start, overlap_end = max(sim_start, start_date), min(sim_end, end_date)

            if overlap_start <= overlap_end:
                s_day = (overlap_start - sim_start).days + 1
                e_day = (overlap_end - sim_start).days + 1

                if grid_id not in grid_daily_m3:
                    grid_daily_m3[grid_id] = np.zeros(total_sim_days + 2)

                # 累加到网格日历数组（支持同网格多井叠加）
                grid_daily_m3[grid_id][s_day: e_day + 1] += daily_rate_m3
                valid_wi_uniques.add(wi_unique)

    print(">>> 3. 构建抽水时段块并生成格式化数据...")
    final_output_structure = {}

    for grid_id, daily_array in grid_daily_m3.items():
        periods = []
        c_start, c_rate = None, 0.0

        for day in range(1, total_sim_days + 1):
            day_rate = round(daily_array[day], 2)
            if c_start is None:
                if day_rate > 0:
                    c_start, c_rate = day, day_rate
            else:
                if day_rate != c_rate:
                    periods.append((c_start, day - 1, c_rate))
                    if day_rate > 0:
                        c_start, c_rate = day, day_rate
                    else:
                        c_start, c_rate = None, 0.0
        if c_start is not None:
            periods.append((c_start, total_sim_days, c_rate))

        if periods:
            # 拼接该 Grid ID 对应的所有 UniqueID
            joined_ids = "-".join(sorted(grid_unique_ids[grid_id]))
            final_output_structure[grid_id] = {
                'well_ids': joined_ids,
                'periods': periods
            }

    print(f">>> 4. 正在按最新格式写入 {output_ppex} ...")
    with open(output_ppex, 'w', encoding='utf-8') as f:
        f.write("Groundwater Pumping Information\n")
        f.write(f"{len(final_output_structure)}\n")

        for gid in sorted(final_output_structure.keys()):
            data = final_output_structure[gid]
            # 第 3n+1 行：拼接后的 WellID
            f.write(f"{data['well_ids']}\n")
            # 第 3n+2 行：GridID 和 抽水时段数量
            f.write(f"{gid:<8} {len(data['periods'])}\n")
            # 第 3n+3 行开始：Start_Day  End_Day  Rate
            for p in data['periods']:
                f.write(f"{p[0]:<8} {p[1]:<8} {p[2]}\n")

    print(f">>> 5. 导出有效井清单至 {valid_wells_csv} ...")
    df_valid = grid_df[
        grid_df['WI_Unique'].astype(str).str.strip().str.upper().isin(valid_wi_uniques)]
    df_valid.to_csv(valid_wells_csv, index=False)

    print(f"\n✅ 成功！已生成符合新格式的 pumpex 文件。")


if __name__ == "__main__":
    # 配置
    SUMMARY_FILE = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\results\Summary_All_Wells.xlsx"  # 汇总好的年度数据
    GRID_MAPPING_FILE = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\wells_within_manitowoc_with_gridid.csv"  # 含有 WI_Unique 和 Id 的文件
    OUTPUT_PPEX_FILE = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\gwflow.pumpex"
    OUTPUT_VALID_CSV = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\wells_within_manitowoc_with_gridid_valid.csv"

    SIMULATION_START = "2002/1/1"
    SIMULATION_END = "2024/12/31"
    PUMPING_SCALE_FACTOR = 0.1

    generate_gwflow_ppex(SUMMARY_FILE, GRID_MAPPING_FILE, OUTPUT_PPEX_FILE, OUTPUT_VALID_CSV,
                         SIMULATION_START, SIMULATION_END, PUMPING_SCALE_FACTOR)

