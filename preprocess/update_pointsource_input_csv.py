import os
import glob
import pandas as pd

# --- 1. 定义文件夹路径 ---
INPUT_PTSRC_DIR = r'D:\data_m\manitowoc\inoutlets\ptsource_discharge'
INPUT_CSV_DIR = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\processpointsources'
OUTPUT_DIR = r'D:\data_m\manitowoc\inoutlets\ptsource_discharge_swatplus'


def process_ptsrc_files():
    """
    主函数，用于处理所有 .ptsrc 文件并更新对应的 CSV。
    """

    # 确保输出文件夹存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 查找所有 .ptsrc 文件
    ptsrc_files = glob.glob(os.path.join(INPUT_PTSRC_DIR, '*.ptsrc'))

    if not ptsrc_files:
        print(f"在 '{INPUT_PTSRC_DIR}' 文件夹中未找到任何 .ptsrc 文件。")
        return

    print(f"找到 {len(ptsrc_files)} 个 .ptsrc 文件。开始处理...")

    # --- 2. 逐个读取 .ptsrc 文件 ---
    for ptsrc_path in ptsrc_files:
        print(f"\n--- 正在处理: {os.path.basename(ptsrc_path)} ---")

        csv_filename = None
        update_rules = []

        try:
            with open(ptsrc_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()

                    # 忽略注释行和空行
                    if not line or line.startswith('#'):
                        continue

                    # 第一行是 CSV 文件名
                    if csv_filename is None:
                        csv_filename = line
                    else:
                        # 后续行是更新规则
                        update_rules.append(line)

        except Exception as e:
            print(f"读取 {ptsrc_path} 时出错: {e}")
            continue

        if csv_filename is None:
            print(f"文件 {ptsrc_path} 为空或格式不正确（未找到CSV文件名）。")
            continue

        # --- 3. 寻找并读取对应的 CSV 文件 ---
        target_csv_path = os.path.join(INPUT_CSV_DIR, csv_filename)

        if not os.path.exists(target_csv_path):
            print(f"跳过：在 '{INPUT_CSV_DIR}' 中未找到目标CSV文件: {csv_filename}")
            continue

        try:
            df = pd.read_csv(target_csv_path)
            # 确保日期和年份列为整数类型以便比较
            df['yr'] = df['yr'].astype(int)
            df['mo'] = df['mo'].astype(int)
            df['day_mo'] = df['day_mo'].astype(int)

        except Exception as e:
            print(f"读取 {target_csv_path} 时出错: {e}")
            continue

        # 获取所有可更新的环境变量列名
        # 假设 'ob_name' 是最后一个非数据列
        try:
            ob_name_index = list(df.columns).index('ob_name')
            valid_vars = set(df.columns[ob_name_index + 1:])
        except ValueError:
            print(f"CSV {csv_filename} 缺少 'ob_name' 列，无法确定变量。")
            continue

        # --- 4. 应用更新规则 ---

        # 创建一个辅助列 'mmdd_str' (例如 '0401', '1231') 以便进行日期范围比较
        df['mmdd_str'] = df['mo'].astype(str).str.zfill(2) + \
                         df['day_mo'].astype(str).str.zfill(2)

        print(f"正在将 {len(update_rules)} 条规则应用于 {csv_filename}...")

        for rule_str in update_rules:
            try:
                # 解析规则
                variable, year_range, day_range, value_str = rule_str.split(',')
                variable = variable.strip()
                value = float(value_str.strip())

                # 检查变量是否有效
                if variable not in valid_vars:
                    print(f"  - 警告: 变量 '{variable}' 不在CSV列中，跳过此规则。")
                    continue

                # 解析年份范围
                start_yr, end_yr = map(int, year_range.split('-'))
                year_mask = (df['yr'] >= start_yr) & (df['yr'] <= end_yr)

                # 解析日期范围
                if day_range.strip().lower() == 'daily':
                    combined_mask = year_mask
                else:
                    start_md, end_md = day_range.split('-')
                    start_md = start_md.strip()
                    end_md = end_md.strip()

                    # 转换 MM.DD 为 MMDD 字符串 (例如 '04.01' -> '0401')
                    start_mmdd = start_md.replace('.', '')
                    end_mmdd = end_md.replace('.', '')

                    # 转换 MM.DD 为月份整数
                    start_month = int(start_md.split('.')[0])
                    end_month = int(end_md.split('.')[0])

                    if start_month <= end_month:
                        # 情况 A: 非跨年 (例如 04.01 - 05.31)
                        date_mask = (df['mmdd_str'] >= start_mmdd) & \
                                    (df['mmdd_str'] <= end_mmdd)
                    else:
                        # 情况 B: 跨年 (例如 12.01 - 01.31)
                        # 选择 (大于等于 12.01) 或 (小于等于 01.31)
                        date_mask = (df['mmdd_str'] >= start_mmdd) | \
                                    (df['mmdd_str'] <= end_mmdd)

                    combined_mask = year_mask & date_mask

                # 应用更新
                rows_affected = combined_mask.sum()
                if rows_affected > 0:
                    df.loc[combined_mask, variable] = value
                # print(f"  - 规则: {rule_str} -> 更新了 {rows_affected} 行。")

            except Exception as e:
                print(f"  - 错误: 处理规则 '{rule_str}' 时出错: {e}")

        # --- 5. 保存更新后的 CSV ---

        # 删除辅助列
        df = df.drop(columns=['mmdd_str'])

        output_path = os.path.join(OUTPUT_DIR, csv_filename)
        try:
            df.to_csv(output_path, index=False)
            print(f"成功：更新后的文件已保存至: {output_path}")
        except Exception as e:
            print(f"保存 {output_path} 时出错: {e}")

    print("\n所有文件处理完毕。")


# --- 运行主函数 ---
if __name__ == "__main__":
    process_ptsrc_files()