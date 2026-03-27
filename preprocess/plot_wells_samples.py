import os
import pandas as pd
import matplotlib.pyplot as plt
import logging

# --- 路径配置 ---
WELL_LIST_CSV = r"D:\tmp\selected_wells_without_pdf.csv"  # 目标水井编号
INVENTORY_EXCEL = r"D:\tmp\selected_wells_inventory.xlsx"  # 水井目录
SAMPLES_DIR = r"D:\tmp\wdnr_wells"  # 存放原始数据文件的文件夹
OUTPUT_ROOT = r"D:\tmp\well_analysis_output"  # 结果输出根目录

# 存储代码对应含义 (用于绘图标题)
PARAM_MAP = {
    4189: "Groundwater level (Storet 4189)",
    631: "Nitrate+Nitrite (Storet 631)"
}

logging.basicConfig(level=logging.INFO, format='%(message)s')


def process_well_data():
    # 1. 加载目标列表和目录
    if not os.path.exists(WELL_LIST_CSV):
        print("未找到 well_list.csv")
        return

    target_ids = pd.read_csv(WELL_LIST_CSV, header=None, dtype=str)[0].str.strip().tolist()
    inventory = pd.read_excel(INVENTORY_EXCEL, dtype={'WI Unique Well #': str})
    inventory['WI Unique Well #'] = inventory['WI Unique Well #'].str.strip()

    # 2. 遍历目标编号
    for well_id in target_ids:
        # 获取 PLSS 信息
        well_info = inventory[inventory['WI Unique Well #'] == well_id]
        if well_info.empty:
            logging.warning(f"[{well_id}] 在名录中未找到")
            continue

        row = well_info.iloc[0]
        # 创建文件夹名: T17N_R20E_S24
        plss_folder_name = f"T{row['Township']}_R{row['Range']}{row['Range Direction']}_S{row['Section']}"
        plss_path = os.path.join(OUTPUT_ROOT, plss_folder_name)

        if not os.path.exists(plss_path):
            os.makedirs(plss_path)

        # 3. 查找样本文件
        sample_file = os.path.join(SAMPLES_DIR, f"{well_id}_Samples.xlsx")
        if not os.path.exists(sample_file):
            continue

        try:
            df = pd.read_excel(sample_file)

            # --- 关键改进：数据类型清洗 ---
            # 1. 处理日期
            if 'Sample Collection Date' in df.columns:
                df['Sample Collection Date'] = pd.to_datetime(df['Sample Collection Date'],
                                                              errors='coerce')

            # 2. 核心修复：强制转换 Storet Code 为整数
            # 使用 pd.to_numeric 处理可能存在的字符串或浮点数，errors='coerce' 会将无法转换的变为空值
            if 'Storet Parameter Code' in df.columns:
                df['Storet Parameter Code'] = pd.to_numeric(df['Storet Parameter Code'],
                                                            errors='coerce').fillna(-1).astype(int)
            else:
                logging.error(f"[{well_id}] 文件中缺少 'Storet Parameter Code' 列")
                continue

            # 4. 针对两个参数绘图
            for p_code, p_name in PARAM_MAP.items():
                subset = df[df['Storet Parameter Code'] == p_code].copy()

                # 剔除 Result 缺失或非数字的数据
                subset['Sample Analytical Result Amount'] = pd.to_numeric(subset['Sample Analytical Result Amount'], errors='coerce')
                subset = subset.dropna(subset=['Sample Collection Date', 'Sample Analytical Result Amount'])

                if subset.empty:
                    continue

                # 排序
                subset = subset.sort_values('Sample Collection Date')

                # 绘图逻辑
                plt.figure(figsize=(10, 6))
                plt.plot(subset['Sample Collection Date'], subset['Sample Analytical Result Amount'],
                         marker='o', markersize=4, linestyle='-', linewidth=1.5, color='tab:blue')

                # 优化展示
                plt.title(f"Well: {well_id}\n{p_name}", fontsize=12, pad=15)
                plt.xlabel("Sample Collection Date", fontsize=10)
                plt.ylabel("Concentration (Result)", fontsize=10)
                plt.grid(True, linestyle=':', alpha=0.6)

                # 自动调整日期显示
                plt.gcf().autofmt_xdate()
                plt.tight_layout()

                # 保存
                img_path = os.path.join(plss_path, f"{well_id}_Storet_{p_code}.jpg")
                plt.savefig(img_path, dpi=200)
                plt.close()

            logging.info(f"[{well_id}] 数据已归类至 {plss_folder_name}")

        except Exception as e:
            logging.error(f"[{well_id}] 处理失败: {e}")


if __name__ == "__main__":
    process_well_data()