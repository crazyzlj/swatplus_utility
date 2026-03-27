import os
import pandas as pd
import logging

# --- 配置 ---
SAMPLES_DIR = r"D:\tmp\wdnr_wells"
COMBINED_OUTPUT = r"D:\tmp\all_wells_monitoring_data.xlsx"
LOG_FILE = r"D:\tmp\combine_samples.log"

logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
logging.getLogger('').addHandler(console)


def combine_well_samples():
    all_data = []

    if not os.path.exists(SAMPLES_DIR):
        logging.error(f"路径不存在: {SAMPLES_DIR}")
        return

    # 1. 识别所有xlsx文件
    files = [f for f in os.listdir(SAMPLES_DIR) if f.endswith('.xlsx')]
    logging.info(f"找到 {len(files)} 个文件，准备开始原样合并...")

    for file_name in files:
        file_path = os.path.join(SAMPLES_DIR, file_name)

        try:
            # 2. 尝试使用 openpyxl 引擎读取
            # 如果文件确实是标准 xlsx，指定 engine='openpyxl' 能解决大部分识别问题
            df = pd.read_excel(file_path, engine='openpyxl')

            if df.empty:
                logging.warning(f"[{file_name}] 为空文件，跳过")
                continue

            # 直接添加，不做任何列插入操作
            all_data.append(df)
            logging.info(f"[{file_name}] 读取成功，行数: {len(df)}")

        except Exception as e:
            # 如果 openpyxl 失败，尝试不指定引擎（让系统自适应）
            try:
                df = pd.read_excel(file_path)
                all_data.append(df)
                logging.info(f"[{file_name}] 通过备选模式读取成功")
            except Exception as e2:
                logging.error(f"[{file_name}] 读取彻底失败: {e2}")

    # 3. 合并数据
    if all_data:
        logging.info("正在执行最终合并...")
        # ignore_index=True 确保合并后的行索引是连续的
        final_df = pd.concat(all_data, ignore_index=True)

        # 写入 Excel
        final_df.to_excel(COMBINED_OUTPUT, index=False)
        logging.info(f"合并完成！总记录数: {len(final_df)}")
        logging.info(f"结果已保存至: {COMBINED_OUTPUT}")
    else:
        logging.warning("没有可合并的数据。")


if __name__ == "__main__":
    combine_well_samples()