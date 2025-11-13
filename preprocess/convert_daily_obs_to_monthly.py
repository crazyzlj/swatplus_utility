import pandas as pd


def convert_daily_to_monthly_flow(input_file: str, output_file: str, date_col: str, flow_col: str):
    """
    读取逐日流量数据CSV文件，计算每月的平均流量，并保存为新的CSV文件。

    Args:
        input_file (str): 输入的日流量CSV文件名。
        output_file (str): 输出的月平均流量CSV文件名。
        date_col (str): CSV文件中包含日期的列名。
        flow_col (str): CSV文件中包含流量数据的列名。
    """
    print(f"--- 开始处理文件: {input_file} ---")

    # 1. 读取CSV文件
    try:
        # 使用 parse_dates 直接让 pandas 将日期列识别为日期格式
        # 使用 index_col 将日期列设为索引，这是进行时间序列分析的最佳实践
        df = pd.read_csv(
                input_file,
                parse_dates=[date_col],
                index_col=date_col
        )
        print(f"成功读取 {len(df)} 条日流量数据。")
    except FileNotFoundError:
        print(f"错误：输入文件未找到 -> {input_file}")
        return
    except KeyError as e:
        print(f"错误：文件中找不到指定的列名 {e}。请检查您的列名设置。")
        return
    except Exception as e:
        print(f"读取文件时发生错误: {e}")
        return

    # 2. 核心步骤：按月重采样并计算均值
    # .resample('M') 会按日历月对数据进行分组
    # .mean() 会计算每个月分组内所有日流量的平均值
    print("正在按月计算平均流量...")
    monthly_avg_flow = df[flow_col].resample('M').mean()

    # 3. 格式化输出
    # 将结果转换为一个新的DataFrame，方便保存
    output_df = monthly_avg_flow.to_frame()

    # 为了更清晰地展示，可以将索引（每月的最后一天）格式化为'年-月'
    output_df.index = output_df.index.strftime('%Y-%m')

    # 重命名列以反映其内容
    output_df.rename(columns={flow_col: 'Average_Monthly_Flow_m3s'}, inplace=True)

    # 将索引重置为普通列
    output_df.reset_index(inplace=True)
    output_df.rename(columns={date_col: 'Month'}, inplace=True)

    # 4. 保存到新的CSV文件
    try:
        # index=False 表示不将DataFrame的索引写入到文件中
        # float_format='%.3f' 将流量数据格式化为三位小数
        output_df.to_csv(output_file, index=False, float_format='%.3f')
        print(f"\n--- 处理完成！---")
        print(f"已将月平均流量数据保存至: {output_file}")
    except Exception as e:
        print(f"保存文件时发生错误: {e}")


# --- 如何使用此脚本 ---
if __name__ == '__main__':
    # ========================== 用户配置 ==========================

    # 1. 输入的日流量文件名 (请确保此文件与脚本在同一目录下，或提供完整路径)
    INPUT_CSV_FILE = r'D:\data_m\manitowoc\observed\flow_cms_usgs04085427.csv'

    # 2. 输出的月流量文件名
    OUTPUT_CSV_FILE = r'D:\data_m\manitowoc\observed\flow_cms_monthly_usgs04085427.csv'

    # 3. CSV文件中的日期和流量列的名称
    DATE_COLUMN_NAME = 'Date'
    FLOW_COLUMN_NAME = 'Value'

    # =============================================================

    # 调用转换函数
    convert_daily_to_monthly_flow(
            input_file=INPUT_CSV_FILE,
            output_file=OUTPUT_CSV_FILE,
            date_col=DATE_COLUMN_NAME,
            flow_col=FLOW_COLUMN_NAME
    )