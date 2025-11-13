import pandas as pd
import numpy as np
import io


def load_and_prepare_data(filepath_or_data):
    """
    加载并预处理径流数据。
    - 将CSV数据读入DataFrame。
    - 将'Date'列转换为datetime对象并设为索引。
    - 处理可能的缺失值（使用线性插值）。
    """
    try:
        # 尝试将输入作为文件路径读取
        df = pd.read_csv(filepath_or_data)
    except FileNotFoundError:
        # 如果文件未找到，假定输入是字符串数据
        print("从文本数据加载...")
        df = pd.read_csv(io.StringIO(filepath_or_data))

    df['Date'] = pd.to_datetime(df['Date'], format='%Y/%m/%d')
    df.set_index('Date', inplace=True)

    # 使用线性插值填充少数缺失值
    df['Flow'].interpolate(method='linear', inplace=True)

    # 确保数据按时间排序
    df.sort_index(inplace=True)
    return df


def classify_annual_runoff(df):
    """
    计算年径流总量，并根据百分位法划分丰、平、枯水年。
    """
    # 计算每年的平均日径流量，然后乘以当年的天数，以精确计算年总量
    # 这种方法可以优雅地处理闰年
    annual_mean_flow = df['Flow'].resample('YE').mean()
    days_in_year = df.index.to_series().resample('YE').size()
    annual_total_flow = annual_mean_flow * days_in_year

    # 如果数据不是从1月1日开始，第一年的总量可能偏小，最后一个可能不完整
    # 为了准确，我们只使用完整的年份进行统计和划分
    full_years_mask = (days_in_year > 360)
    annual_total_flow = annual_total_flow[full_years_mask]

    if annual_total_flow.empty:
        raise ValueError("没有完整的年份数据可供分析。")

    # 计算整个时期的百分位阈值
    p25 = np.percentile(annual_total_flow, 25)
    p75 = np.percentile(annual_total_flow, 75)

    print("\n--- 水文年型划分标准 ---")
    print(
        f"整个时期 ( {annual_total_flow.index.year.min()} - {annual_total_flow.index.year.max()} ) 的统计阈值:")
    print(f"枯水年 (Dry) 年径流总量 < {p25:.2f}")
    print(f"平水年 (Normal) 年径流总量在 [ {p25:.2f}, {p75:.2f} ] 之间")
    print(f"丰水年 (Wet) 年径流总量 > {p75:.2f}")

    # 创建年度结果DataFrame
    annual_df = pd.DataFrame({
        'Year': annual_total_flow.index.year,
        'AnnualTotalFlow': annual_total_flow.values
    })

    # 应用划分标准
    conditions = [
        annual_df['AnnualTotalFlow'] < p25,
        (annual_df['AnnualTotalFlow'] >= p25) & (annual_df['AnnualTotalFlow'] <= p75),
        annual_df['AnnualTotalFlow'] > p75
    ]
    choices = ['枯水年 (Dry)', '平水年 (Normal)', '丰水年 (Wet)']
    annual_df['Classification'] = np.select(conditions, choices, default='N/A')

    return annual_df


def find_optimal_split(annual_df, start_year, end_year, min_period_len=5):
    """
    在指定时间范围内寻找最佳的率定/验证期分割点。
    """
    target_period_df = annual_df[
        (annual_df['Year'] >= start_year) & (annual_df['Year'] <= end_year)
        ].copy()

    best_split_year = None
    min_imbalance_score = float('inf')

    print(f"\n--- 寻找 {start_year}-{end_year} 最佳分割点 ---")
    print("目标：使两个时期的丰/平/枯水年分布尽可能相似。")

    # 遍历所有可能的分割点
    # 确保分割后的每个时段长度都不小于min_period_len
    for i in range(min_period_len, len(target_period_df) - min_period_len + 1):
        split_year = target_period_df.iloc[i - 1]['Year']

        period1_df = target_period_df.iloc[:i]
        period2_df = target_period_df.iloc[i:]

        # 计算每个时段的丰平枯比例
        dist1 = period1_df['Classification'].value_counts(normalize=True).reindex(
                ['丰水年 (Wet)', '平水年 (Normal)', '枯水年 (Dry)']).fillna(0)
        dist2 = period2_df['Classification'].value_counts(normalize=True).reindex(
                ['丰水年 (Wet)', '平水年 (Normal)', '枯水年 (Dry)']).fillna(0)

        # 计算不均衡评分（各类型比例差异的绝对值之和）
        imbalance_score = np.sum(np.abs(dist1 - dist2))

        print(f"尝试分割点: {split_year} | "
              f"时段1: {period1_df['Year'].min()}-{period1_df['Year'].max()} ({len(period1_df)}年) | "
              f"时段2: {period2_df['Year'].min()}-{period2_df['Year'].max()} ({len(period2_df)}年) | "
              f"不均衡评分: {imbalance_score:.3f}")

        if imbalance_score < min_imbalance_score:
            min_imbalance_score = imbalance_score
            best_split_year = split_year

    return best_split_year


def display_results(annual_df, split_year):
    """
    格式化并展示最终结果。
    """
    print("\n" + "=" * 50)
    print("      第一部分：各年份水文类型划分结果")
    print("=" * 50)
    print(annual_df.to_string(index=False))

    if split_year:
        print("\n" + "=" * 50)
        print("      第二部分：推荐的率定与验证期方案")
        print("=" * 50)
        print(f"分析表明，以 {split_year} 年为分割点最为均衡。")

        period1_start = 2008
        period1_end = split_year
        period2_start = split_year + 1
        period2_end = 2023

        period1_stats = annual_df[annual_df['Year'].between(period1_start, period1_end)]
        period2_stats = annual_df[annual_df['Year'].between(period2_start, period2_end)]

        def get_stats_summary(df, name):
            counts = df['Classification'].value_counts().reindex(
                    ['丰水年 (Wet)', '平水年 (Normal)', '枯水年 (Dry)']).fillna(0).astype(int)
            mean_flow = df['AnnualTotalFlow'].mean()
            return (f"  {name} ({df['Year'].min()}-{df['Year'].max()}, 共{len(df)}年):\n"
                    f"    - 平均年径流总量: {mean_flow:.2f}\n"
                    f"    - 丰/平/枯分布: {counts['丰水年 (Wet)']} / {counts['平水年 (Normal)']} / {counts['枯水年 (Dry)']}")

        stats1_str = get_stats_summary(period1_stats, "时段A")
        stats2_str = get_stats_summary(period2_stats, "时段B")

        print("\n--- 方案一 (先率定, 后验证) ---")
        print(f"建议率定期: {period1_start}-{period1_end}")
        print(f"建议验证期: {period2_start}-{period2_end}")
        print(stats1_str)
        print(stats2_str)

        print("\n--- 方案二 (先验证, 后率定) ---")
        print(f"建议率定期: {period2_start}-{period2_end}")
        print(f"建议验证期: {period1_start}-{period1_end}")
        print(stats2_str)
        print(stats1_str)
    else:
        print("\n未能找到合适的分割点。")


# --- 程序主入口 ---
if __name__ == "__main__":
    # 将您的数据文件名放在这里，或者直接将数据粘贴到下面的字符串中
    # 如果文件在同一目录下，只需写文件名即可
    FILE_PATH = r'D:\data_m\manitowoc\observed\flow_cms_usgs04085427.csv'

    # 生成从1998到2024年的模拟随机数据
    date_range = pd.date_range(start='1998-01-01', end='2024-12-31', freq='D')
    # 创建一个模拟的季节性信号和一些随机性
    flow_values = 10 + 8 * np.sin(2 * np.pi * date_range.dayofyear / 365.25) + np.random.rand(
        len(date_range)) * 5
    # 模拟不同年份的丰枯变化
    year_factors = np.random.uniform(0.5, 1.5, size=(2024 - 1998 + 1))
    flow_values *= np.repeat(year_factors,
                             [len(g) for _, g in pd.Series(flow_values).groupby(date_range.year)])

    simulated_df = pd.DataFrame({'Date': date_range.strftime('%Y/%m/%d'), 'Flow': flow_values})
    SIMULATED_DATA = simulated_df.to_csv(index=False)

    try:
        # 1. 加载数据
        flow_df = load_and_prepare_data(FILE_PATH)
        print(f"成功从文件 '{FILE_PATH}' 加载数据。")
    except (FileNotFoundError, pd.errors.EmptyDataError):
        print(f"警告: 未找到或无法读取文件 '{FILE_PATH}'。将使用内置的模拟数据进行演示。")
        flow_df = load_and_prepare_data(SIMULATED_DATA)

    # 2. 划分水文年型
    annual_classification_df = classify_annual_runoff(flow_df)

    # 3. 寻找最佳分割点
    best_split = find_optimal_split(annual_classification_df, start_year=2008, end_year=2023)

    # 4. 展示所有结果
    display_results(annual_classification_df, best_split)