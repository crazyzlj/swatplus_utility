import pandas as pd
import numpy as np


def reshape_single_col_to_matrix(input_csv, output_txt, n_cols):
    """
    将单列 CSV 转换为矩阵格式的文本文件。

    :param input_csv: 输入的csv文件路径
    :param output_txt: 输出的txt文件路径
    :param n_cols: 你想要的目标列数 (n)
    """
    try:
        # 1. 读取数据 (header=None 表示没有标题行)
        df = pd.read_csv(input_csv, header=None)

        # 将数据展平为一维数组
        data = df.values.flatten()
        total_count = len(data)

        print(f"读取到数据总个数: {total_count}")

        # 2. 检查数据量是否能整除列数
        if total_count % n_cols != 0:
            print(f"错误: 数据总数 ({total_count}) 不能被列数 ({n_cols}) 整除！")
            print(f"余数为: {total_count % n_cols}，请检查数据完整性或修改列数。")
            return

        # 3. 自动计算行数 (m) 并重塑矩阵
        # reshape(-1, n_cols) 中 -1 代表让 numpy 自动计算行数
        m_rows = total_count // n_cols
        matrix = data.reshape(m_rows, n_cols)

        print(f"转换成功: 生成 {m_rows} 行 x {n_cols} 列 的矩阵")

        # 4. 保存为文本文件，使用 Tab (\t) 分割
        # fmt='%s' 保持原始数据格式（如果是整数就是整数，浮点就是浮点）
        np.savetxt(output_txt, matrix, delimiter='\t', fmt='%s')

        print(f"文件已保存至: {output_txt}")

    except FileNotFoundError:
        print(f"找不到文件: {input_csv}")
    except Exception as e:
        print(f"发生未知错误: {e}")


if __name__ == '__main__':
    # --- 用户配置区域 ---
    input_file = 'data.csv'  # 你的输入文件名
    output_file = 'output.txt'  # 你的输出文件名
    n_columns = 129  # 【重要】在这里修改你想要的列数 (n)
    # ------------------

    reshape_single_col_to_matrix(input_file, output_file, n_columns)