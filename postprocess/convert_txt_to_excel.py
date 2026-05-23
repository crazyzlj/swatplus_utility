from __future__ import absolute_import
import os
import sys

if os.path.abspath(os.path.join(sys.path[0], '..')) not in sys.path:
    sys.path.insert(0, os.path.abspath(os.path.join(sys.path[0], '..')))

import pandas as pd


def swat_txt_to_excel(txt_file):
    """
    将 SWAT+ 的文本输出文件（如 basin_pw_day.txt）转换为 Excel
    """
    if not os.path.exists(txt_file):
        print(f"错误: 找不到文件 {txt_file}")
        return

    # 1. 获取表头（第2行）
    with open(txt_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        # lines[1] 是表头行，strip() 去除换行符，split() 按空格分割
        headers = lines[1].strip().split()

    # 2. 读取数据
    # sep='\s+' 表示匹配一个或多个空格
    # skiprows=3 表示跳过前3行（标题行、表头行、单位行）
    # names=headers 指定表头
    df = pd.read_csv(txt_file, sep='\s+', skiprows=3, names=headers, index_col=False)

    # 3. 输出 Excel 文件名
    excel_file = txt_file.replace('.txt', '.xlsx')

    # 4. 保存为 Excel

    df.to_excel(excel_file, index=False)
    print(f"成功: {txt_file} -> {excel_file}")


if __name__ == '__main__':
    files_to_convert = [
    # r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0512-snow-hole-cdut\gwflow_cell_wb_yr.txt'
                        r'D:\data_m\manitowoc_test30m\manitowoc_test30mv5\Scenarios\Default\TxtInOut-0522-snow-cc3.12\gwflow_basin_wb_day.txt'
                        ]

    for file in files_to_convert:
        swat_txt_to_excel(file)