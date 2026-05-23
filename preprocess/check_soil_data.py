import pandas as pd
import numpy as np


def validate_swat_soils(input_excel, output_report="Soils_QAQC_Report.xlsx"):
    """
    检查导出的SWAT+土壤Excel数据的合理性，输出错误和警告报告。
    """
    print(f"正在读取数据: {input_excel}")
    df = pd.read_excel(input_excel)

    # 使用列表存储所有检查记录
    report_logs = []

    def log_issue(muid, layer, severity, message):
        report_logs.append({
            "MUID": muid,
            "Layer": layer,
            "Severity": severity,  # 'ERROR' 或 'WARNING'
            "Description": message
        })

    for index, row in df.iterrows():
        muid = row['MUID']
        nlayers = int(row['NLAYERS'])

        # 1. 宏观结构检查
        if not (1 <= nlayers <= 10):
            log_issue(muid, 'All', 'ERROR', f"NLAYERS异常: {nlayers} (必须介于1-10)")
            continue  # 层数异常则跳过层级检查

        valid_hydgrp = ['A', 'B', 'C', 'D']
        hydgrp = str(row['HYDGRP']).strip().upper()
        if hydgrp not in valid_hydgrp:
            log_issue(muid, 'All', 'WARNING',
                      f"HYDGRP非常规: '{hydgrp}' (SWAT+计算CN值需标准A/B/C/D)")

        zmx = row['SOL_ZMX']
        if zmx <= 0:
            log_issue(muid, 'All', 'ERROR', f"SOL_ZMX (总深度) 必须大于0，当前值: {zmx}")
        elif zmx > 3500:
            log_issue(muid, 'All', 'WARNING', f"SOL_ZMX极深: {zmx} mm，请确认是否合理")

        # 记录上一层的深度，用于递增验证
        prev_depth = 0

        # 2. 逐层属性检查
        for i in range(1, nlayers + 1):
            depth = row[f'SOL_Z{i}']
            bd = row[f'SOL_BD{i}']
            awc = row[f'SOL_AWC{i}']
            k = row[f'SOL_K{i}']
            cbn = row[f'SOL_CBN{i}']
            clay = row[f'CLAY{i}']
            silt = row[f'SILT{i}']
            sand = row[f'SAND{i}']
            rock = row[f'ROCK{i}']
            alb = row[f'SOL_ALB{i}']
            usle_k = row[f'USLE_K{i}']
            ph = row[f'SOL_PH{i}']

            # --- 深度逻辑 ---
            if depth <= prev_depth:
                log_issue(muid, i, 'ERROR',
                          f"层深度SOL_Z ({depth}) 小于或等于上一层 ({prev_depth})")
            prev_depth = depth

            # --- 质量守恒检查 ---
            texture_sum = clay + silt + sand
            # 允许 [98, 102] 的舍入误差范围。但如果是全0代表数据缺失
            if texture_sum == 0:
                log_issue(muid, i, 'ERROR', "缺失土壤质地数据 (CLAY+SILT+SAND = 0)")
            elif not (98 <= texture_sum <= 102):
                log_issue(muid, i, 'ERROR', f"质地加和未达100%: {texture_sum:.1f}%")

            if not (0 <= rock <= 100):
                log_issue(muid, i, 'ERROR', f"ROCK比例越界: {rock}%")

            # --- 物理特性检查 ---
            if not (0.1 <= bd <= 2.65):
                log_issue(muid, i, 'ERROR', f"SOL_BD违背物理常识: {bd} g/cm³")
            elif bd > 2.0:
                log_issue(muid, i, 'WARNING', f"SOL_BD容重偏高: {bd} g/cm³")

            if not (0 <= awc <= 1):
                log_issue(muid, i, 'ERROR', f"SOL_AWC越界: {awc} mm/mm")
            elif awc > 0.6:
                log_issue(muid, i, 'WARNING', f"SOL_AWC异常高: {awc} mm/mm")

            if k < 0:
                log_issue(muid, i, 'ERROR', f"SOL_K (导水率) 不能为负: {k}")
            elif k == 0 or k < 0.01:
                log_issue(muid, i, 'WARNING', f"SOL_K极低 ({k})，若非隔水层基岩请检查数据")
            elif k > 2000:
                log_issue(muid, i, 'WARNING', f"SOL_K异常高 ({k} mm/hr)，水文计算可能剧烈震荡")

            if not (0 <= alb <= 1):
                log_issue(muid, i, 'ERROR', f"SOL_ALB (反照率) 必须在0-1之间: {alb}")

            if not (0 <= usle_k <= 1.0):
                log_issue(muid, i, 'ERROR', f"USLE_K越界: {usle_k}")

            # --- 化学特性检查 ---
            if not (0 <= cbn <= 100):
                log_issue(muid, i, 'ERROR', f"SOL_CBN (有机碳) 越界: {cbn}%")
            elif cbn > 30:
                log_issue(muid, i, 'WARNING', f"SOL_CBN极高: {cbn}%，通常非矿质土")

            if not (0 <= ph <= 14):
                log_issue(muid, i, 'ERROR', f"SOL_PH越界: {ph}")
            elif ph == 0:
                log_issue(muid, i, 'WARNING', "SOL_PH为0，通常代表数据库中存在缺失值占位符")

        # 3. 总深度自洽检查
        # 最后一层的深度应当等于 SOL_ZMX
        if nlayers > 0 and abs(prev_depth - zmx) > 1.0:  # 允许1mm误差
            log_issue(muid, 'All', 'ERROR', f"底层深度 ({prev_depth}) 与 SOL_ZMX ({zmx}) 不匹配")

    # 4. 汇总与导出
    if report_logs:
        df_report = pd.DataFrame(report_logs)
        # 按照 MUID 和 严重程度排序
        df_report = df_report.sort_values(by=['MUID', 'Severity'])
        df_report.to_excel(output_report, index=False)
        print(f"数据核查完成！共发现 {len(df_report)} 条异常/警告。请查看生成的报告：{output_report}")
    else:
        print("数据核查完成！未发现任何异常或警告，数据质量极佳。")


if __name__ == "__main__":
    # 使用上一步生成的Excel文件路径
    excel_file = r"D:\data_m\manitowoc\soil\soils_manitowoc.xlsx"
    out_file = r"D:\data_m\manitowoc\soil\soils_manitowoc_checked.xlsx"
    validate_swat_soils(excel_file, output_report=out_file)