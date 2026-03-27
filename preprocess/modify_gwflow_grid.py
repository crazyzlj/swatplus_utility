import re


def process_swat_gwflow(input_file, output_file, add_value=3.0):
    """
    专门处理 SWAT+ gwflow 网格数据
    保留首行注释，跳过 0.00，对其他数值增加指定值，并严格保持原有的分隔符。
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        if not lines:
            return

        processed_lines = []
        # 1. 保留第一行注释行
        processed_lines.append(lines[0])

        # 2. 正则表达式：匹配数值及其后的分隔符（空格或制表符）
        # (\d+\.\d+|\d+) 匹配浮点数或整数
        # (\s*) 匹配随后的空白字符
        pattern = re.compile(r'(\d+\.\d+|\d+)(\s*)')

        for line in lines[1:]:
            if not line.strip():
                processed_lines.append(line)
                continue

            def replace_match(match):
                val_str = match.group(1)
                separator = match.group(2)
                val_float = float(val_str)

                # 判断是否为 0.00 (跳过不处理)
                if abs(val_float) < 1e-7:
                    return f"{val_str}{separator}"
                else:
                    # 增加给定值
                    new_val = val_float + add_value

                    # 格式化输出：尝试保留原有的精度位数
                    # 如果原字符包含小数点，统计其小数位数并保持一致
                    if '.' in val_str:
                        precision = len(val_str.split('.')[1])
                        new_val_str = f"{new_val:.{precision}f}"
                    else:
                        new_val_str = str(int(new_val) if new_val.is_integer() else new_val)

                    return f"{new_val_str}{separator}"

            # 对当前行进行全量替换
            new_line = pattern.sub(replace_match, line)
            processed_lines.append(new_line)

        # 3. 写入新文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(processed_lines)

        print(f"成功处理文件！数值已增加 {add_value}，结果保存至: {output_file}")

    except Exception as e:
        print(f"处理过程中出错: {e}")


# 执行处理
if __name__ == "__main__":
    # 针对您的 initial_head 示例
    wp = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4'
    inifile = '%s\gwflow-grid-data-initial_head.txt' % wp
    outfile = '%s\gwflow-grid-data-initial_head_modify.txt' %wp
    process_swat_gwflow(inifile, outfile, add_value=3.0)