# 用Python实现从文本文件中读取符合条件的记录，并保存，具体要求如下：
# 1. 程序的参数是输入数据的文件夹TxtInOut，输出数据的文件夹MgtOutExamples
# 2. 在TxtInOut/landuse.lum文件中，读取所有的name，这个文件的格式如下：
# landuse.lum: written by SWAT+ editor v3.0.11 on 2025-11-05 15:26 for SWAT+ rev.61.0.1
# name                         cal_group          plnt_com                                        mgt               cn2         cons_prac             urban            urb_ro           ov_mann              tile               sep               vfs              grww               bmp
# gras_lum                          null         gras_comm                                       null        rc_strow_g     up_down_slope              null              null    convtill_nores              null              null              null              null              null
#
# 其中，第一行是文件名，第二行是表头，第三行开始是数据，只需要提取name列，保存到一个list中，命名为lu_mgt_names
#
# 3. 按照lu_mgt_names，在TxtInOut/hru-data.hru文件中读取第一个出现的数据，并记录它的name，这个文件的格式如下：
#
# hru-data.hru: written by SWAT+ editor v3.0.11 on 2025-11-05 15:25 for SWAT+ rev.61.0.1
#       id  name                          topo             hydro              soil            lu_mgt   soil_plant_init         surf_stor              snow             field
#        1  hru00001              topohru00001          hyd00001            426201          gras_lum        soilplant1              null           snow001              null
#
# 其中，第一行是文件名，第二行是表头，第三行开始是数据，即匹配lu_mgt列的值，如果在lu_mgt_names中，则保存该条数据的name，并把该lu_mgt从lu_mgt_names中移除
#
# 4. 我们现在已经有了{lu_mgt: hru_name}，接下来，根据hru_name的编号部分（即hru09999的编号就是9999）从mgt_out.txt文件中，读取匹配的数据，并以lu_mgt-hru_name.txt的命名方式，保存到MgtOutExamples文件夹中，mgt_out.txt的格式为：
#
#   manitowoc_test30mv4       SWAT+ 2025-11-06        MODULAR Rev 2025.61.0.2.11-346-gab47ce3
#           hru        year         mon         day crop/fert/pest operation  phubase        phuplant  soil_water      plant_bioms   surf_rsd       soil_no3      soil_solp         op_var           var1          var2             var3             var4            var5            var6           var7
#           ---         ---         ---         ---       ---         ---       deg_c           deg_c          mm            kg/ha      kg/ha          kg/ha          kg/ha          ---              ---           ---              ---              ---             ---             ---            ---
#          9593        2002           5           6 fldcult             TILLAGE  9.6706517E-02  0.0000000E+00   174.8746      0.0000000E+00   8609.717       390.7611       96.79048      0.3000000
#
# 其中，第一行是文件信息，第二行是变量名，第三行是变量的单位，第四行开始是数据，其中hru列是HRU的编号，需要注意的是crop/fert/pest这一列有可能是空值，因此读取每行数据时，可以仅分隔hru列即可。
#
# 5. hru-data.hru和mgt_out.txt文件很大，不适合一次读取整个文件，可以逐行读取并处理。
#
import sys
import os
import re


def process_swat_files(input_dir, output_dir):
    """
    处理SWAT+文件，根据landuse.lum和hru-data.hru，
    从mgt_out.txt中提取示例管理文件。
    """

    print(f"--- 脚本开始 ---")
    print(f"输入文件夹: {input_dir}")
    print(f"输出文件夹: {output_dir}")

    # 确保输出文件夹存在
    os.makedirs(output_dir, exist_ok=True)

    # --- 步骤 1 & 2: 从 landuse.lum 读取 name ---
    lu_mgt_names = []
    landuse_file = os.path.join(input_dir, 'landuse.lum')

    print(f"\n[步骤 1] 正在读取 {landuse_file}...")
    try:
        with open(landuse_file, 'r', encoding='utf-8') as f:
            next(f)  # 跳过第一行 (文件名)
            header_line = next(f)  # 读取第二行 (表头)

            # 找到 'name' 列的索引
            header = header_line.split()
            try:
                name_col_index = header.index('name')
            except ValueError:
                print(f"错误: 在 {landuse_file} 中未找到 'name' 列。")
                sys.exit(1)

            # 读取数据
            for line in f:
                cols = line.split()
                if cols:
                    lu_mgt_names.append(cols[name_col_index])

    except FileNotFoundError:
        print(f"错误: 输入文件 {landuse_file} 未找到。")
        sys.exit(1)
    except Exception as e:
        print(f"读取 {landuse_file} 时发生错误: {e}")
        sys.exit(1)

    print(f"从 landuse.lum 找到 {len(lu_mgt_names)} 个 'name'。")

    # --- 步骤 3: 匹配 hru-data.hru ---
    lu_hru_map = {}  # 存储 {lu_mgt: hru_name}
    # 使用 set 提高查找效率，并用于跟踪还需查找的lu_mgt
    lu_mgt_to_find = set(lu_mgt_names)
    hru_data_file = os.path.join(input_dir, 'hru-data.hru')

    print(f"\n[步骤 2] 正在逐行读取 {hru_data_file}...")
    try:
        with open(hru_data_file, 'r', encoding='utf-8') as f:
            next(f)  # 跳过第一行 (文件名)
            header_line = next(f)  # 读取第二行 (表头)

            # 找到 'name' 和 'lu_mgt' 列的索引
            header = header_line.split()
            try:
                name_col_index = header.index('name')
                lu_mgt_col_index = header.index('lu_mgt')
            except ValueError:
                print(f"错误: 在 {hru_data_file} 中未找到 'name' 或 'lu_mgt' 列。")
                sys.exit(1)

            # 逐行读取数据文件
            for line in f:
                # 优化：如果都找到了，就停止读取
                if not lu_mgt_to_find:
                    print("已为所有 landuse name 找到匹配的 HRU。")
                    break

                cols = line.split()
                if cols:
                    lu_mgt = cols[lu_mgt_col_index]

                    # 检查这个 lu_mgt 是否是我们正在寻找的
                    if lu_mgt in lu_mgt_to_find:
                        hru_name = cols[name_col_index]
                        lu_hru_map[lu_mgt] = hru_name

                        # 找到后，将其从待查找集合中移除
                        lu_mgt_to_find.remove(lu_mgt)
                        print(f"  > 匹配: {lu_mgt} -> {hru_name}")

    except FileNotFoundError:
        print(f"错误: 输入文件 {hru_data_file} 未找到。")
        sys.exit(1)
    except Exception as e:
        print(f"读取 {hru_data_file} 时发生错误: {e}")
        sys.exit(1)

    print(f"成功将 {len(lu_hru_map)} 个 landuse 映射到 HRU。")

    # --- 步骤 4: 准备处理 mgt_out.txt ---

    # 创建一个 {hru_id_str: (lu_mgt, hru_name)} 的映射
    # 'hru_id_str' 是从 hru_name 提取的数字ID字符串
    hru_id_map = {}
    for lu_mgt, hru_name in lu_hru_map.items():
        # 使用正则表达式从 hru_name (如 'hru09999') 提取数字
        match = re.search(r'\d+', hru_name)
        if match:
            # 转换为 int 再转回 str 来处理前导零 (例如 '00001' -> '1')
            hru_id_num = int(match.group(0))
            hru_id_str = str(hru_id_num)
            hru_id_map[hru_id_str] = (lu_mgt, hru_name)
        else:
            print(f"警告: 无法从 HRU name '{hru_name}' 提取数字 ID。")

    if not hru_id_map:
        print("未找到可处理的 HRU ID，脚本退出。")
        sys.exit(0)

    print(f"\n[步骤 3] 准备从 mgt_out.txt 提取 {len(hru_id_map)} 个 HRU 的数据...")

    # --- 步骤 5: 逐行处理 mgt_out.txt ---

    mgt_out_file = os.path.join(input_dir, 'mgt_out.txt')
    output_files = {}  # 存储 {hru_id_str: file_handle}
    header_lines = []

    try:
        with open(mgt_out_file, 'r', encoding='utf-8') as f_in:
            # 读取并存储前三行表头
            header_lines.append(next(f_in))  # 第一行 (文件信息)
            header_lines.append(next(f_in))  # 第二行 (变量名)
            header_lines.append(next(f_in))  # 第三行 (单位)

            # 逐行读取数据
            for line in f_in:
                # 仅分割第一个元素 (hru) 来检查
                cols = line.strip().split(maxsplit=1)
                if not cols:
                    continue

                line_hru_id = cols[0]

                # 检查这一行的 hru ID 是否在我们关心的 Hru ID 映射中
                if line_hru_id in hru_id_map:
                    # 如果这是我们第一次遇到这个 HRU ID，
                    # 为它创建一个新文件并写入表头
                    if line_hru_id not in output_files:
                        lu_mgt, hru_name = hru_id_map[line_hru_id]
                        out_filename = f"{lu_mgt}-{hru_name}.txt"
                        out_path = os.path.join(output_dir, out_filename)

                        print(f"  > 创建文件: {out_filename}")
                        f_out = open(out_path, 'w', encoding='utf-8')
                        f_out.writelines(header_lines)
                        output_files[line_hru_id] = f_out

                    # 将当前行写入对应的输出文件
                    output_files[line_hru_id].write(line)

    except FileNotFoundError:
        print(f"错误: 输入文件 {mgt_out_file} 未找到。")
        sys.exit(1)
    except Exception as e:
        print(f"读取 {mgt_out_file} 时发生错误: {e}")
    finally:
        # 确保所有打开的输出文件都被关闭
        for f_out in output_files.values():
            f_out.close()

    print(f"\n[步骤 4] 处理完成。")
    print(f"总共创建了 {len(output_files)} 个管理文件示例。")
    print(f"输出文件夹: {output_dir}")
    print(f"--- 脚本结束 ---")


if __name__ == "__main__":
    input_folder = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\TxtInOut'
    output_folder = r'D:\data_m\manitowoc_test30m\manitowoc_test30mv4\Scenarios\Default\Results\MgtOutExamples'

    process_swat_files(input_folder, output_folder)