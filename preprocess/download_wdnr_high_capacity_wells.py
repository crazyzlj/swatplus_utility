import email
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os
import re


def extract_html_from_mhtml(file_path):
    """解析 .mhtml 文件并提取其中的 HTML 纯文本"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            msg = email.message_from_file(f)
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    charset = part.get_content_charset() or 'utf-8'
                    return part.get_payload(decode=True).decode(charset, errors='ignore')
    except Exception as e:
        print(f"读取 MHTML 文件出错: {e}")
    return ""


def get_wells_links(mhtml_path):
    """解析单页，提取井号、链接以及三个新增属性字段"""
    print(f"正在解析 {mhtml_path} ...")
    html_content = extract_html_from_mhtml(mhtml_path)
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    wells_data = []

    # 1. 寻找主表格并智能映射列索引
    idx_wuc, idx_wad, idx_wcd = -1, -1, -1
    tables = soup.find_all('table')
    for table in tables:
        header_row = table.find('tr')
        if header_row:
            headers = [c.get_text(strip=True).lower() for c in header_row.find_all(['th', 'td'])]
            idx_wuc = next((i for i, h in enumerate(headers) if 'water use code' in h), -1)
            idx_wad = next((i for i, h in enumerate(headers) if 'approval date' in h), -1)
            idx_wcd = next((i for i, h in enumerate(headers) if 'construction date' in h), -1)
            if idx_wuc != -1:
                break

    # 2. 寻找详情链接及提取数据
    action_links = soup.find_all('a', string=lambda text: text and "View Water Use" in text.strip())
    for link in action_links:
        href = link.get('href')
        if not href: continue

        if href.startswith('/'):
            href = "https://apps.dnr.wi.gov" + href

        row = link.find_parent('tr')
        if row:
            cols = row.find_all('td')
            well_id = "Unknown"
            for col in cols:
                text = col.get_text(separator=" ", strip=True)
                match = re.search(r'([A-Za-z]{2,3}\d{3,4})', text)
                if match:
                    well_id = match.group(1).upper()
                    break

            if well_id == "Unknown":
                continue
            water_use_code = cols[idx_wuc].get_text(strip=True) if idx_wuc != -1 and idx_wuc < len(
                cols) else ""
            well_approval_date = cols[idx_wad].get_text(
                strip=True) if idx_wad != -1 and idx_wad < len(cols) else ""
            well_construction_date = cols[idx_wcd].get_text(
                strip=True) if idx_wcd != -1 and idx_wcd < len(cols) else ""

            wells_data.append({
                'well_id': well_id.upper(),  # 统一大写以防匹配失败
                'url': href,
                'water_use_code': water_use_code,
                'well_approval_date': well_approval_date,
                'well_construction_date': well_construction_date
            })
    return wells_data


def fix_dataframe_header(df):
    """
    智能表头修复函数：
    扫描 DataFrame 的前几行，寻找包含 "Field Name"、"Month" 或年份的行。
    找到后，将其设为真正的列名，并丢弃上面的无用行。
    """
    # 将现有的列名当作第一行数据，防止真正的表头被 pandas 误读为列名
    temp_df = pd.DataFrame(pd.np.vstack([df.columns, df]) if hasattr(pd, 'np') else [
                                                                                        df.columns.values] + df.values.tolist())

    header_idx = -1
    for i, row in temp_df.iterrows():
        # 将整行转为小写字符串进行特征识别
        row_str = " ".join([str(x).lower() for x in row])
        if "field name" in row_str or "month" in row_str or re.search(r'\b(19|20)\d{2}\b', row_str):
            header_idx = i
            break

    if header_idx != -1:
        # 提取真实表头
        new_cols = [str(x).strip() for x in temp_df.iloc[header_idx].values]
        # 处理空列名
        new_cols = [c if c.lower() not in ['nan', 'none', ''] else f"Unnamed_{j}" for j, c in
                    enumerate(new_cols)]
        # 重建干净的 DataFrame
        df_fixed = pd.DataFrame(temp_df.iloc[header_idx + 1:].values, columns=new_cols)
        return df_fixed.dropna(how='all')

    return df  # 如果没找到特征行，返回原表


def fetch_and_save_well_data(wells_data, output_dir):
    """遍历获取数据，修复表头，保存 Excel，并聚合成总表"""
    os.makedirs(output_dir, exist_ok=True)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

    summary_records = []
    months_keys = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov',
                   'Dec']
    months_full = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august',
                   'september', 'october', 'november', 'december']

    for item in wells_data:
        well_id = item['well_id']
        url = item['url']
        excel_filename = os.path.join(output_dir, f"{well_id}.xlsx")

        target_df = None
        from_local = False

        # 1. 优先读取本地缓存 (统一使用 header=None 以防原文件表头错乱)
        if os.path.exists(excel_filename):
            print(f"[{well_id}] 发现本地缓存，进行读取与修复...")
            try:
                target_df = pd.read_excel(excel_filename, header=None)
                from_local = True
            except Exception as e:
                print(f"[{well_id}] 本地文件读取失败，将重新下载。")
                target_df = None

        # 2. 如果无本地文件则发起网络请求
        if target_df is None:
            print(f"[{well_id}] 正在从网络下载数据...")
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()

                # 同样使用 header=None 防止 pandas 盲目抓取表头
                dfs = pd.read_html(response.text, header=None)
                if dfs:
                    for df in dfs:
                        if len(df.columns) >= 2:
                            target_df = df
                            break
                    if target_df is None: target_df = dfs[0]
                time.sleep(1)
            except Exception as e:
                print(f"[{well_id}] 下载或解析出错: {e}")
                continue

                # ==========================================
        # 核心修复区：对 df 应用智能表头定位并清洗
        # ==========================================
        if target_df is not None:
            target_df = fix_dataframe_header(target_df)
            # 无论数据来源，均保存一份修复后的、干净的 Excel 覆盖旧文件
            target_df.to_excel(excel_filename, index=False)

        # ==========================================
        # 聚合提取逻辑
        # ==========================================
        try:
            year_cols = [c for c in target_df.columns if re.search(r'\b(19|20)\d{2}\b', str(c))]

            if not year_cols:
                print(f"[{well_id}] 未能识别到年份列，可能无数据，跳过汇总。")
                continue

            # 获取包含月份名称的那一列 (通常是修复后的第一列)
            month_col = target_df.columns[0]

            for y_col in year_cols:
                year_val = re.search(r'\b((19|20)\d{2})\b', str(y_col)).group(1)
                record = {
                    'UniqueID-Year': f"{well_id}-{year_val}",
                    'Water Use Code': item['water_use_code'],
                    'Well Approval Date': item['well_approval_date'],
                    'Well Construction Date': item['well_construction_date']
                }
                for m in months_keys: record[m] = None

                has_valid_data = False

                for _, row in target_df.iterrows():
                    row_month_text = str(row[month_col]).strip().lower()

                    matched_month_key = None
                    for i, m_full in enumerate(months_full):
                        # 同时支持全称和三字母缩写匹配
                        if m_full in row_month_text or months_keys[i].lower() == row_month_text[:3]:
                            matched_month_key = months_keys[i]
                            break

                    if matched_month_key:
                        raw_val = row[y_col]
                        val = pd.to_numeric(str(raw_val).replace(',', ''), errors='coerce')
                        if pd.notna(val):
                            record[matched_month_key] = val
                            has_valid_data = True

                if has_valid_data:
                    summary_records.append(record)

        except Exception as e:
            print(f"[{well_id}] 聚合数据时出错: {e}")

    # 保存总表
    if summary_records:
        summary_df = pd.DataFrame(summary_records)
        summary_filename = os.path.join(output_dir, "Summary_All_Wells.xlsx")
        summary_df.to_excel(summary_filename, index=False)
        print(f"\n聚合完成！共整合了 {len(summary_df)} 条年度数据，已保存至: {summary_filename}")
    else:
        print("\n未提取到任何有效的汇总数据。")


if __name__ == "__main__":
    # ================= 配置区 =================
    input_files = [r"D:\data_m\manitowoc\groundwater\high_capacity_wells\Source1.mhtml",
                   r"D:\data_m\manitowoc\groundwater\high_capacity_wells\Source2.mhtml"]

    # 指定输出文件夹的名称或路径
    output_folder = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\results"
    filter_csv_path = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\wells_within_manitowoc_with_gridid.csv"
    output_csv_path = r"D:\data_m\manitowoc\groundwater\high_capacity_wells\wells_within_manitowoc_with_gridid_withdates.csv"
    # ==========================================

    print(">>> 步骤 1: 读取研究区白名单 CSV ...")
    valid_wells_set = set()
    df_filter = None
    if os.path.exists(filter_csv_path):
        df_filter = pd.read_csv(filter_csv_path)
        if 'WI_Unique' in df_filter.columns:
            # 全部转大写并去空隙，保证匹配精度
            valid_wells_set = set(
                df_filter['WI_Unique'].dropna().astype(str).str.strip().str.upper())
            print(f"发现 {len(valid_wells_set)} 个有效的研究区井号 (WI_Unique)。")
        else:
            print(f"错误: {filter_csv_path} 中未找到 'WI_Unique' 列！")
    else:
        print(f"警告: 未找到 {filter_csv_path}，将不进行过滤。")

    print("\n>>> 步骤 2: 解析 MHTML 提取元数据 ...")
    all_extracted_wells_dict = {}
    for file_path in input_files:
        if os.path.exists(file_path):
            wells = get_wells_links(file_path)
            for w in wells:
                all_extracted_wells_dict[w['well_id']] = w
        else:
            print(f"警告: 未找到 MHTML 文件 '{file_path}'。")

    print("\n>>> 步骤 3: 结合 CSV 白名单生成附带日期的 CSV ...")
    if df_filter is not None and not df_filter.empty:
        def get_meta(w_id, key):
            w_id_upper = str(w_id).strip().upper()
            return all_extracted_wells_dict.get(w_id_upper, {}).get(key, "")


        # 1. 提取并填充元数据
        df_filter['Water Use Code'] = df_filter['WI_Unique'].apply(
                lambda x: get_meta(x, 'water_use_code'))
        df_filter['Well Approval Date'] = df_filter['WI_Unique'].apply(
                lambda x: get_meta(x, 'well_approval_date'))
        df_filter['Well Construction Date'] = df_filter['WI_Unique'].apply(
                lambda x: get_meta(x, 'well_construction_date'))

        # 2. 核心修改：数据过滤
        # 获取从网页中成功提取到的所有 Unique Well IDs 集合
        extracted_well_ids = [str(k).strip().upper() for k in all_extracted_wells_dict.keys()]

        # 利用 isin() 过滤 DataFrame，仅保留在提取列表中的行
        df_matched_only = df_filter[
            df_filter['WI_Unique'].astype(str).str.strip().str.upper().isin(extracted_well_ids)]

        # 3. 导出有效数据
        if not df_matched_only.empty:
            df_matched_only.to_csv(output_csv_path, index=False)
            print(
                f"已生成带元数据的新 CSV: {output_csv_path} (有效匹配 {len(df_matched_only)} 个井口)")
        else:
            print("警告：白名单中的井号与网页数据均未匹配，未生成包含日期的 CSV 文件。")

    print("\n>>> 步骤 4: 开始下载及整合详细用水量数据 ...")
    # 按照白名单过滤将要抓取的队列
    final_wells_list = []
    for w_id, data in all_extracted_wells_dict.items():
        if valid_wells_set:
            if w_id in valid_wells_set:
                final_wells_list.append(data)
        else:
            # 如果没找到CSV，就不过滤，全量抓取
            final_wells_list.append(data)

    if final_wells_list:
        print(f"过滤后，共有 {len(final_wells_list)} 个目标井需要处理数据。")
        fetch_and_save_well_data(final_wells_list, output_dir=output_folder)
    else:
        print("未找到任何在研究区内的目标井数据，程序结束。")