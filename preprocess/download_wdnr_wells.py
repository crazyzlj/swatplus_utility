import requests
import os
import time
import pandas as pd  # 如果你的编号在Excel里，需要用到pandas，没有的话可以去掉

# ================= 配置区域 =================
# 模拟你的水井编号列表，实际使用时请替换为从文件读取
# well_ids = ['ACF856', 'ACF857', 'ABC123']

# 或者：从Excel读取编号 (假设Excel有一列叫 'WUWN')
# df = pd.read_excel('your_well_list.xlsx')
# well_ids = df['WUWN'].tolist()

# 保存文件的文件夹
OUTPUT_DIR = r"D:\tmp\wdnr_wells"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

well_id_xlsx = OUTPUT_DIR + os.sep + 'well_ids_test.csv'
df = pd.read_csv(well_id_xlsx)
well_ids = df['WI Unique Well #'].tolist()

# 设置请求头，伪装成浏览器（非常重要，防止被服务器拒绝）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


# ================= 主逻辑 =================

def download_well_data(well_id):
    print(f"[{well_id}] 开始处理...")

    # 创建一个Session对象，它会自动管理Cookies
    session = requests.Session()
    session.headers.update(HEADERS)

    # ------------------------------------------
    # 任务 1: 下载水井建设报告 (PDF)
    # ------------------------------------------
    pdf_url = f"https://apps.dnr.wi.gov/wellconstructionpub/ReportViewer.aspx?id=WellConstructionReport&download=true&WUWN={well_id}"
    pdf_path = os.path.join(OUTPUT_DIR, f"{well_id}_Report.pdf")

    if os.path.exists(pdf_path):
        return

    try:
        r_pdf = session.get(pdf_url, timeout=30)
        if r_pdf.status_code == 200 and len(r_pdf.content) > 1000:  # 简单检查文件大小，避免下载到空文件
            with open(pdf_path, 'wb') as f:
                f.write(r_pdf.content)
            print(f"  - PDF 下载成功")
        else:
            print(f"  - PDF 下载失败 (Status: {r_pdf.status_code})")
    except Exception as e:
        print(f"  - PDF 下载出错: {e}")

    # ------------------------------------------
    # 任务 2: 下载历史采样数据 (XLSX) - 关键步骤
    # ------------------------------------------
    # 第一步：访问详情页，让服务器种下 Session Cookie
    details_url = f"https://apps.dnr.wi.gov/grnext/Samples/Details/{well_id}"
    export_url = "https://apps.dnr.wi.gov/grnext/Samples/Export/All"
    xlsx_path = os.path.join(OUTPUT_DIR, f"{well_id}_Samples.xlsx")

    if os.path.exists(xlsx_path):
        return

    try:
        # 1. 先“看”一眼详情页
        r_step1 = session.get(details_url, timeout=30)

        # 检查是否真的访问到了页面（有时ID不对会重定向到错误页）
        if r_step1.status_code == 200:
            # 2. 带着刚才获得的Cookie去请求下载链接
            r_step2 = session.get(export_url, timeout=30)

            # 检查返回的内容是不是Excel (通过Header或内容头)
            if r_step2.status_code == 200:
                # 这里的逻辑是：如果文件太小可能是个报错页面，根据实际情况调整
                with open(xlsx_path, 'wb') as f:
                    f.write(r_step2.content)
                print(f"  - 采样数据(XLSX) 下载成功")
            else:
                print(f"  - 采样数据下载失败 (Status: {r_step2.status_code})")
        else:
            print(f"  - 无法访问详情页，可能是ID错误")

    except Exception as e:
        print(f"  - 采样数据下载出错: {e}")

    # ------------------------------------------
    # 礼貌性延时
    # ------------------------------------------
    time.sleep(2)  # 建议设置1-2秒，防止请求过快被封IP


# ================= 执行循环 =================
if __name__ == "__main__":
    total = len(well_ids)
    for i, well_id in enumerate(well_ids):
        print(f"进度: {i + 1}/{total}")
        download_well_data(well_id)