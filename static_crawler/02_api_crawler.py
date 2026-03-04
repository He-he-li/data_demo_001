import requests
import re
import json
import csv
import os
from datetime import datetime


def crawl_and_save():
    url = "http://quotes.toscrape.com/js/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"🚀 正在请求: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        html = response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求失败: {e}")
        return

    # 1. 提取内联 JSON 数据
    pattern = r'<script[^>]*>([\s\S]*?"text"[\s\S]*?)</script>'
    matches = re.findall(pattern, html)

    if not matches:
        print("❌ 未找到包含数据的 script 标签")
        return

    script_content = matches[0]
    json_match = re.search(r'var\s+data\s*=\s*(\[.*?\]);', script_content, re.DOTALL)

    if not json_match:
        print("❌ 正则匹配 JSON 失败")
        return

    try:
        data = json.loads(json_match.group(1))
        print(f"🎉 成功解析 {len(data)} 条数据！")
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败: {e}")
        return

    # 2. 准备保存文件
    # 生成带时间戳的文件名，例如：quotes_20260304_133000.csv
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"quotes_{timestamp}.csv"

    # 确保当前目录下有 output 文件夹（可选，这里直接保存在当前目录）
    # if not os.path.exists('output'): os.makedirs('output')
    # filepath = os.path.join('output', filename)
    filepath = filename

    print(f"💾 正在保存数据到: {filepath} ...")

    # 3. 写入 CSV
    # encoding='utf-8-sig' 是为了让 Excel 能正确显示中文，不会乱码
    # newline='' 是为了防止在 Windows 下出现空行
    try:
        with open(filepath, mode='w', encoding='utf-8-sig', newline='') as file:
            writer = csv.writer(file)

            # 写入表头
            writer.writerow(["ID", "名言内容 (Text)", "作者 (Author)", "标签 (Tags)"])

            for i, item in enumerate(data, 1):
                # 处理数据：解码 Unicode 引号，将标签列表转为字符串
                text = item['text'].replace('\\u201c', '"').replace('\\u201d', '"')
                author = item['author']['name']
                tags = " | ".join(item['tags'])  # 用 | 分隔多个标签

                # 写入一行
                writer.writerow([i, text, author, tags])

        print(f"✅ 保存成功！共写入 {len(data)} 条记录。")
        print(f"📂 文件位置: {os.path.abspath(filepath)}")

        # 询问是否直接用 Excel 打开 (仅限 Windows)
        if os.name == 'nt':
            open_file = input("\n是否立即用 Excel 打开该文件？(y/n): ").strip().lower()
            if open_file == 'y':
                os.startfile(filepath)

    except Exception as e:
        print(f"❌ 文件保存失败: {e}")


if __name__ == "__main__":
    crawl_and_save()