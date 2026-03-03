import requests
from bs4 import BeautifulSoup
import time


def crawl_static():
    url = "http://quotes.toscrape.com/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    print("🚀 [静态] 开始请求...")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ 请求失败: {response.status_code}")
        return

    # 解析 HTML
    soup = BeautifulSoup(response.text, 'lxml')

    # 定位元素：找到所有 class 为 "quote" 的 div
    quotes = soup.find_all('div', class_='quote')

    data_list = []
    for quote in quotes:
        text = quote.find('span', class_='text').get_text(strip=True)
        author = quote.find('small', class_='author').get_text(strip=True)
        tags = [tag.get_text(strip=True) for tag in quote.find_all('a', class_='tag')]

        data_list.append({
            "text": text,
            "author": author,
            "tags": tags
        })
        print(f"✅ 获取: {text[:30]}... by {author}")

    print(f"🎉 静态爬取完成，共获取 {len(data_list)} 条数据。")
    return data_list


if __name__ == "__main__":
    crawl_static()