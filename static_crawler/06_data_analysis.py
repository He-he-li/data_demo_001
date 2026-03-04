import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

# 设置 matplotlib 支持中文显示 (防止乱码)
# Windows 系统通常使用 'SimHei' (黑体), Mac 使用 'Arial Unicode MS'
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


def analyze_quotes():
    # 1. 自动查找最新的 CSV 文件
    csv_files = glob.glob("quotes_*.csv")

    if not csv_files:
        print("❌ 未找到任何 quotes_*.csv 文件！请先运行爬虫脚本生成数据。")
        return

    # 按修改时间排序，取最新的一个
    latest_file = max(csv_files, key=os.path.getctime)
    print(f"📂 正在分析最新数据文件: {latest_file}")

    # 2. 读取 CSV 数据
    try:
        df = pd.read_csv(latest_file)
        print(f"✅ 成功加载 {len(df)} 条数据！\n")
        print("--- 数据前 5 行预览 ---")
        print(df.head())
        print("\n")
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return

    # 3. 数据清洗与预处理
    # 将 "标签 (Tags)" 列的字符串 "a | b | c" 转换回列表，方便统计
    # 假设列名是 "标签 (Tags)"，如果报错请检查实际列名
    tag_col = "标签 (Tags)"
    author_col = "作者 (Author)"
    text_col = "名言内容 (Text)"

    # 展平所有标签到一个大列表中
    all_tags = []
    for tags_str in df[tag_col]:
        if isinstance(tags_str, str):
            # 分割字符串并去除空格
            tags_list = [t.strip() for t in tags_str.split('|')]
            all_tags.extend(tags_list)

    # 4. 分析任务 A: 作者频次统计
    print("📊 正在统计作者频次...")
    author_counts = df[author_col].value_counts()
    print("--- 作者排行榜 (Top 5) ---")
    print(author_counts.head())

    # 5. 分析任务 B: 热门标签统计
    print("\n🏷️ 正在统计热门标签...")
    tag_series = pd.Series(all_tags)
    tag_counts = tag_series.value_counts()
    print("--- 热门标签排行榜 (Top 5) ---")
    print(tag_counts.head())

    # 6. 分析任务 C: 名言长度分析
    df['length'] = df[text_col].str.len()
    avg_length = df['length'].mean()
    print(f"\n📏 名言平均长度: {avg_length:.2f} 字符")

    # 7. 可视化绘图
    print("\n🎨 正在生成图表... (请等待弹窗)")

    # 创建画布，包含 3 个子图
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('Quotes to Scrape - 数据分析报告', fontsize=16, fontweight='bold')

    # 图 1: 作者频次 (柱状图)
    axes[0].barh(author_counts.index[:10], author_counts.values[:10], color='skyblue')
    axes[0].set_title('Top 10 多产作者', fontsize=12)
    axes[0].set_xlabel('名言数量')
    axes[0].invert_yaxis()  # 让第一名在最上面

    # 图 2: 热门标签 (条形图)
    axes[1].bar(tag_counts.index[:10], tag_counts.values[:10], color='salmon')
    axes[1].set_title('Top 10 热门标签', fontsize=12)
    axes[1].set_ylabel('出现次数')
    axes[1].tick_params(axis='x', rotation=45)  # 旋转标签防止重叠

    # 图 3: 名言长度分布 (直方图)
    axes[2].hist(df['length'], bins=15, color='lightgreen', edgecolor='black', alpha=0.7)
    axes[2].axvline(avg_length, color='red', linestyle='dashed', linewidth=2, label=f'平均值: {avg_length:.1f}')
    axes[2].set_title('名言长度分布', fontsize=12)
    axes[2].set_xlabel('字符数')
    axes[2].set_ylabel('频数')
    axes[2].legend()

    plt.tight_layout()
    plt.show()

    print("✅ 分析完成！图表已显示。")


if __name__ == "__main__":
    analyze_quotes()