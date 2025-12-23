import pandas as pd

# 读取四个CSV文件
df_luo = pd.read_csv('1.csv', encoding='utf-8')
df_zhou = pd.read_csv('2.csv', encoding='utf-8')
df_he = pd.read_csv('3.csv', encoding='utf-8')
df_jiang = pd.read_csv('4.csv', encoding='utf-8')

# 统一列名：只保留前11列（以贺子豪的表为标准，去掉空列）
columns_to_keep = [
    '日期', '采集点', '忍者', '采集者', '树莓派设备号', 
    '摄像头设备号', '开始时间', '结束时间', '截屏采集时长', 
    '原始上送时长', '有效时长'
] # 手动写的列名

# 对每个DataFrame只保留前11列
df_luo = df_luo.iloc[:, :11]
df_zhou = df_zhou.iloc[: , :11]
df_he = df_he.iloc[:, :11]
df_jiang = df_jiang.iloc[: , :11]

# 统一列名（确保列名一致）
df_luo.columns = columns_to_keep
df_zhou.columns = columns_to_keep
df_he.columns = columns_to_keep
df_jiang.columns = columns_to_keep

# 垂直合并四个表（按顺序：罗圣峰、周俊健、贺子豪、江伊潭）
df_merged = pd.concat([df_luo, df_zhou, df_he, df_jiang], ignore_index=True)

# 保存为新的CSV文件
df_merged.to_csv('Ninja.csv', index=False, encoding='utf-8-sig') 

print(f"合并完成！")
print(f"总行数: {len(df_merged)}")
print(f"总列数: {len(df_merged.columns)}")
print(f"\n列名:  {list(df_merged.columns)}")
print(f"\n前5行预览:")
print(df_merged.head())