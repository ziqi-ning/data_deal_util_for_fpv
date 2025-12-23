import pandas as pd

# 1. 读取数据并清理
df_q = pd.read_csv('QA.csv', dtype=str, encoding='utf-8-sig')
df_9 = pd.read_csv('all.csv', dtype=str, encoding='utf-8-sig')

df_q.fillna("", inplace=True)
df_9.fillna("", inplace=True)

# 清洗对齐，strip首尾空格
df_q['采集日期'] = df_q['采集日期'].str.strip()
df_q['设备ID'] = df_q['设备ID'].str.strip()
df_9['采集日期'] = df_9['采集日期'].str.strip()
df_9['设备ID'] = df_9['设备ID'].str.strip()

# 2. 生成“待审批”键集合
待审批_set = set(
    zip(
        df_q.loc[df_q['审批状态'] == '待审批', '采集日期'],
        df_q.loc[df_q['审批状态'] == '待审批', '设备ID']
    )
)

# 3. 新列赋值
def approval_state(row):
    key = (row['采集日期'], row['设备ID'])
    return "待审批" if key in 待审批_set else ""

df_9_result = df_9.copy()
df_9_result['审批状态'] = df_9_result.apply(approval_state, axis=1)

# 4. 输出新文件
outname = 'result.csv'
df_9_result.to_csv(outname, index=False, encoding='utf-8-sig')
print(f'转换完成，新表已输出到: {outname}')