#!/usr/bin/env python3  # 指定Python解释器
# -*- coding: utf-8 -*-  # 字符集声明
"""
FPV采集数据合并脚本（修正版 v3.3）
修改内容见下
"""

import pandas as pd  # 引入pandas用于数据处理
import numpy as np  # 引入numpy（尽管本脚本中没用到，通常配合pandas用）
import html  # 导入html库，用于处理转义字符
import warnings  # 用于处理警告
warnings.filterwarnings('ignore')  # 关闭所有警告

# ==================== 配置区（文件路径可修改） ====================
PATH_TABLE1 = "忍者项目记录-合并版.csv"  # 表1的文件路径
PATH_TABLE2 = "FPV-QA用 - 审批操作看板.csv"  # 表2的文件路径
PATH_OUTPUT = "FPV采集数据统计合并结果6.csv"  # 合并结果输出文件
PATH_CLEAN_LOG = "数据清洗对照表.csv"  # 清洗日志文件

# ==================== 数据清洗相关函数 ====================

def clean_device_id(raw_id):
    """清洗设备ID，处理空值、特殊字符和无效内容"""
    if pd.isna(raw_id) or raw_id == '':  # 如果原始ID为空
        return None  # 返回None
    id_str = str(raw_id).strip()  # 转字符串并去除前后空格
    if '登记表' in id_str or '无匹配' in id_str:  # 排除特殊文本
        return None  # 返回None
    id_str = html.unescape(id_str)  # 处理HTML转义字符
    id_str = id_str.replace('"', '').replace("'", '').replace('"', '').replace('"', '')  # 去掉引号
    id_str = id_str.replace(',', '').replace('，', '')  # 去掉中英文逗号
    id_str = id_str.strip().lower()  # 去空格并转小写
    return id_str if id_str else None  # 如果非空则返回，否则返回None

def clean_date(raw_date):
    """标准化日期格式为YYYY-MM-DD"""
    if pd.isna(raw_date) or raw_date == '':  # 空值直接返回None
        return None
    date_str = str(raw_date).strip()  # 转为字符串并去除前后空格
    try:  # 尝试解析日期
        if '/' in date_str:  # 如果包含斜杠
            dt = pd.to_datetime(date_str, format='%Y/%m/%d', errors='coerce')  # 用Y/m/d格式
        elif '-' in date_str:  # 如果包含短横线
            dt = pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')  # 用Y-m-d格式
        else:
            dt = pd.to_datetime(date_str, errors='coerce')  # 用默认pandas解析
        return dt.strftime('%Y-%m-%d') if pd.notna(dt) else None  # 成功则转字符串，否则None
    except:  # 捕获异常
        return None

def clean_column_names(df):
    """去除每一列名的特殊空白字符"""
    cleaned_cols = []  # 新的列名列表
    for col in df.columns:  # 遍历原始列名
        col_clean = str(col).strip()  # 转为字符串并去除前后空格
        col_clean = col_clean.replace('\t', '').replace('\n', '').replace('\r', '').replace('\xa0', '')  # 去掉各种不可见字符
        cleaned_cols.append(col_clean)  # 加入到新列名列表
    df.columns = cleaned_cols  # 替换为新列名
    return df  # 返回处理后的DataFrame

def safe_get_column(df, *possible_names):
    """从DataFrame安全获取列（兼容别名、变化）"""
    for name in possible_names:  # 依次检查可能的列名
        if name in df.columns:
            return df[name]  # 找到则返回
    return None  # 都没有则返回None

def to_numeric_safe(series):
    """安全地将Series转成数值类型，异常变NaN"""
    return pd.to_numeric(series, errors='coerce')  # 返回强转后的结果

def format_number(value, decimals=2):
    """小数格式化，默认保留两位小数"""
    if pd.isna(value):  # 如果是缺失值则返回None
        return None
    try:
        num = float(value)  # 强转浮点数
        return round(num, decimals)  # 保留指定的小数位数
    except:
        return None  # 强转异常则返回None

# ==================== 主流程 ====================

def main():
    print("=" * 70)  # 打印分割线
    print("FPV采集数据合并工具 v3.3（修正版）")  # 打印标题
    print("=" * 70)  # 打印分割线
    print()  # 空行
    
    # ========== Step 1: 读取表1 ==========
    print("Step 1: 读取表1（忍者项目记录）...")  # 打印步骤
    try:
        df_table1 = pd.read_csv(PATH_TABLE1, encoding='utf-8-sig')  # 优先用utf-8读取
    except:
        df_table1 = pd.read_csv(PATH_TABLE1, encoding='gbk')  # 失败则尝试gbk
    df_table1 = clean_column_names(df_table1)  # 清洗列名
    df_table1 = df_table1.dropna(how='all')  # 删除全空行
    print(f"✓ 读取成功，共 {len(df_table1)} 行")  # 打印行数
    
    df_table1['表1_原始日期'] = df_table1['日期']  # 复制“日期”备用
    df_table1['表1_原始设备ID'] = df_table1['摄像头设备号']  # 复制设备号备用
    df_table1['标准日期'] = df_table1['日期'].apply(clean_date)  # 标准日期
    df_table1['标准设备ID'] = df_table1['摄像头设备号'].apply(clean_device_id)  # 标准设备ID
    
    if '截屏采集时长' in df_table1.columns:  # 如果存在“截屏采集时长”列
        df_table1['截屏采集时长'] = to_numeric_safe(df_table1['截屏采集时长'])  # 转成数值型
    
    print(f"  清洗完成：日期缺失 {df_table1['标准日期'].isna().sum()} 条，"
          f"设备ID缺失 {df_table1['标准设备ID'].isna().sum()} 条")  # 打印缺失统计
    print()  # 空行
    
    # ========== Step 2: 读取表2 ==========
    print("Step 2: 读取表2（QA片段）...")  # 打印步骤
    try:
        df_table2 = pd.read_csv(PATH_TABLE2, encoding='utf-8-sig')  # 优先utf-8读取
    except:
        df_table2 = pd.read_csv(PATH_TABLE2, encoding='gbk')  # 失败尝试gbk
    df_table2 = clean_column_names(df_table2)  # 清洗列名
    df_table2 = df_table2.dropna(how='all')  # 清空行
    print(f"✓ 读取成功，共 {len(df_table2)} 行")  # 打印行数
    
    required_cols = ['采集日期', '设备ID', '原始上送时长', '运营端不合格时长']  # 必须要有的列
    missing_cols = [col for col in required_cols if col not in df_table2.columns]  # 检查缺哪些
    if missing_cols:
        print(f"  ❌ 错误：表2缺少必需列:   {missing_cols}")  # 打印报错信息
        print(f"  实际所有列名: {list(df_table2.columns)}")
        return  # 退出后续逻辑
    
    print("  ⚠️ 注意：表2的时长单位是分钟，正在转换为小时...")  # 告知单位已更改
    df_table2['原始上送时长'] = to_numeric_safe(df_table2['原始上送时长']) / 60  # 转小时
    df_table2['运营端不合格时长'] = to_numeric_safe(df_table2['运营端不合格时长']) / 60  # 转小时
    
    df_table2['表2_原始日期'] = df_table2['采集日期']  # 保留原日期
    df_table2['表2_原始设备ID'] = df_table2['设备ID']  # 保留原设备ID
    df_table2['标准日期'] = df_table2['采集日期'].apply(clean_date)  # 清洗后的日期
    df_table2['标准设备ID'] = df_table2['设备ID'].apply(clean_device_id)  # 清洗后的设备ID
    
    print(f"  清洗完成：日期缺失 {df_table2['标准日期'].isna().sum()} 条，"
          f"设备ID缺失 {df_table2['标准设备ID'].isna().sum()} 条")  # 打印缺失统计
    print()
    
    # ========== Step 3: 聚合表2 ==========
    print("Step 3: 聚合表2数据...")  # 打印步骤
    df_table2_valid = df_table2[
        df_table2['标准日期'].notna() & 
        df_table2['标准设备ID'].notna()
    ].copy()  # 只保留日期和设备ID有效的行
    print(f"  有效数据：{len(df_table2_valid)} 行")  # 打印数目
    
    if len(df_table2_valid) > 0:  # 如果有数据
        agg_dict = {
            '原始上送时长': 'sum',  # 汇总同一天同设备的所有“原始上送时长”
            '运营端不合格时长': 'sum',  # 汇总“运营端不合格时长”
            '表2_原始日期': 'first',  # 备用信息取首项
            '表2_原始设备ID': 'first'
        }
        if '算法端可用数据时长' in df_table2_valid.columns:
            agg_dict['算法端可用数据时长'] = 'sum'  # 若存在则加上
        df_table2_agg = df_table2_valid.groupby(
            ['标准日期', '标准设备ID']
        ).agg(agg_dict).reset_index()  # 按清洗后的日期设备ID分组聚合
        print(f"✓ 聚合完成，生成 {len(df_table2_agg)} 条记录")  # 打印新表大小
    else:
        df_table2_agg = pd.DataFrame(columns=[
            '标准日期', '标准设备ID', '原始上送时长', '运营端不合格时长',
            '表2_原始日期', '表2_原始设备ID'
        ])  # 没数据新建空表
        print("  警告：表2无有效数据")
    print()
    
    # ========== Step 4: 关联两表 ==========
    print("Step 4: 关联两表（FULL OUTER JOIN）...")  # 打印步骤
    df_table1_valid = df_table1[
        df_table1['标准日期'].notna() & 
        df_table1['标准设备ID'].notna()
    ].copy()  # 只保留有效数据
    print(f"  表1有效数据：{len(df_table1_valid)} 行")  # 打印数目
    
    df_merged = pd.merge(
        df_table1_valid,
        df_table2_agg,
        on=['标准日期', '标准设备ID'],  # 用标准日期和设备ID对齐
        how='outer',  # 全外连接，所有数据都保留
        indicator=True,  # 增加_merge指示来源
        suffixes=('_t1', '_t2')  # 相同列加后缀
    )
    print(f"✓ 关联完成，共 {len(df_merged)} 条记录")  # 打印合并结果数
    print(f"  - 仅表1：{(df_merged['_merge'] == 'left_only').sum()} 条")  # 打印表1独有
    print(f"  - 仅表2：{(df_merged['_merge'] == 'right_only').sum()} 条")  # 打印表2独有
    print(f"  - 两表都有：{(df_merged['_merge'] == 'both').sum()} 条")  # 打印都有的数量
    print()
    
    # ========== Step 5: 构建输出表 ==========
    print("Step 5: 构建输出表并计算字段...")  # 打印步骤
    df_output = pd.DataFrame()  # 新建输出表
    df_output['采集日期'] = df_merged['标准日期']  # 主键
    df_output['日报截屏上送时长（小时）'] = df_merged.get('截屏采集时长').apply(format_number)  # 格式化日报时长
    
    raw_duration_col = safe_get_column(df_merged, '原始上送时长', '原始上送时长_t2')  # 获取原始上送时长
    unqualified_duration_col = safe_get_column(df_merged, '运营端不合格时长', '运营端不合格时长_t2')  # 获取不合格时长
    
    if raw_duration_col is not None:
        raw_duration_numeric = to_numeric_safe(raw_duration_col)  # 强转数值
        df_output['原始上送时长（小时）'] = raw_duration_numeric.apply(format_number)  # 转小时
        df_output['原始上送时长（分钟）'] = (raw_duration_numeric * 60).apply(format_number)  # 转分钟
    else:
        df_output['原始上送时长（小时）'] = None
        df_output['原始上送时长（分钟）'] = None
    if unqualified_duration_col is not None:
        unqualified_duration_numeric = to_numeric_safe(unqualified_duration_col)
        df_output['运营端不合格时长（小时）'] = unqualified_duration_numeric.apply(format_number)
        df_output['运营端不合格时长（分钟）'] = (unqualified_duration_numeric * 60).apply(format_number)
    else:
        df_output['运营端不合格时长（小时）'] = None
        df_output['运营端不合格时长（分钟）'] = None

    def calc_ratio_percentage(row):
        total = row['原始上送时长（小时）']  # 总时长
        unqualified = row['运营端不合格时长（小时）']  # 不合格时长
        try:
            total_num = float(total) if pd.notna(total) else None
            unq_num = float(unqualified) if pd.notna(unqualified) else None
            if total_num is not None and unq_num is not None and total_num > 0:
                ratio = unq_num / total_num
                return f"{round(ratio * 100, 2)}%"
        except (ValueError, TypeError):
            pass
        return None
    df_output['不合格时长占比'] = df_output.apply(calc_ratio_percentage, axis=1)  # 计算百分比
    
    def calc_difference(row):
        raw = row['原始上送时长（小时）']
        screenshot = row['日报截屏上送时长（小时）']
        try:  
            raw_num = float(raw) if pd.notna(raw) else None
            screenshot_num = float(screenshot) if pd.notna(screenshot) else None
            if raw_num is not None and screenshot_num is not None:
                diff = raw_num - screenshot_num
                return format_number(diff, 2)
        except (ValueError, TypeError):
            pass
        return None
    df_output['数据记录差异'] = df_output.apply(calc_difference, axis=1)  # 计算两表时长差
    
    df_output['状态'] = None  # 状态列留空
    df_output['备注'] = None  # 备注列留空
    
    df_output['设备ID'] = df_merged['标准设备ID']  # 标准设备ID列
    df_output['采集点'] = df_merged.get('采集点')  # 采集点
    df_output['采集点对接人'] = df_merged.get('忍者')  # 采集点对接人=忍者
    df_output = df_output.sort_values(['采集日期', '设备ID'], ascending=[True, True]).reset_index(drop=True)  # 排序
    
    print(f"✓ 输出表构建完成，共 {len(df_output)} 条记录")
    print(f"  - 采集点对接人来源：忍者列")
    print(f"  - 表2时长已从分钟转换为小时")
    print(f"  - 数据记录差异 = 原始上送时长 - 日报截屏上送时长")
    print(f"  - 设备ID、采集点、采集点对接人已移至最后")
    print(f"  - ⚠️ 状态和备注列已留空")
    print()
    
    # ========== Step 6: 输出结果到文件 ==========
    print("Step 6: 保存结果...")
    df_output.to_csv(PATH_OUTPUT, index=False, encoding='utf-8-sig')  # 输出csv
    print(f"✓ 合并结果已保存：{PATH_OUTPUT}")
    print()
    
    # ========== Step 7: 生成清洗对照表 ==========
    print("Step 7: 生成清洗对照表...")
    df_clean_log_t1 = df_table1[['表1_原始日期', '表1_原始设备ID', '标准日期', '标准设备ID']].copy()
    df_clean_log_t1['来源表'] = '表1'
    df_clean_log_t1.rename(columns={'表1_原始日期': '原始日期', '表1_原始设备ID': '原始设备ID'}, inplace=True)
    df_clean_log_t2 = df_table2[['表2_原始日期', '表2_原始设备ID', '标准日期', '标准设备ID']].copy()
    df_clean_log_t2['来源表'] = '表2'
    df_clean_log_t2.rename(columns={'表2_原始日期': '原始日期', '表2_原始设备ID': '原始设备ID'}, inplace=True)
    df_clean_log = pd.concat([df_clean_log_t1, df_clean_log_t2], ignore_index=True)  # 合并两表映射关系
    df_clean_log['需要检查'] = (
        (df_clean_log['原始日期'].astype(str) != df_clean_log['标准日期'].astype(str)) |
        (df_clean_log['原始设备ID'].astype(str) != df_clean_log['标准设备ID'].astype(str)) |
        df_clean_log['标准日期'].isna() |
        df_clean_log['标准设备ID'].isna()
    )  # 判断有无标准化变化或空
    df_clean_log_changed = df_clean_log[df_clean_log['需要检查']].drop_duplicates()
    df_clean_log_changed.to_csv(PATH_CLEAN_LOG, index=False, encoding='utf-8-sig')  # 保存需要检查的映射log
    print(f"✓ 清洗对照表已保存：{PATH_CLEAN_LOG}")
    print(f"  共 {len(df_clean_log_changed)} 条需要检查的记录")
    print()
    
    # ========== 统计摘要 ==========
    print("=" * 70)
    print("✓ 数据合并完成！")
    print("=" * 70)
    print()
    print("📊 数据统计：")
    print(f"  - 总记录数：{len(df_output)}")
    print()
    print("🔧 本次修改内容：")
    print(f"  1. ✅ 采集点对接人 = 忍者（不是采集者）")
    print(f"  2. ✅ 表2时长单位从分钟转换为小时（÷60）")
    print(f"  3. ✅ 数据记录差异 = 原始上送 - 日报截屏")
    print(f"  4. ✅ 设备ID、采集点、采集点对接人移至最后三列")
    print(f"  5. ✅ 状态和备注列留空")
    print()
    print("📁 生成的文件：")
    print(f"  1. {PATH_OUTPUT}")
    print(f"  2. {PATH_CLEAN_LOG}")
    print()

if __name__ == '__main__':  # 仅当直接运行脚本时
    main()  # 执行主流程