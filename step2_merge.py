#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FPV采集数据合并脚本（修正版 v3.3）
修改内容：
1. 采集点对接人 = 忍者（不是采集者）
2. 表2的时长单位是分钟，需要÷60转换为小时
3. 数据记录差异 = 原始上送时长 - 日报截屏上送时长
4. 设备ID、采集点、采集点对接人移到最后三列
5. ⚠️ 新增：输出表的"状态"和"备注"列留空
"""

import pandas as pd
import numpy as np
import html
import warnings
warnings.filterwarnings('ignore')

# ==================== 配置区（文件路径可修改） ====================
PATH_TABLE1 = "Ninja.csv"
PATH_TABLE2 = "QA.csv"
PATH_OUTPUT = "all.csv"
PATH_CLEAN_LOG = "数据清洗对照表.csv"

# ==================== 数据清洗函数 ====================

def clean_device_id(raw_id):
    """清洗设备ID"""
    if pd.isna(raw_id) or raw_id == '':
        return None
    
    id_str = str(raw_id).strip()
    
    if '登记表' in id_str or '无匹配' in id_str:  
        return None
    
    id_str = html.unescape(id_str)
    id_str = id_str.replace('"', '').replace("'", '').replace('"', '').replace('"', '')
    id_str = id_str.replace(',', '').replace('，', '')
    id_str = id_str.strip().lower()
    
    return id_str if id_str else None


def clean_date(raw_date):
    """清洗日期：统一为 YYYY-MM-DD"""
    if pd.isna(raw_date) or raw_date == '':
        return None
    
    date_str = str(raw_date).strip()
    
    try:
        if '/' in date_str:  
            dt = pd.to_datetime(date_str, format='%Y/%m/%d', errors='coerce')
        elif '-' in date_str:  
            dt = pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')
        else:
            dt = pd.to_datetime(date_str, errors='coerce')
        
        return dt.strftime('%Y-%m-%d') if pd.notna(dt) else None
    except:  
        return None


def clean_column_names(df):
    """清洗列名"""
    cleaned_cols = []
    for col in df.columns:
        col_clean = str(col).strip()
        col_clean = col_clean.replace('\t', '').replace('\n', '').replace('\r', '').replace('\xa0', '')
        cleaned_cols.append(col_clean)
    
    df.columns = cleaned_cols
    return df


def safe_get_column(df, *possible_names):
    """安全获取列"""
    for name in possible_names:
        if name in df.columns:
            return df[name]
    return None


def to_numeric_safe(series):
    """安全转换为数值类型"""
    return pd.to_numeric(series, errors='coerce')


def format_number(value, decimals=2):
    """格式化数字：保留最多2位小数"""
    if pd.isna(value):
        return None
    try:
        num = float(value)
        return round(num, decimals)
    except:
        return None


# ==================== 主流程 ====================

def main():
    print("=" * 70)
    print("FPV采集数据合并工具 v3.3（修正版）")
    print("=" * 70)
    print()
    
    # ========== Step 1: 读取表1 ==========
    print("Step 1: 读取表1（忍者项目记录）...")
    try:
        df_table1 = pd.read_csv(PATH_TABLE1, encoding='utf-8-sig') 
    except:  
        df_table1 = pd.read_csv(PATH_TABLE1, encoding='gbk')
    
    df_table1 = clean_column_names(df_table1)
    df_table1 = df_table1.dropna(how='all') 
    print(f"✓ 读取成功，共 {len(df_table1)} 行")
    
    df_table1['表1_原始日期'] = df_table1['日期']
    df_table1['表1_原始设备ID'] = df_table1['摄像头设备号']
    df_table1['标准日期'] = df_table1['日期']. apply(clean_date)
    df_table1['标准设备ID'] = df_table1['摄像头设备号'].apply(clean_device_id)
    
    # 表1的时长单位是小时，直接使用
    if '截屏采集时长' in df_table1.columns:
        df_table1['截屏采集时长'] = to_numeric_safe(df_table1['截屏采集时长'])
    
    print(f"  清洗完成：日期缺失 {df_table1['标准日期'].isna().sum()} 条，"
          f"设备ID缺失 {df_table1['标准设备ID'].isna().sum()} 条")
    print()
    
    # ========== Step 2: 读取表2 ==========
    print("Step 2: 读取表2（QA片段）...")
    try:
        df_table2 = pd.read_csv(PATH_TABLE2, encoding='utf-8-sig')
    except:
        df_table2 = pd. read_csv(PATH_TABLE2, encoding='gbk')
    
    df_table2 = clean_column_names(df_table2)
    df_table2 = df_table2.dropna(how='all')
    print(f"✓ 读取成功，共 {len(df_table2)} 行")
    
    required_cols = ['采集日期', '设备ID', '原始上送时长', '运营端不合格时长']
    missing_cols = [col for col in required_cols if col not in df_table2.columns]
    if missing_cols:
        print(f"  ❌ 错误：表2缺少必需列:   {missing_cols}")
        print(f"  实际所有列名: {list(df_table2.columns)}")
        return
    
    # ⚠️ 关键修改：表2的时长单位是分钟，需要÷60转换为小时
    print("  ⚠️ 注意：表2的时长单位是分钟，正在转换为小时...")
    df_table2['原始上送时长'] = to_numeric_safe(df_table2['原始上送时长']) / 60
    df_table2['运营端不合格时长'] = to_numeric_safe(df_table2['运营端不合格时长']) / 60
    
    df_table2['表2_原始日期'] = df_table2['采集日期']
    df_table2['表2_原始设备ID'] = df_table2['设备ID']
    df_table2['标准日期'] = df_table2['采集日期'].apply(clean_date)
    df_table2['标准设备ID'] = df_table2['设备ID'].apply(clean_device_id)
    
    print(f"  清洗完成：日期缺失 {df_table2['标准日期'].isna().sum()} 条，"
          f"设备ID缺失 {df_table2['标准设备ID'].isna().sum()} 条")
    print()
    
    # ========== Step 3: 聚合表2 ==========
    print("Step 3: 聚合表2数据...")
    df_table2_valid = df_table2[
        df_table2['标准日期'].notna() & 
        df_table2['标准设备ID'].notna()
    ].copy()
    
    print(f"  有效数据：{len(df_table2_valid)} 行")
    
    if len(df_table2_valid) > 0:
        agg_dict = {
            '原始上送时长': 'sum',
            '运营端不合格时长': 'sum',
            '表2_原始日期': 'first',
            '表2_原始设备ID': 'first'
        }
        
        if '算法端可用数据时长' in df_table2_valid.columns:
            agg_dict['算法端可用数据时长'] = 'sum'
        # ⚠️ 不再聚合"可接收数据状态"
        
        df_table2_agg = df_table2_valid.groupby(
            ['标准日期', '标准设备ID']
        ).agg(agg_dict).reset_index()
        
        print(f"✓ 聚合完成，生成 {len(df_table2_agg)} 条记录")
    else:
        df_table2_agg = pd.DataFrame(columns=[
            '标准日期', '标准设备ID', '原始上送时长', '运营端不合格时长',
            '表2_原始日期', '表2_原始设备ID'
        ])
        print("  警告：表2无有效数据")
    print()
    
    # ========== Step 4: 关联两表 ==========
    print("Step 4: 关联两表（FULL OUTER JOIN）...")
    df_table1_valid = df_table1[
        df_table1['标准日期'].notna() & 
        df_table1['标准设备ID'].notna()
    ].copy()
    
    print(f"  表1有效数据：{len(df_table1_valid)} 行")
    
    df_merged = pd.merge(
        df_table1_valid,
        df_table2_agg,
        on=['标准日期', '标准设备ID'],
        how='outer',
        indicator=True,
        suffixes=('_t1', '_t2')
    )
    
    print(f"✓ 关联完成，共 {len(df_merged)} 条记录")
    print(f"  - 仅表1：{(df_merged['_merge'] == 'left_only').sum()} 条")
    print(f"  - 仅表2：{(df_merged['_merge'] == 'right_only').sum()} 条")
    print(f"  - 两表都有：{(df_merged['_merge'] == 'both').sum()} 条")
    print()
    
    # ========== Step 5: 构建输出表 ==========
    print("Step 5: 构建输出表并计算字段...")
    
    df_output = pd.DataFrame()
    
    # 关联键
    df_output['采集日期'] = df_merged['标准日期']
    
    # 表1字段
    df_output['日报截屏上送时长（小时）'] = df_merged. get('截屏采集时长').apply(format_number)
    
    # 表2字段（已经转换为小时）
    raw_duration_col = safe_get_column(df_merged, '原始上送时长', '原始上送时长_t2')
    unqualified_duration_col = safe_get_column(df_merged, '运营端不合格时长', '运营端不合格时长_t2')
    
    if raw_duration_col is not None:   
        raw_duration_numeric = to_numeric_safe(raw_duration_col)
        df_output['原始上送时长（小时）'] = raw_duration_numeric. apply(format_number)
        df_output['原始上送时长（分钟）'] = (raw_duration_numeric * 60).apply(format_number)
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

    # 把日报截屏上送时长和原始上送时长的空值都填成0
    df_output['日报截屏上送时长（小时）'] = df_output['日报截屏上送时长（小时）'].fillna(0)
    df_output['原始上送时长（小时）'] = df_output['原始上送时长（小时）'].fillna(0)
    
    # ========== 计算不合格时长占比（百分比） ==========
    def calc_ratio_percentage(row):
        total = row['原始上送时长（小时）']
        unqualified = row['运营端不合格时长（小时）']
        
        try:
            total_num = float(total) if pd.notna(total) else None
            unq_num = float(unqualified) if pd.notna(unqualified) else None
            
            if total_num is not None and unq_num is not None and total_num > 0:
                ratio = unq_num / total_num
                return f"{round(ratio * 100, 2)}%"
        except (ValueError, TypeError):
            pass
        
        return None
    
    df_output['不合格时长占比'] = df_output.apply(calc_ratio_percentage, axis=1)
    
    # ========== 数据记录差异 = 原始上送时长 - 日报截屏上送时长 ==========
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
    
    df_output['数据记录差异'] = df_output.apply(calc_difference, axis=1)
    
    # ========== 把设备ID、采集点、采集点对接人（忍者）移到最后 ==========
    df_output['设备ID'] = df_merged['标准设备ID']
    df_output['采集点'] = df_merged. get('采集点')
    df_output['采集点对接人'] = df_merged. get('忍者')  # 使用"忍者"列
    
    # 排序
    df_output = df_output.sort_values(
        ['采集日期', '设备ID'], 
        ascending=[True, True]
    ).reset_index(drop=True)
    
    print(f"✓ 输出表构建完成，共 {len(df_output)} 条记录")
    print(f"  - 采集点对接人来源：忍者列")
    print(f"  - 表2时长已从分钟转换为小时")
    print(f"  - 数据记录差异 = 原始上送时长 - 日报截屏上送时长")
    print(f"  - 设备ID、采集点、采集点对接人已移至最后")
    print(f"  - ⚠️ 状态和备注列已留空")
    print()
    
    # ========== Step 6: 输出文件 ==========
    print("Step 6: 保存结果...")
    df_output.to_csv(PATH_OUTPUT, index=False, encoding='utf-8-sig')
    print(f"✓ 合并结果已保存：{PATH_OUTPUT}")
    print()
    
    # ========== Step 7: 清洗对照表 ==========
    print("Step 7: 生成清洗对照表...")
    
    df_clean_log_t1 = df_table1[
        ['表1_原始日期', '表1_原始设备ID', '标准日期', '标准设备ID']
    ].copy()
    df_clean_log_t1['来源表'] = '表1'
    df_clean_log_t1.rename(columns={
        '表1_原始日期': '原始日期',
        '表1_原始设备ID':   '原始设备ID'
    }, inplace=True)
    
    df_clean_log_t2 = df_table2[
        ['表2_原始日期', '表2_原始设备ID', '标准日期', '标准设备ID']
    ].copy()
    df_clean_log_t2['来源表'] = '表2'
    df_clean_log_t2.rename(columns={
        '表2_原始日期': '原始日期',
        '表2_原始设备ID':   '原始设备ID'
    }, inplace=True)
    
    df_clean_log = pd.concat([df_clean_log_t1, df_clean_log_t2], ignore_index=True)
    
    df_clean_log['需要检查'] = (
        (df_clean_log['原始日期']. astype(str) != df_clean_log['标准日期'].astype(str)) |
        (df_clean_log['原始设备ID'].astype(str) != df_clean_log['标准设备ID'].astype(str)) |
        df_clean_log['标准日期'].isna() |
        df_clean_log['标准设备ID'].isna()
    )
    
    df_clean_log_changed = df_clean_log[df_clean_log['需要检查']].drop_duplicates()
    df_clean_log_changed. to_csv(PATH_CLEAN_LOG, index=False, encoding='utf-8-sig')
    
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


if __name__ == '__main__':
    main()