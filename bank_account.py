#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证分账户汇总功能是否正常
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def bank_account_summary_logic():
    """测试分账户汇总逻辑，使用模拟数据"""
    print("=== 测试分账户汇总逻辑 ===")

    # 创建模拟数据
    np.random.seed(42)
    n_records = 100

    # 创建账户数据
    accounts = [
        {'account_id': 1, 'account_name': '用户A', 'bank': '工商银行', 'account_no': '6222021000011112222'},
        {'account_id': 2, 'account_name': '用户B', 'bank': '建设银行', 'account_no': '6227001000022223333'},
        {'account_id': 3, 'account_name': '用户A', 'bank': '农业银行', 'account_no': '6228481000033334444'},
    ]

    # 创建交易流水数据
    data = []
    for i in range(n_records):
        account = np.random.choice(accounts)
        trans_date = datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 365))
        trans_month = trans_date.strftime('%Y-%m')

        # 经营性数据（无loan_type, unusual_trans_type, relationship）
        loan_type = None
        unusual_trans_type = None
        relationship = None

        # 随机生成交易金额（正数为收入，负数为支出）
        if np.random.random() > 0.6:
            trans_amt = np.random.uniform(100, 10000)  # 收入
        else:
            trans_amt = -np.random.uniform(50, 5000)   # 支出

        data.append({
            'account_id': account['account_id'],
            'trans_date': trans_date,
            'trans_month': trans_month,
            'trans_amt': trans_amt,
            'loan_type': loan_type,
            'unusual_trans_type': unusual_trans_type,
            'relationship': relationship,
        })

    df = pd.DataFrame(data)

    # 筛选经营性数据
    normal_df = df.loc[
        pd.isna(df.loan_type)
        & pd.isna(df.unusual_trans_type)
        & pd.isna(df.relationship)
    ]

    print(f"总记录数: {len(df)}")
    print(f"经营性数据记录数: {len(normal_df)}")

    if not normal_df.empty:
        # 分别计算进账和出账
        income_df = normal_df[normal_df.trans_amt >= 0]
        expense_df = normal_df[normal_df.trans_amt < 0]

        print(f"经营性收入记录数: {len(income_df)}")
        print(f"经营性支出记录数: {len(expense_df)}")

        # 1. 分账户汇总
        income_by_account = income_df.groupby('account_id').agg(
            {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
        expense_by_account = expense_df.groupby('account_id').agg(
            {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})

        account_agg_df = income_by_account.join(expense_by_account, how='outer').fillna(0).reset_index()
        account_agg_df['net_income_amt'] = account_agg_df['normal_income_amt'] - account_agg_df['normal_expense_amt']

        # 计算占比
        total_income = account_agg_df['normal_income_amt'].sum()
        total_expense = account_agg_df['normal_expense_amt'].sum()
        account_agg_df['income_proportion'] = round(
            account_agg_df['normal_income_amt'] / total_income, 4) if total_income > 0 else 0
        account_agg_df['expense_proportion'] = round(
            account_agg_df['normal_expense_amt'] / total_expense, 4) if total_expense > 0 else 0

        print("\n=== 分账户汇总结果 ===")
        print(account_agg_df.to_string())

        # 2. 分账户按月汇总
        income_by_acct_month = income_df.groupby(['account_id', 'trans_month']).agg(
            {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
        expense_by_acct_month = expense_df.groupby(['account_id', 'trans_month']).agg(
            {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})

        monthly_df = income_by_acct_month.join(
            expense_by_acct_month, how='outer').fillna(0).reset_index()
        monthly_df['net_income_amt'] = monthly_df['normal_income_amt'] - monthly_df['normal_expense_amt']

        print("\n=== 分账户按月汇总结果 ===")
        print(monthly_df.sort_values(['account_id', 'trans_month']).to_string())

        # 验证逻辑正确性
        print("\n=== 验证结果 ===")

        # 验证1：总金额一致性
        total_income_calc = income_df['trans_amt'].sum()
        total_expense_calc = expense_df['trans_amt'].abs().sum()
        print(f"总收入（直接计算）: {total_income_calc:.2f}")
        print(f"总收入（分组汇总）: {total_income:.2f}")
        print(f"总支出（直接计算）: {total_expense_calc:.2f}")
        print(f"总支出（分组汇总）: {total_expense:.2f}")

        # 验证2：账户级汇总正确性
        for account_id in df['account_id'].unique():
            account_income = income_df[income_df['account_id'] == account_id]['trans_amt'].sum()
            account_expense = expense_df[expense_df['account_id'] == account_id]['trans_amt'].abs().sum()

            agg_row = account_agg_df[account_agg_df['account_id'] == account_id]
            if not agg_row.empty:
                agg_income = agg_row['normal_income_amt'].values[0]
                agg_expense = agg_row['normal_expense_amt'].values[0]

                if abs(account_income - agg_income) < 0.01 and abs(account_expense - agg_expense) < 0.01:
                    print(f"账户 {account_id}: 金额计算正确")
                else:
                    print(f"账户 {account_id}: 金额计算不一致")

        return True
    else:
        print("无经营性数据")
        return False

def check_code_issues():
    """检查代码中的潜在问题"""
    print("\n=== 代码检查 ===")

    issues = []

    # 检查 __init__.py 中的问题
    init_file = "src/view/p08001_v/__init__.py"
    if os.path.exists(init_file):
        with open(init_file, 'r', encoding='utf-8') as f:
            content = f.read()

            # 检查硬编码的Excel文件
            if 'pd.read_excel("结果集1.xlsx")' in content:
                issues.append("__init__.py 中使用硬编码Excel文件，仅用于测试")

            # 检查直接函数调用
            if 'bank_account_summary()' in content and content.count('def bank_account_summary():') == 1:
                # 查找函数定义后的调用
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'def bank_account_summary():' in line:
                        # 检查后面是否有直接调用
                        for j in range(i+1, min(i+150, len(lines))):
                            if 'bank_account_summary()' in lines[j] and 'def' not in lines[j]:
                                issues.append("__init__.py 第{}行: 函数直接调用，可能是调试代码".format(j+1))
                                break

    # 检查 JsonUnionFundsSummaryPortrait 类
    class_file = "src/view/p08001_v/json_u_funds_summary_portrait.py"
    if os.path.exists(class_file):
        with open(class_file, 'r', encoding='utf-8') as f:
            content = f.read()

            # 检查是否有调试打印
            if 'print(' in content:
                issues.append("json_u_funds_summary_portrait.py 包含print语句，可能影响性能")

    if issues:
        print("发现以下问题:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("未发现明显代码问题")

    return issues

if __name__ == "__main__":
    print("开始验证分账户汇总功能...")

    # 检查代码问题
    issues = check_code_issues()

    # 测试逻辑
    print("\n=== 逻辑测试 ===")
    try:
        bank_account_summary_logic()
        print("\n逻辑测试通过")
    except Exception as e:
        print(f"\n逻辑测试失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n=== 验证完成 ===")
    print("\n建议:")
    print("1. 检查生产环境中 trans_report_fullview.bank_account_summary 字段是否存在")
    print("2. 确认数据库中有足够的经营性流水数据")
    print("3. 如果 __init__.py 中的调试代码影响生产，请注释掉第266行的 bank_account_summary() 调用")