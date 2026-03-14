#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @FileName  :json_mtr_salary_income_analysis.py
# @Time      :2025/2/25 17:52
# @Author    :chenwen

import pandas as pd
import re
import numpy as np
from typing import Dict, List
from pandas import DateOffset
from view.TransFlow import TransFlow
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)

# 预编译正则表达式
CLEAN_REGEX = re.compile(r'[\d/\\()（）"“”#&]+')
SPACE_REGEX = re.compile(r'\s+')
NON_CHINESE_REGEX = re.compile(r'^[^\u4e00-\u9fff]+|[^\u4e00-\u9fff]+$')
EMPTY_REGEX = re.compile(r'^\s*$')

# 常量定义
SALARY_LABEL = '0101010102'
RISK_THRESHOLD = 50
YEAR_OFFSET = DateOffset(years=1)

# 企业识别模式
COMPANY_PATTERN = re.compile(r'''
^(?: 
    (?:[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领]?\#行政区域)? 
    [\w\u4e00-\u9fff]{2,} # 字号
    (?:[(（]\#行业描述[)）])? 
    组织形式\#公司类型
    |个体户|经营部|工作室|事务所|合作社|分[院局所] 
)$''', re.X)

PERSON_PATTERN = re.compile(r'''
^(?: 
    [·・‧•\w\u4e00-\u9fff]{2,4}$|
    (?:先生|女士|老师|医生|律师|小姐|阿姨|师傅|同学|某[先生女士]) 
)$''', re.X)


class JsonMtrSalaryIncomeAnalysis(TransFlow):
    """薪资收入分析模块"""

    @staticmethod
    def _format_amt(value: float) -> float:
        return round(value, 2) if pd.notnull(value) else 0.00

    @staticmethod
    def _format_month(month_str: str) -> str:
        return f"{month_str[:4]}-{month_str[4:]}" if len(month_str) == 6 else month_str

    @staticmethod
    def _process_datetime(df: pd.DataFrame) -> pd.DataFrame:
        """时间处理优化（使用loc批量操作）"""
        processed_df = df.copy()
        processed_df.loc[:, 'trans_time'] = pd.to_datetime(processed_df['trans_time'])
        processed_df.loc[:, 'year_month'] = processed_df['trans_time'].dt.strftime('%Y-%m')
        return processed_df

    @staticmethod
    def _clean_company_name(name_series: pd.Series) -> pd.Series:
        return (
            name_series.str.replace(CLEAN_REGEX, '', regex=True)  # 去除特殊字符
            .str.replace(SPACE_REGEX, ' ', regex=True)  # 压缩空白字符
            .str.replace(NON_CHINESE_REGEX, '', regex=True)  # 移除非中文字符
            .str.strip()  # 去除首尾空格
            .replace(EMPTY_REGEX, pd.NA, regex=True)  # 空值转换
        )

    @classmethod
    def _is_company(cls, name: str) -> bool:
        if pd.isna(name) or len(name.strip()) < 4:
            return False
        if PERSON_PATTERN.search(name):
            return False
        return bool(
            COMPANY_PATTERN.search(name) or
            re.search(r'公司|厂|店|集团|中心|院|银行|学校|医院|个体|经营部', name) or
            re.search(r'(?:股份|有限|责任|合伙)[公司企业]', name)
        )

    def _prepare_salary_data(self) -> pd.DataFrame:
        """准备薪资数据（优化筛选顺序）"""
        # 第一步：获取原始数据时间范围
        if self.trans_u_flow_portrait.empty:
            return pd.DataFrame()

        # 第二步：筛选近一年数据
        max_time = pd.to_datetime(self.trans_u_flow_portrait['trans_time']).max()
        time_mask = self.trans_u_flow_portrait['trans_time'] >= (max_time - YEAR_OFFSET)
        time_filtered = self.trans_u_flow_portrait.loc[time_mask]

        # 第三步：基础数据筛选
        base_mask = (
                (time_filtered['mutual_exclusion_label'] == SALARY_LABEL) &
                (time_filtered['trans_amt'] > 0)
        )
        base_df = time_filtered.loc[base_mask].copy()
        if base_df.empty:
            return pd.DataFrame()

        # 第四步：时间处理
        processed_df = self._process_datetime(base_df)

        # 第五步：清洗对手方名称
        processed_df.loc[:, 'opponent_name'] = self._clean_company_name(processed_df['opponent_name'])

        # 第六步：企业类型筛选
        company_mask = processed_df['opponent_name'].apply(self._is_company)
        return processed_df.loc[company_mask].dropna(subset=['opponent_name'])

    def _calculate_metrics(self, salary_df: pd.DataFrame) -> Dict:
        """指标计算优化"""
        if salary_df.empty or self.trans_u_flow_portrait.empty:
            return {
                "monthly_avg": 0.0,
                "units": np.array([]),
                "total_salary": 0.0,
                "proportion": 0.0
            }

        total_income = self.trans_u_flow_portrait.loc[self.trans_u_flow_portrait['trans_amt'] > 0, 'trans_amt'].sum()
        salary_total = salary_df['trans_amt'].sum()
        month_count = salary_df['year_month'].nunique()

        return {
            "monthly_avg": salary_total / month_count if month_count > 0 else 0.0,
            "units": salary_df['opponent_name'].dropna().unique(),
            "total_salary": salary_total,
            "proportion": (salary_total / total_income) if total_income > 0 else 0.0
        }

    def _generate_trend_chart(self, salary_df: pd.DataFrame) -> List[Dict]:
        """趋势图生成，无薪资收入月份补0"""
        # 获取原始时间范围
        max_time = pd.to_datetime(self.trans_u_flow_portrait['trans_time']).max()
        start_time = max_time - YEAR_OFFSET

        # 生成完整月份序列（格式：YYYY-MM）
        full_months = pd.date_range(
            start=start_time.replace(day=1),  # 从起始月的第一天开始
            end=max_time,
            freq='MS'
        ).strftime('%Y-%m').tolist()

        # 聚合现有数据
        grouped = salary_df.groupby('year_month', as_index=False)
        aggregated = grouped.agg(total_amt=('trans_amt', 'sum'))

        # 创建完整时间轴DataFrame
        trend_df = pd.DataFrame({'year_month': full_months})

        # 合并实际数据并补零
        merged_df = trend_df.merge(
            aggregated,
            how='left',
            on='year_month'
        ).fillna({'total_amt': 0})

        # 格式化处理
        processed = merged_df.assign(
            mtr_year_month=lambda x: x['year_month'].apply(self._format_month),
            mtr_salary_income_amt=lambda x: x['total_amt'].round(2)
        )

        return processed[['mtr_year_month', 'mtr_salary_income_amt']].to_dict('records')

    def _generate_risk_tips(self, metrics: Dict, salary_df: pd.DataFrame) -> str:
        """风险提示优化"""
        sorted_df = salary_df.sort_values('trans_time')
        risk_tips = []

        if metrics['proportion'] > RISK_THRESHOLD:
            risk_tips.append(f"薪资收入占所有收入的{metrics['proportion']:.2f}%，请关注是否为经营类收入")

        if metrics['units'].size > 0:
            latest_unit = sorted_df['opponent_name'].iloc[-1]
            unit_data = sorted_df[sorted_df['opponent_name'] == latest_unit]

            date_range = (
                f"（{unit_data['year_month'].min()} ~ {unit_data['year_month'].max()}）"
                if unit_data['year_month'].nunique() > 1 else ""
            )
            formatted_amt = self._format_amt(metrics['total_salary'] / 10000)
            risk_tips.append(f"近一年薪资收入为{formatted_amt}万元，最近的发薪单位为{latest_unit}{date_range}")

        return "；".join(risk_tips) if risk_tips else ""

    def process(self):
        result_container = self.variables.setdefault('salary_income_analysis', {
            "salary_income_trend_chart": [],
            "salary_income_avg_amt": 0.00,
            "salary_payroll_unit": "",
            "salary_income_proportion": 0.00,
            "salary_income_risk_tips": ""
        })

        if self.trans_u_flow_portrait is None or self.trans_u_flow_portrait.empty:
            logger.info("无原始交易数据")
            return

        salary_df = self._prepare_salary_data()
        if salary_df.empty:
            logger.info("无符合条件的薪资数据")
            return

        metrics = self._calculate_metrics(salary_df)
        # 20250424 薪资收入占比，调整为4位小数
        result_container.update({
            "salary_income_trend_chart": self._generate_trend_chart(salary_df),
            "salary_income_avg_amt": self._format_amt(metrics['monthly_avg']),
            "salary_payroll_unit": '；'.join(metrics['units']) if metrics['units'].size > 0 else "",
            "salary_income_proportion": round(metrics['proportion'], 4),
            "salary_income_risk_tips": self._generate_risk_tips(metrics, salary_df)
        })
