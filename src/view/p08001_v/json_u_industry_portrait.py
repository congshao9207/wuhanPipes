import bisect

import pandas as pd
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from util.common_util import get_industry_risk_level, get_industry_risk_tips
import datetime
from view.p08001_v.industry_metrics import industry_metrics


class JsonUIndustryPortrait(TransFlow):

    def __init__(self):
        super().__init__()
        self.df = pd.DataFrame()
        self.now = datetime.datetime.now()
        self.bal_mean = 0

    def process(self):
        self.variables['industry_info'] = {
            'industry_base_info': {},
            'industry_summary': [],
            'industry_finance': {},
            'up_down_stream': {},
            'industry_key_indicator': []
        }
        self.df = self.trans_u_flow_portrait.copy()
        self.df['trans_date'] = self.df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d'))
        self.df['month'] = self.df['trans_time'].apply(lambda x: format(x, '%Y-%m'))
        # 新增处理类别
        self.df['uni_type_new'] = self.df['mutual_exclusion_label'].apply(lambda x: x[2:6])
        # 新增对主营行业为空的处理
        if self.main_indu is None or self.main_indu == "":
            return
        if not (self.main_indu[:3] == 'H61' or self.main_indu[:2] == '61'):
            return
        self._base_info()
        self._industry_summary()
        self._industry_finance()
        self._up_down_stream()
        self._industry_key_indicator()

    @staticmethod
    def _peak_and_off_season(self, indu_code):
        industry_peak_season = ''
        industry_off_season = ''
        flow_peak_season = ''
        flow_off_season = ''
        """行业淡旺季处理"""
        if indu_code[:3] == 'H61' or indu_code[:2] == '61':
            industry_peak_season = '夏季（6月至8月）、节假日期间以及春季（3月至5月）'
            # industry_off_season = '冬季（12月至2月）'
            # 淡旺季数据
            season_tips = self.variables['operational_analysis']['risk_tips_season']
            if season_tips:
                season_str = season_tips.split(';')
                for tips in season_str:
                    if '旺季' in tips:
                        flow_peak_season = tips
                    else:
                        flow_off_season = tips
        return industry_peak_season, industry_off_season, flow_peak_season, flow_off_season

    def _base_info(self):
        res = dict()
        res['industry_type'] = self.main_indu_name
        (res['industry_peak_season'], res['industry_off_season'],
         res['flow_peak_season'], res['flow_off_season']) = self._peak_and_off_season(self, self.main_indu)
        res['industry_grade'] = get_industry_risk_level(self.main_indu)
        res['industry_hint'] = get_industry_risk_tips(self.main_indu)
        com_busi_info = []
        for ent_code, is_main in self.main_ent.items():
            face_info, share_info = self.company_detail(ent_code, is_main)
            com_busi_info.append({'face_info': face_info, 'share_info': share_info})
        res['com_bus_info'] = com_busi_info
        self.variables['industry_info']['industry_base_info'] = res

    def _industry_summary(self):
        res = []
        if self.df.empty:
            return
        if self.main_indu[:3] == 'H61' or self.main_indu[:2] == '61':
            # 筹资活动出账
            loan_net_amt = -self.df[self.df.uni_type.str.endswith('0202')]['trans_amt'].sum()
            bal_df = self.df.sort_values(by=['trans_date', 'id']). \
                drop_duplicates(subset=['trans_date', 'account_id'], keep='last'). \
                groupby('trans_date').agg(date_bal=pd.NamedAgg('account_balance', 'sum')).reset_index()
            full_date_list = pd.date_range(bal_df['trans_date'].min(), bal_df['trans_date'].max(), freq='D')
            # 日期转为字符串格式
            full_date_list = full_date_list.strftime('%Y-%m-%d').tolist()
            full_date_df = pd.merge(pd.DataFrame({'trans_date': full_date_list}),
                                    bal_df[['trans_date', 'date_bal']], how='left', on='trans_date')
            full_date_df['date_bal'].fillna(method='ffill', inplace=True)
            bal_mean = full_date_df['date_bal'].mean()
            self.bal_mean = bal_mean
            net_loan_prop = round(loan_net_amt / bal_mean, 2) if pd.notna(bal_mean) and bal_mean > 0 else 0
            if net_loan_prop > 2:
                res.append({'variable_name': 'net_loan_prop', 'value': net_loan_prop,
                            'risk_hint': '近一个月贷款净支出超过余额日均两倍', 'risk_level': 'RED'})
            recent_loan_repay = self.df[(self.df.uni_type == '010202') & (self.df.trans_amt > 0) &
                                        (self.df['trans_time'] >= self.now - pd.offsets.DateOffset(months=12)) &
                                        (self.df['trans_time'] <= self.now - pd.offsets.DateOffset(months=10))
                                        ]['trans_amt'].sum()
            recent_loan_prop = round(recent_loan_repay / bal_mean, 2) if pd.notna(bal_mean) and bal_mean > 0 else 0
            if recent_loan_prop > 2:
                res.append({'variable_name': 'recent_loan_prop', 'value': recent_loan_prop,
                            'risk_hint': '未来两个月存在大额还款超过余额日均两倍', 'risk_level': 'RED'})
            private_loan = self.df[self.df.mutual_exclusion_label.isin(['0101020258', '0102020259'])]['trans_amt'].sum()
            private_loan_prop = -private_loan / loan_net_amt if loan_net_amt != 0 else 0
            if private_loan_prop > 0.6:
                res.append({'variable_name': 'private_loan_prop', 'value': private_loan_prop,
                            'risk_hint': '民间借贷负债占比超过60%', 'risk_level': 'RED'})
            total_income = self.df[self.df.uni_type.str.startswith('01')]['trans_amt'].sum()
            total_loan = self.df[(self.df.uni_type.str.endswith('0202')) & (self.df.trans_amt > 0)]['trans_amt'].sum()
            income_loan_prop = total_income / total_loan if total_loan > 0 else 999
            bal_income_prop = bal_mean / total_income if total_income > 0 else 999
            if income_loan_prop < 0.05:
                res.append({'variable_name': 'income_loan_prop', 'value': income_loan_prop,
                            'risk_hint': '销贷比过低', 'risk_level': 'RED'})
            if bal_income_prop < 0.01:
                res.append({'variable_name': 'income_loan_prop', 'value': income_loan_prop,
                            'risk_hint': '余额留存率过低', 'risk_level': 'RED'})
        self.variables['industry_info']['industry_summary'] = res

    def _industry_finance(self):
        res = dict()
        if self.df.empty:
            return
        if self.main_indu[:3] == 'H61' or self.main_indu[:2] == '61':
            avg_monthly_income = self.df[(self.df['trans_amt'] > 0) & (self.df.uni_type_new.astype(str).str.startswith('0101'))]. \
                groupby('month').agg({'trans_amt': 'sum'})['trans_amt'].mean()
            if pd.isna(avg_monthly_income):
                avg_monthly_income = 0
            avg_monthly_expense = self.df[(self.df['trans_amt'] < 0) & (self.df.uni_type_new.astype(str).str.startswith('0201'))]. \
                groupby('month').agg({'trans_amt': 'sum'})['trans_amt'].mean()
            if pd.isna(avg_monthly_expense):
                avg_monthly_expense = 0
            avg_monthly_net_income = avg_monthly_income + avg_monthly_expense
            net_income_ratio = avg_monthly_net_income / avg_monthly_income if avg_monthly_income > 0 else 0
            total_income = self.df[self.df.uni_type_new.astype(str).str.startswith('0101')]['trans_amt'].sum()
            total_expense = self.df[(self.df.uni_type_new.astype(str).str.startswith('0201'))]['trans_amt'].sum()
            net_income = total_income + total_expense
            net_income_balance_ratio = net_income / self.bal_mean if self.bal_mean > 0 else 0
            net_income_ratio_prop = 0.72
            net_income_balance_ratio_prop = 0.58
            income_label_df = self.df[self.df.uni_type_new.astype(str).str.startswith('0101')]. \
                groupby('mutual_exclusion_label', as_index=False). \
                agg(label_code=pd.NamedAgg('mutual_exclusion_label', lambda x: x.tolist()[0]),
                    label_name=pd.NamedAgg('label1', lambda x: x.tolist()[0]),
                    label_remark=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    label_total_amount=pd.NamedAgg('trans_amt', 'sum'),
                    label_amount_prop=pd.NamedAgg('trans_amt', lambda x: round(sum(x) / total_income, 4)
                    if total_income > 0 else 0)).sort_values(by='label_total_amount', ascending=False)
            expense_label_df = self.df[self.df.uni_type_new.astype(str).str.startswith('0201')]. \
                groupby('mutual_exclusion_label', as_index=False). \
                agg(label_code=pd.NamedAgg('mutual_exclusion_label', lambda x: x.tolist()[0]),
                    label_name=pd.NamedAgg('label1', lambda x: x.tolist()[0]),
                    label_remark=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    label_total_amount=pd.NamedAgg('trans_amt', lambda x: abs(sum(x))),  # 取绝对值转为正值
                    label_amount_prop=pd.NamedAgg('trans_amt', lambda x: round(abs(sum(x)) / abs(total_expense), 4)
                    if abs(total_expense) > 0 else 0)).sort_values(by='label_total_amount', ascending=False)
            income_remark_df = self.df[self.df.uni_type_new.astype(str).str.startswith('0101')]. \
                groupby('remark', as_index=False). \
                agg(word_name=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    word_remark=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    word_total_amount=pd.NamedAgg('trans_amt', 'sum'),
                    word_amount_prop=pd.NamedAgg('trans_amt', lambda x: round(sum(x) / total_income, 4)
                    if total_income > 0 else 0)).sort_values(by='word_total_amount', ascending=False)
            expense_remark_df = self.df[self.df.uni_type_new.astype(str).str.startswith('0201')]. \
                groupby('remark', as_index=False). \
                agg(word_name=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    word_remark=pd.NamedAgg('remark', lambda x: x.tolist()[0]),
                    word_total_amount=pd.NamedAgg('trans_amt', lambda x: abs(sum(x))),
                    word_amount_prop=pd.NamedAgg('trans_amt', lambda x: round(abs(sum(x)) / abs(total_expense), 4)
                    if abs(total_expense) > 0 else 0)).sort_values(by='word_total_amount', ascending=False)
            res['avg_monthly_income'] = round(avg_monthly_income, 2)
            res['avg_monthly_expense'] = round(abs(avg_monthly_expense), 2)
            res['avg_monthly_net_income'] = round(avg_monthly_net_income, 2)
            res['net_income_ratio'] = round(net_income_ratio, 4)
            res['net_income_balance_ratio'] = round(net_income_balance_ratio, 4)
            res['net_income_ratio_prop'] = round(net_income_ratio_prop, 4)
            res['net_income_balance_ratio_prop'] = round(net_income_balance_ratio_prop, 4)
            res['top_income_label_type'] = income_label_df.head(5).to_dict('records')
            res['top_expense_label_type'] = expense_label_df.head(5).to_dict('records')
            res['top_income_key_word'] = income_remark_df.head(5).to_dict('records')
            res['top_expense_key_word'] = expense_remark_df.head(5).to_dict('records')
        self.variables['industry_info']['industry_finance'] = res

    def _up_down_stream(self):
        res = dict()
        if self.df.empty:
            return
        if self.main_indu[:3] == 'H61' or self.main_indu[:2] == '61':
            upstream_cnt = self.df[self.df.uni_type_new.astype(str).str.startswith('0101')]['opponent_name'].nunique()
            downstream_cnt = self.df[(self.df.uni_type_new.astype(str).str.startswith('0201'))]['opponent_name'].nunique()
            upstream_point = [10, 14, 85, 16, 14, 13, 9, 31, 56, 26, 101, 126, 304, 19, 23, 37, 23, 103, 56, 19, 58, 47, 73, 143, 33,
                              5, 32, 17, 51, 14, 187, 55, 155, 17, 61, 70, 37, 16, 8, 22, 105, 59, 8, 20, 55, 7, 374, 18, 22, 128, 57,
                              9, 18, 43, 15, 56, 149, 142, 87, 61, 118, 29, 30, 20, 16, 8, 44, 50, 88, 45, 350, 30, 29, 57, 98, 182,
                              27, 70, 153, 266, 10, 30, 65, 11, 33, 36, 15, 27, 65, 25, 27, 169, 4, 23, 33, 299, 22, 35, 79, 63, 80,
                              24, 7, 21, 11, 20, 210, 532, 42, 9, 180, 76, 131, 86, 333, 95, 38, 79, 151, 21, 21, 21, 200, 30, 42, 59,
                              34, 179, 62, 18, 29, 391, 54, 172, 87, 31, 157, 249, 54, 140, 95, 25, 12, 36, 264, 188, 104, 79, 140,
                              118, 71, 52, 63, 26, 131, 45, 27, 48, 21, 120, 229, 57, 80, 103, 144, 82, 10, 169, 100, 109, 309, 105,
                              217, 107, 63, 23, 71, 372, 107, 282
                              ]
            downstream_point = [9, 20, 59, 9, 13, 6, 15, 26, 64, 13, 69, 258, 53, 8, 8, 31, 13, 51, 21, 9, 28, 25, 23, 58, 20, 18, 40,
                                9, 37, 4, 74, 23, 39, 11, 18, 11, 31, 3, 4, 2, 200, 23, 6, 15, 33, 5, 85, 7, 13, 16, 28, 2, 12, 23, 2,
                                14, 187, 158, 88, 25, 51, 15, 77, 10, 13, 2, 36, 10, 44, 23, 16, 21, 16, 19, 82, 117, 6, 42, 17, 169,
                                10, 24, 17, 17, 37, 13, 20, 28, 44, 17, 15, 47, 28, 7, 23, 25, 21, 14, 40, 45, 53, 9, 5, 23, 15, 15,
                                96, 454, 16, 7, 109, 36, 75, 10, 151, 51, 35, 49, 81, 10, 17, 35, 67, 19, 24, 37, 36, 96, 93, 8, 10,
                                201, 24, 94, 26, 22, 65, 73, 35, 92, 44, 12, 15, 16, 105, 35, 37, 63, 28, 22, 26, 16, 19, 15, 35, 30,
                                16, 51, 13, 88, 266, 29, 20, 11, 52, 26, 13, 50, 138, 33, 69, 21, 113, 43, 38, 6, 15, 150, 37, 71
                                ]
            res['upstream_customers'] = upstream_cnt
            res['downstream_customers'] = downstream_cnt
            # 上游客户占比计算
            up_valid_samples = len(upstream_point)
            # 计算当前值在行业样本中的位置占比
            rank_count = sum(1 for x in upstream_point if upstream_cnt >= x)
            res['upstream_cus_prop'] = round(rank_count / up_valid_samples, 2)

            # 下游客户占比计算（相同逻辑）
            down_valid_samples = len(downstream_point)
            rank_count = sum(1 for x in downstream_point if downstream_cnt >= x)
            res['downstream_cus_prop'] = round(rank_count / down_valid_samples, 2)
            # 新增散点图样本数据
            res['upstream_cus_point'] = upstream_point
            res['downstream_cus_point'] = downstream_point
        self.variables['industry_info']['up_down_stream'] = res

    def _build_indicator_dict(self, value, industry_code, indicator_key, name, logic, value_formatter):
        """构建指标字典的工厂方法"""
        # 获取行业基准数据
        percentile, level = self._get_industry_benchmark(
            current_value=value,
            industry_code=industry_code,
            metric_key=indicator_key
        )

        return {
            "key_ind_variable": indicator_key,
            "key_ind_name": name,
            "key_ind_logic": logic,
            "key_ind_value": value_formatter(value),
            "key_ind_prop": f"{percentile}%",
            "key_ind_level": level
        }

    @staticmethod
    def _get_industry_benchmark(current_value, industry_code, metric_key):
        """行业基准分析（公共函数）"""
        # 新增有效性校验
        if pd.isna(current_value):
            return 0, '未知'

        # 获取行业指标数据集
        try:
            samples = industry_metrics[industry_code][metric_key]
        except KeyError:
            return 0, '普通'  # 默认处理

        if not samples:
            return 0, '普通'

        # 计算百分位（使用二分查找优化性能）
        sorted_samples = sorted(samples)
        position = bisect.bisect_left(sorted_samples, current_value)
        percentile = int(round(position / len(sorted_samples) * 100))

        # 确定评价等级（左开右闭区间）
        if percentile >= 90:
            level = "头部"
        elif percentile >= 70:
            level = "优势"
        elif percentile >= 30:
            level = "普通"
        elif percentile >= 10:
            level = "落后"
        else:
            level = "淘汰"

        return percentile, level

    def _industry_key_indicator(self):
        res = []
        if self.df.empty:
            return
        # 获取行业代码
        industry_code = self.main_indu[:3]
        # 关键指标分析
        # 总收入， 总支出
        all_income, all_expense = self._cal_amount(self.df)
        # GOP额，GOP率
        gop_amt, gop_ratio = self._cal_gop(self.df)
        # 处理GOP额指标
        res.append(self._build_indicator_dict(
            value=gop_amt,
            industry_code=industry_code,
            indicator_key='gop额',
            name='GOP额',
            logic='总经营收入 - 总经营成本',
            value_formatter=lambda x: round(x, 2)
        ))

        # 处理GOP率指标
        res.append(self._build_indicator_dict(
            value=gop_ratio,
            industry_code=industry_code,
            indicator_key='gop率',
            name='GOP率',
            logic='GOP额 / 总经营收入',
            value_formatter=lambda x: f"{round(x * 100, 2)}%"
        ))
        self.variables['industry_info']['industry_key_indicator'] = res

    @staticmethod
    def _cal_amount(df):
        all_income = df[df['trans_amt'] > 0]['trans_amt'].sum()
        all_expense = df[df['trans_amt'] < 0]['trans_amt'].sum()
        return all_income, all_expense

    @staticmethod
    def _cal_gop(df):
        gop_amt = df['trans_amt'].sum()
        gop_ratio = gop_amt / df[df['trans_amt'] > 0]['trans_amt'].sum()
        return gop_amt, gop_ratio

    @staticmethod
    def company_detail(idno, is_main):
        sql = "select * from %s where basic_id = (SELECT id FROM info_com_bus_basic WHERE credit_code = '%s' " \
              "and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1)"
        face_df = sql_to_df(sql=sql % ("info_com_bus_face", idno))
        detail = {}
        share = []
        share_df = sql_to_df(sql=sql % ("info_com_bus_shareholder", idno))
        if share_df.shape[0] > 0:
            share_df.rename(columns={'sub_conam': 'share_sub_conan', 'funded_ratio': 'share_funded_ratio',
                                     'con_date': 'share_con_date'}, inplace=True)
            share = share_df[['share_holder_name', 'share_holder_type', 'share_sub_conan', 'share_funded_ratio',
                              'share_con_date']].to_dict('records')
        if face_df.shape[0] > 0:
            face_df.rename(columns={'credit_code': 'ent_code'}, inplace=True)
            # 新增is_main字段
            detail['is_main'] = str(is_main)
            detail['ent_name'] = face_df.loc[0, 'ent_name']
            detail['ent_code'] = face_df.loc[0, 'ent_code']
            detail['fr_name'] = face_df.loc[0, 'fr_name']
            detail['es_date'] = "" if pd.isna(face_df.loc[0, 'es_date']) else \
                format(face_df.loc[0, 'es_date'], '%Y-%m-%d')
            detail['appr_date'] = "" if pd.isna(face_df.loc[0, 'appr_date']) else \
                format(face_df.loc[0, 'appr_date'], '%Y-%m-%d')
            detail['industry_phyname'] = face_df.loc[0, 'industry_phyname']
            detail['basic_address'] = face_df.loc[0, 'address']
            detail['opera_scope'] = face_df.loc[0, 'operate_scope']
            detail['ent_type'] = face_df.loc[0, 'ent_type']
            detail['reg_cap'] = face_df.loc[0, 'reg_cap']
            detail['ent_status'] = face_df.loc[0, 'ent_status']
            open_from = "*" if pd.isna(face_df.loc[0, 'open_from']) else format(face_df.loc[0, 'open_from'], "%Y-%m-%d")
            open_to = "*" if pd.isna(face_df.loc[0, 'open_to']) else format(face_df.loc[0, 'open_to'], "%Y-%m-%d")
            detail['open_date_range'] = open_from + "至" + open_to
        return detail, share
