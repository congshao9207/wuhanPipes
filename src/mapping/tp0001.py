# @Time : 12/11/20 2:58 PM 
# @Author : lixiaobo
# @File : tp00001.py.py 
# @Software: PyCharm
import datetime
import json
import pandas as pd
from pandas.tseries import offsets

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df

logger = LoggerUtil().logger(__name__)


class Tp0001(Transformer):
    """
    违约模型
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'performance_amount_index': None,  # 履约金额综合指数
            'oth_loan_reason_else_6m': 0,  # 6个月内其他查询原因查询次数
            'overdue_biggest_money_history_day': None,  # 逾期最大金额距今最早天数
            'cts_app_145': None,  # 9个月内贷款类APP卸载数量
            'app_platform_month6': None,  # 近6月申请机构数
            'main_spouse_gap': 0  # 主体配偶等级差
        }
        self.qh_data = None
        self.jg_data = None
        self.yf_backtracking = None
        self.yf_statistic = None

    def basic_data(self):
        qh_sql = """
            select create_time, detail_info_data from info_other_loan_summary where user_name = %(user_name)s and 
            id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id  desc limit 1"""
        jg_sql = """
            select * from info_risk_cts_item where risk_cts_id = (
            select id from info_risk_cts where mobile = %(mobile)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id  desc limit 1)
        """
        yf_backtracking_sql = """
            select * from info_risk_backtracking_item where risk_backtracking_id = 
            (select id from info_risk_backtracking where mobile = %(mobile)s and 
            id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id  desc limit 1)
        """
        yf_statistic_sql = """
            select * from info_loan_statistics_item where loan_statistics_id = 
            (select id from info_loan_statistics where mobile = %(mobile)s and
            id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id  desc limit 1)
        """
        self.qh_data = sql_to_df(sql=qh_sql, params={'user_name': self.user_name,
                                                     'id_card_no': self.id_card_no})
        self.jg_data = sql_to_df(sql=jg_sql, params={'mobile': self.phone})
        self.yf_backtracking = sql_to_df(sql=yf_backtracking_sql, params={'mobile': self.phone,
                                                                          'id_card_no': self.id_card_no})
        self.yf_statistic = sql_to_df(sql=yf_statistic_sql, params={'mobile': self.phone,
                                                                    'id_card_no': self.id_card_no})

    def model_variables(self):
        if self.qh_data is not None and self.qh_data.shape[0] > 0:
            detail_data = json.loads(self.qh_data.loc[0, 'detail_info_data'])
            query_date = self.qh_data.loc[0, 'create_time']
            if pd.isna(query_date):
                query_date = datetime.datetime.now()
            if len(detail_data) > 0:
                other_loan_df = pd.DataFrame(columns=['date', 'indu_code', 'org_scale',
                                                      'reason_code', 'org_code'])
                for i in range(len(detail_data)):
                    other_loan_df.loc[i, 'date'] = detail_data[i]['dateUpdated']
                    other_loan_df.loc[i, 'indu_code'] = detail_data[i]['industryCode']
                    other_loan_df.loc[i, 'org_scale'] = detail_data[i]['orgScale']
                    other_loan_df.loc[i, 'reason_code'] = detail_data[i]['reasonCode']
                    other_loan_df.loc[i, 'org_code'] = detail_data[i]['var1']
                other_loan_df['date'] = other_loan_df['date'].fillna('2010-01-01')
                other_loan_df['date'] = pd.to_datetime(other_loan_df['date'])
                other_loan_df.sort_values(by='date', inplace=True)
                other_loan_df = other_loan_df[(~other_loan_df['org_code'].str.contains('806460')) &
                                              (other_loan_df['date'] >= query_date + offsets.DateOffset(years=-2))]
                if other_loan_df.shape[0] > 0:
                    other_loan_df['month_from_now'] = \
                        other_loan_df['date'].apply(lambda x: (query_date.year - x.year) * 12 + query_date.month -
                                                    x.month + (query_date.day - x.day) // 100)
                    self.variables['oth_loan_reason_else_6m'] = \
                        other_loan_df.loc[(other_loan_df['month_from_now'] <= 5) &
                                          (other_loan_df['reason_code'] != '01')].shape[0]

        if self.jg_data is not None and self.jg_data.shape[0] > 0:
            jg_df = self.jg_data[self.jg_data['field_name'] == 'cts_app_145']
            if jg_df.shape[0] > 0:
                self.variables['cts_app_145'] = jg_df['field_value'].astype(float).tolist()[0]

        if self.yf_statistic is not None and self.yf_statistic.shape[0] > 0:
            statistic_df = self.yf_statistic[(self.yf_statistic['field_name'] == 'performance_amount_index') &
                                             (self.yf_statistic['field_value'] != '') &
                                             (pd.notna(self.yf_statistic['field_value']))]
            if statistic_df.shape[0] > 0:
                self.variables['performance_amount_index'] = statistic_df['field_value'].astype(float).tolist()[0]

        if self.yf_backtracking is not None and self.yf_backtracking.shape[0] > 0:
            backtracking_df = self.yf_backtracking[
                (self.yf_backtracking['field_name'].isin(['overdue_biggest_money_history_day',
                                                          'app_platform_month6'])) &
                (pd.notna(self.yf_backtracking['field_value'])) &
                (self.yf_backtracking['field_value'] != '')]
            df1 = backtracking_df[backtracking_df['field_name'] == 'overdue_biggest_money_history_day']
            if df1.shape[0] > 0:
                self.variables['overdue_biggest_money_history_day'] = df1['field_value'].astype(float).tolist()[0]
            df2 = backtracking_df[backtracking_df['field_name'] == 'app_platform_month6']
            if df2.shape[0] > 0:
                self.variables['app_platform_month6'] = df2['field_value'].astype(float).tolist()[0]

    def transform(self):
        if self.id_card_no is not None and self.phone is not None:
            self.basic_data()
            self.model_variables()
        else:
            if self.origin_data is not None:
                if len(self.origin_data) == 2:
                    main_level = self.origin_data[0].get('default_risk_level')
                    spouse_level = self.origin_data[1].get('default_risk_level')
                    if main_level is not None and spouse_level is not None:
                        self.variables['main_spouse_gap'] = main_level - spouse_level
