import pandas as pd
from pandas.tseries import offsets

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LAFinInfo(GroupedTransformer):
    """
    财务风险分析，此文件每个个人主体都要调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.variables = {
            # 贷前查询次数
            "fin_qh_p2p_category_1m_cnt": None,  # 前海1个月内p2p大类查询次数
            "fin_qh_small_loan_category_1m_cnt": None,  # 前海1个月内小贷大类查询次数
            "fin_qh_insure_category_1m_cnt": None,  # 前海1个月内担保大类查询次数
            "fin_qh_bank_category_1m_cnt": None,  # 前海1个月内银行大类查询次数
            "fin_qh_rent_category_1m_cnt": None,  # 前海1个月内融资租赁大类查询次数
            "fin_qh_consume_category_1m_cnt": None,  # 前海1个月内消金大类查询次数
            "fin_qh_other_category_1m_cnt": None,  # 前海1个月内其他大类查询次数
            "fin_qh_p2p_category_3m_cnt": None,  # 前海3个月内p2p大类查询次数
            "fin_qh_small_loan_category_3m_cnt": None,  # 前海3个月内小贷大类查询次数
            "fin_qh_insure_category_3m_cnt": None,  # 前海3个月内担保大类查询次数
            "fin_qh_bank_category_3m_cnt": None,  # 前海3个月内银行大类查询次数
            "fin_qh_rent_category_3m_cnt": None,  # 前海3个月内融资租赁大类查询次数
            "fin_qh_consume_category_3m_cnt": None,  # 前海3个月内消金大类查询次数
            "fin_qh_other_category_3m_cnt": None,  # 前海3个月内其他大类查询次数
            "fin_qh_p2p_category_6m_cnt": None,  # 前海6个月内p2p大类查询次数
            "fin_qh_small_loan_category_6m_cnt": None,  # 前海6个月内小贷大类查询次数
            "fin_qh_insure_category_6m_cnt": None,  # 前海6个月内担保大类查询次数
            "fin_qh_bank_category_6m_cnt": None,  # 前海6个月内银行大类查询次数
            "fin_qh_rent_category_6m_cnt": None,  # 前海6个月内融资租赁大类查询次数
            "fin_qh_consume_category_6m_cnt": None,  # 前海6个月内消金大类查询次数
            "fin_qh_other_category_6m_cnt": None,  # 前海6个月内其他大类查询次数
            # 贷后查询次数
            "fin_qh_p2p_category_1m_cnt_laf": None,  # 前海1个月内p2p大类查询次数_贷后
            "fin_qh_small_loan_category_1m_cnt_laf": None,  # 前海1个月内小贷大类查询次数_贷后
            "fin_qh_insure_category_1m_cnt_laf": None,  # 前海1个月内担保大类查询次数_贷后
            "fin_qh_bank_category_1m_cnt_laf": None,  # 前海1个月内银行大类查询次数_贷后
            "fin_qh_rent_category_1m_cnt_laf": None,  # 前海1个月内融资租赁大类查询次数_贷后
            "fin_qh_consume_category_1m_cnt_laf": None,  # 前海1个月内消金大类查询次数_贷后
            "fin_qh_other_category_1m_cnt_laf": None,  # 前海1个月内其他大类查询次数_贷后
            "fin_qh_p2p_category_3m_cnt_laf": None,  # 前海3个月内p2p大类查询次数_贷后
            "fin_qh_small_loan_category_3m_cnt_laf": None,  # 前海3个月内小贷大类查询次数_贷后
            "fin_qh_insure_category_3m_cnt_laf": None,  # 前海3个月内担保大类查询次数_贷后
            "fin_qh_bank_category_3m_cnt_laf": None,  # 前海3个月内银行大类查询次数_贷后
            "fin_qh_rent_category_3m_cnt_laf": None,  # 前海3个月内融资租赁大类查询次数_贷后
            "fin_qh_consume_category_3m_cnt_laf": None,  # 前海3个月内消金大类查询次数_贷后
            "fin_qh_other_category_3m_cnt_laf": None,  # 前海3个月内其他大类查询次数_贷后
            "fin_qh_p2p_category_6m_cnt_laf": None,  # 前海6个月内p2p大类查询次数_贷后
            "fin_qh_small_loan_category_6m_cnt_laf": None,  # 前海6个月内小贷大类查询次数_贷后
            "fin_qh_insure_category_6m_cnt_laf": None,  # 前海6个月内担保大类查询次数_贷后
            "fin_qh_bank_category_6m_cnt_laf": None,  # 前海6个月内银行大类查询次数_贷后
            "fin_qh_rent_category_6m_cnt_laf": None,  # 前海6个月内融资租赁大类查询次数_贷后
            "fin_qh_consume_category_6m_cnt_laf": None,  # 前海6个月内消金大类查询次数_贷后
            "fin_qh_other_category_6m_cnt_laf": None,  # 前海6个月内其他大类查询次数_贷后
            # 查询详情
            "fin_qh_query_time": None,  # 时间
            "fin_qh_industry_code": None,  # 机构类型
            "fin_qh_query_reason": None  # 查询原因
        }
        self.mapper = {
            "MCL": "小额贷款",
            "P2P": "网贷/P2P",
            "CNS": "持牌消费金融",
            "BAK": "银行",
            "LEA": "融资租赁",
            "INS": "保险",
            "ASM": "资管",
            "INV": "投资",
            "FGC": "融资担保/典当/担保",
            "CRF": "众筹",
            "FAC": "商业保理",
            "HTL": "助贷",
            "CAR": "汽车金融",
            "TRU": "信托",
            "THR": "第三方",
            "OTH": "其他"
        }
        self.reason = {
            1: "贷款审批",
            2: "贷中管理",
            3: "贷后管理",
            4: "本人查询",
            5: "异议查询",
            99: "其他"
        }

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "fin"

    def _df_query_cnt(self, df, months, params, key, month_col='month_from_now', type_col='industryCode'):
        if isinstance(params, str):
            self.variables[key] = df[(df[month_col] < months) &
                                     (df[type_col] == params)].shape[0]
        elif isinstance(params, list):
            self.variables[key] = df[(df[month_col] < months) &
                                     (df[type_col].isin(params))].shape[0]

    def qh_query_info(self):
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                SELECT detail_info_data
                FROM info_other_loan_summary  
                WHERE  user_name = %(user_name)s and id_card_no = %(id_card_no)s 
                and %(last_report_time)s between create_time and expired_at
                ORDER BY id DESC LIMIT 1;
            """
            df1 = sql_to_df(sql1, params={'user_name': self.user_name,
                                          'id_card_no': self.id_card_no,
                                          'last_report_time': self.last_report_time})
        sql2 = """
           SELECT detail_info_data
           FROM info_other_loan_summary  
           WHERE  user_name = %(user_name)s and id_card_no = %(id_card_no)s 
           and unix_timestamp(NOW()) < unix_timestamp(expired_at)
           ORDER BY id DESC LIMIT 1;
        """
        df2 = sql_to_df(sql2, params={'user_name': self.user_name, 'id_card_no': self.id_card_no})
        month_list = [1, 3, 6]
        now = pd.datetime.now()
        year_ago = now - offsets.DateOffset(months=12)
        if df1 is not None and df1.shape[0] > 0:
            json_str1 = df1['detail_info_data'].tolist()[0]
            df1 = pd.read_json(json_str1)
            df1['date'] = df1['dateUpdated'].fillna('2010-01-01')
            df1['date'] = pd.to_datetime(df1['date'])
            df1.sort_values(by='date', inplace=True, ascending=False)
            df1 = df1[(~df1['var1'].str.contains('806460')) &
                      (df1['date'] >= year_ago)]
            df1['month_from_now'] = df1['date'].apply(
                lambda x: (now.year - x.year) * 12 + now.month - x.month + (now.day - x.day) // 100)
            if df1.shape[0] > 0:
                for m in month_list:
                    self._df_query_cnt(df1, m, 'P2P', 'fin_qh_p2p_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, 'MCL', 'fin_qh_small_loan_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, 'FGC', 'fin_qh_insure_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, 'BAK', 'fin_qh_bank_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, 'LEA', 'fin_qh_rent_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, ['CNS', 'HTL'], 'fin_qh_consume_category_%dm_cnt' % m)
                    self._df_query_cnt(df1, m, ["INS", "ASM", "INV", "CRF", "FAC", "CAR", "TRU", "THR", "OTH"],
                                       'fin_qh_other_category_%dm_cnt' % m)
        if df2 is not None and df2.shape[0] > 0:
            json_str2 = df2['detail_info_data'].tolist()[0]
            df2 = pd.read_json(json_str2)
            df2['date'] = df2['dateUpdated'].fillna('2010-01-01')
            df2['date'] = pd.to_datetime(df2['date'])
            df2.sort_values(by='date', inplace=True, ascending=False)
            df2 = df2[(~df2['var1'].str.contains('806460')) &
                      (df2['date'] >= year_ago)]
            df2['month_from_now'] = df2['date'].apply(
                lambda x: (now.year - x.year) * 12 + now.month - x.month + (now.day - x.day) // 100)
            df2['industry_detail'] = df2['industryCode'].map(self.mapper)
            df2['reason_detail'] = df2['reasonCode'].map(self.reason)
            if df2.shape[0] > 0:
                for m in month_list:
                    self._df_query_cnt(df2, m, 'P2P', 'fin_qh_p2p_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, 'MCL', 'fin_qh_small_loan_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, 'FGC', 'fin_qh_insure_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, 'BAK', 'fin_qh_bank_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, 'LEA', 'fin_qh_rent_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, ['CNS', 'HTL'], 'fin_qh_consume_category_%dm_cnt_laf' % m)
                    self._df_query_cnt(df2, m, ["INS", "ASM", "INV", "CRF", "FAC", "CAR", "TRU", "THR", "OTH"],
                                       'fin_qh_other_category_%dm_cnt_laf' % m)
                self.variables['fin_qh_query_time'] = df2['dateUpdated'].tolist()
                self.variables['fin_qh_industry_code'] = df2['industry_detail'].tolist()
                self.variables['fin_qh_query_reason'] = df2['reason_detail'].tolist()

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        self.qh_query_info()
