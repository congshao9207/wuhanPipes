import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LAOwnerInfo(GroupedTransformer):
    """
    企业主分析，此文件每个个人主体都需要调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.variables = {
            "owner_criminal_score_level": None,  # 贷前公安不良等级
            "owner_criminal_score_level_laf": None,  # 贷后公安不良等级
            "owner_tax_name": [],  # 公司名称
            "owner_tax_amt": [],  # 欠税金额
            "owner_tax_type": [],  # 税款类型
            "owner_tax_date": [],  # 欠税发生时间
            "owner_list_name": [],  # 姓名
            "owner_list_tyle": [],  # 名单类型
            "owner_list_case_no": [],  # 案号
            "owner_list_detail": [],  # 详情
        }

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "owner"

    def criminal_score(self):
        def score_transform(score, c_type):
            if score is not None:
                try:
                    score = float(score)
                except ValueError:
                    return "A"
                if score > 60:
                    level = "A"
                elif score == 60:
                    level = "B"
                elif score > 20:
                    level = "C"
                else:
                    level = "D"
            else:
                if c_type in ['Q', 'S', 'X', 'Z']:
                    level = 'C'
                elif c_type == 'E':
                    level = 'B'
                else:
                    level = 'A'
            return level

        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select score, crime_type
                from info_criminal_case
                where id_card_no = %(id_card_no)s
                and %(last_report_time)s between create_time and expired_at
                order by id desc limit 1
            """
            df1 = sql_to_df(sql1, params={'id_card_no': self.id_card_no,
                                          'last_report_time': self.last_report_time})
        sql2 = """
            select score, crime_type
            from info_criminal_case
            where id_card_no = %(id_card_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df2 = sql_to_df(sql2, params={'id_card_no': self.id_card_no})
        if df1 is not None and df1.shape[0] > 0:
            self.variables['owner_criminal_score_level'] = \
                score_transform(df1['score'].tolist()[0], df1['crime_type'].tolist()[0])
        if df2.shape[0] > 0:
            self.variables['owner_criminal_score_level_laf'] = \
                score_transform(df2['score'].tolist()[0], df2['crime_type'].tolist()[0])

    def bad_behavior_info(self):
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select id 
                from info_court 
                where unique_id_no = %(id_card_no)s and %(last_report_time)s between create_time and expired_at
                order by id desc limit 1
            """
            df1 = sql_to_df(sql1, params={'id_card_no': self.id_card_no,
                                          'last_report_time': self.last_report_time})
        sql2 = """
            select id 
            from info_court 
            where unique_id_no = %(id_card_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df2 = sql_to_df(sql2, params={'id_card_no': self.id_card_no})
        court_id1 = None
        if df1 is not None and df1.shape[0] > 0:
            court_id1 = df1['id'].tolist()[0]
        court_id2 = None
        if df2.shape[0] > 0:
            court_id2 = df2['id'].tolist()[0]
        tax_sql = """
            select * 
            from info_court_tax_arrears
            where court_id = %s
        """
        behavior_sql = """
            select name, '罪犯及嫌疑人' as type, case_no as case_no, criminal_reason as detail, trial_date as detail_time
            from info_court_criminal_suspect where court_id = %(court_id)s
            union all 
            select name, '失信老赖' as type, execute_case_no as case_no, 
            execute_content as detail, execute_date as detail_time
            from info_court_deadbeat where court_id = %(court_id)s and execute_status != '已结案'
            union all 
            select name, '限制高消费' as type, execute_case_no as case_no, 
            execute_content as detail, specific_date as detail_time
            from info_court_limit_hignspending where court_id = %(court_id)s
            union all 
            select name, '限制出入境' as type, execute_no as case_no, 
            execute_content as detail, specific_date as detail_time
            from info_court_limited_entry_exit where court_id = %(court_id)s
        """
        if court_id1 is not None:
            court_df1 = sql_to_df(tax_sql % str(court_id1))
            behavior_df1 = sql_to_df(behavior_sql, params={'court_id': str(court_id1)})
        else:
            court_df1 = None
            behavior_df1 = None
        if court_id2 is not None:
            court_df2 = sql_to_df(tax_sql % str(court_id2))
            behavior_df2 = sql_to_df(behavior_sql, params={'court_id': str(court_id2)})
        else:
            court_df2 = None
            behavior_df2 = None
        if court_df2 is not None:
            if court_df1 is None:
                court_df1 = pd.DataFrame(columns=court_df2.columns)
            for row in court_df2.itertuples():
                temp_taxes = getattr(row, 'taxes')
                temp_taxes_time = getattr(row, 'taxes_time')
                temp_taxes_type = getattr(row, 'taxes_type')
                temp_df = court_df1[(court_df1['taxes'] == temp_taxes) &
                                    (court_df1['taxes_time'] == temp_taxes_time) &
                                    (court_df1['taxes_type'] == temp_taxes_type)
                                    ] if court_df1.shape[0] > 0 else court_df1
                if temp_df.shape[0] == 0:
                    self.variables['owner_tax_name'].append(getattr(row, 'name'))
                    self.variables['owner_tax_amt'].append(temp_taxes)
                    self.variables['owner_tax_type'].append(temp_taxes_type)
                    self.variables['owner_tax_date'].append(
                        format(pd.to_datetime(temp_taxes_time), '%Y-%m-%d') if pd.notna(temp_taxes_time) else None)
        if behavior_df2 is not None:
            if behavior_df1 is None:
                behavior_df1 = pd.DataFrame(columns=behavior_df2.columns)
            for row in behavior_df2.itertuples():
                temp_name = getattr(row, 'name')
                temp_type = getattr(row, 'type')
                temp_case_no = getattr(row, 'case_no')
                temp_detail_time = getattr(row, 'detail_time')
                temp_df = behavior_df1[(behavior_df1['name'] == temp_name) &
                                       (behavior_df1['type'] == temp_type) &
                                       (behavior_df1['case_no'] == temp_case_no) &
                                       (behavior_df1['detail_time'] == temp_detail_time)
                                       ] if behavior_df1.shape[0] > 0 else behavior_df1
                if temp_df.shape[0] == 0:
                    self.variables['owner_list_name'].append(temp_name)
                    self.variables['owner_list_tyle'].append(temp_type)
                    self.variables['owner_list_case_no'].append(temp_case_no)
                    self.variables['owner_list_detail'].append(getattr(row, 'detail'))

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        self.criminal_score()
        self.bad_behavior_info()
