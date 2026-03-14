import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class V24001(Transformer):
    """
    企业工商核查,贷前+贷后详细信息展示
    """

    def __init__(self):
        super().__init__()
        self.variables = {'info_com_bus': []}
        # self.variables = {
        #     'info_com_bus_shares_frost': None,  # 工商核查_现在是否有股权冻结信息_贷后+贷前
        #     'info_com_bus_shares_impawn': None,  # 工商核查_现在是否有股权出质登记信息_贷后+贷前
        #     'info_com_bus_mor_detail': None,  # 工商核查_现在是否有动产抵押登记信息_贷后+贷前
        #     'info_com_bus_liquidation': None,  # 工商核查_是否有清算信息_贷后+贷前
        #     'info_com_bus_illegal_list': None,  # 工商核查_现在是否有严重违法失信信息_贷后+贷前
        #     'info_com_bus_case_info': None,  # 工商核查_是否有行政处罚信息_贷后+贷前
        #     'info_com_bus_exception': None  # 工商核查_是否有经营异常信息_贷后+贷前
        # }
        self.pre_biz_date = None
        self.id_list = list()

    # 获取企业名对应的info_com_bus_basic表格id
    def _get_info_com_bus_basic_id(self):
        sql1 = """
            select 
                id 
            FROM 
                info_com_bus_basic 
            where 
                %(result_date)s between create_time and expired_at
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s) 
                order by id desc limit 1
        """
        sql2 = """
            select 
                id 
            FROM 
                info_com_bus_basic 
            where 
                NOW() between create_time and expired_at
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s) 
                order by id desc limit 1
        """
        df1 = sql_to_df(sql=sql1,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no,
                                'result_date': self.pre_biz_date})
        df2 = sql_to_df(sql=sql2,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        if len(df1) > 0:
            self.id_list.append(str(df1.values[0][0]))
        else:
            self.id_list.append('Na')
        if len(df2) > 0:
            self.id_list.append(str(df2.values[0][0]))
        else:
            self.id_list.append('Na')

    # 工商核查各命中项详细信息展示
    def _hit_com_bus_info_details(self):
        hit_list = {
            'info_com_bus_shares_frost': {'table_name': 'info_com_bus_shares_frost',
                                          'criteria': "and a.judicial_froz_state like '%%冻结%%' and "
                                                      "a.judicial_froz_state not regexp '解冻|解除|失效'",
                                          'type': '工商核查企业股权冻结信息'},
            'info_com_bus_shares_impawn': {'table_name': 'info_com_bus_shares_impawn',
                                           'criteria': "and a.imp_exe_state like '%%有效%%'",
                                           'type': '工商核查企业股权出质信息'},
            'info_com_bus_mor_detail': {'table_name': 'info_com_bus_mort_basic',
                                        'criteria': "and a.mort_status like '%%有效%%'",
                                        'type': '工商核查企业动产抵押信息'},
            'info_com_bus_liquidation': {'table_name': 'info_com_bus_liquidation',
                                         'criteria': "",
                                         'type': '工商核查企业清算信息'},
            'info_com_bus_illegal_list': {'table_name': 'info_com_bus_illegal',
                                          'criteria': "and (a.illegal_rresult_out is null "
                                                      "or a.illegal_rresult_out='')",
                                          'type': '工商核查企业严重违法失信信息'},
            'info_com_bus_case_info': {'table_name': 'info_com_bus_case',
                                       'criteria': "",
                                       'type': '工商核查企业行政处罚信息'},
            'info_com_bus_exception': {'table_name': 'info_com_bus_exception',
                                       'criteria': "and (a.result_out is null or a.result_out = '')",
                                       'type': '工商核查企业经营异常信息'}
        }
        for var in hit_list.keys():
            if var != 'info_com_bus_mor_detail':
                sql = """
                    select
                        a.*
                    from
                        %s a""" % hit_list[var]['table_name'] + """
                    where
                        a.basic_id=%(id)s """ + hit_list[var]['criteria']
            else:
                sql = """
                    select
                        a.*,b.can_reason,b.can_date,c.ma_balt_details,c.ma_balt_date, d.*
                    from
                        info_com_bus_mort_basic a
                    left join
                        info_com_bus_mort_cancel b
                    on
                        a.id=b.mort_id
                    left join
                        info_com_bus_mort_change c
                    on
                        a.id=c.mort_id
                    left join
                        info_com_bus_mort_creditor d
                    on 
                        a.id=d.mort_id
                    where
                        a.basic_id=%(id)s """ + hit_list[var]['criteria']
            df_before_loan = sql_to_df(sql=sql,
                                       params={'id': self.id_list[0]})
            df_after_loan = sql_to_df(sql=sql,
                                      params={'id': self.id_list[1]})
            df_before_loan.fillna('', inplace=True)
            df_after_loan.fillna('', inplace=True)
            self.variables['info_com_bus'].append({'variable': var,
                                                   'type': hit_list[var]['type'],
                                                   'before': [],
                                                   'after': []})
            if len(df_before_loan) > 0:
                for row in df_before_loan.itertuples():
                    self.variables['info_com_bus'][-1]['before'].append({})
                    for col in df_before_loan.columns:
                        self.variables['info_com_bus'][-1]['before'][-1][col] = str(getattr(row, col))
            if len(df_after_loan) > 0:
                for row in df_after_loan.itertuples():
                    self.variables['info_com_bus'][-1]['after'].append({})
                    for col in df_after_loan.columns:
                        self.variables['info_com_bus'][-1]['after'][-1][col] = str(getattr(row, col))
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = 'Na'
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = 'Na'

        # self.variables["variable_product_code"] = "06001"
        self._get_info_com_bus_basic_id()
        self._hit_com_bus_info_details()
