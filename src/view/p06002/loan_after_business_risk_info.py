import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LABusInfo(GroupedTransformer):
    """
    此文件仅调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.loan_after_obj = None
        self.variables = {
            # 经营异常信息
            "bus_abnormal_name": [],  # 企业名称
            "bus_abnormal_cause": [],  # 列入原因
            "bus_abnormal_date": [],  # 列入日期
            "bus_abnormal_org": [],  # 列入作出决定机关
            "bus_abnormal_clear_cause": [],  # 移出原因
            "bus_abnormal_clear_date": [],  # 移出日期
            "bus_abnormal_clear_org": [],  # 移出作出决定机关
            # 股东变更信息
            "bus_change_name": [],  # 企业名称
            "bus_change_category": [],  # 变更项目
            "bus_change_date": [],  # 变更日期
            "bus_change_content_before": [],  # 变更前内容
            "bus_change_content_after": [],  # 变更后内容
            # 对外投资信息
            "bus_invest_name": [],  # 企业名称
            "bus_invest_code": [],  # 统一社会信用代码
            "bus_invest_legal_rep": [],  # 法定代表人
            "bus_invest_regist": [],  # 注册号
            "bus_invest_type": [],  # 企业类型
            "bus_invest_capital": [],  # 注册资本
            "bus_invest_status": [],  # 企业登记装填
            "bus_invest_date": [],  # 成立日期
            "bus_invest_com_cnt": [],  # 企业总数量
            "bus_invest_proportion": [],  # 出资比例
            "bus_invest_form": []  # 出资方式
        }

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "bus"

    def get_com_list(self):
        after_df = pd.DataFrame(self.loan_after_obj)
        com_list = after_df[after_df['userType'] == 'COMPANY']['idno'].tolist()
        return com_list

    def info_com_bus_exception(self, com_list):
        inter_com_str = '"' + '","'.join(com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select b.ent_name, a.* from info_com_bus_exception a inner join (
                    select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) 
                    and '%s' between create_time and expired_at group by credit_code
                ) b on a.basic_id = b.id 
                where a.date_out is null
            """ % (inter_com_str, str(self.last_report_time))
            df1 = sql_to_df(sql1)
        sql2 = """
            select b.ent_name, a.* from info_com_bus_exception a inner join (
                select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at)
            ) b on a.basic_id = b.id 
            where a.date_out is null
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        if df1 is None:
            df1 = pd.DataFrame(columns=df2.columns)
        for row in df2.itertuples():
            temp_name = getattr(row, 'ent_name')
            temp_name_in = getattr(row, 'org_name_in')
            temp_date_in = getattr(row, 'date_in')
            temp_df = df1[(df1['ent_name'] == temp_name) &
                          (df1['org_name_in'] == temp_name_in) &
                          (df1['date_in'] == temp_date_in)] if df1.shape[0] > 0 else df1
            if temp_df.shape[0] == 0:
                self.variables['bus_abnormal_name'].append(temp_name)
                self.variables['bus_abnormal_cause'].append(getattr(row, 'result_in'))
                self.variables['bus_abnormal_date'].append(
                    format(pd.to_datetime(temp_date_in), '%Y-%m-%d') if pd.notna(temp_date_in) else None)
                self.variables['bus_abnormal_org'].append(getattr(row, 'org_name_in'))
                self.variables['bus_abnormal_clear_cause'].append(getattr(row, 'result_out'))
                self.variables['bus_abnormal_clear_date'].append(
                    format(pd.to_datetime(getattr(row, 'date_out')), '%Y-%m-%d')
                    if pd.notna(getattr(row, 'date_out')) else None)
                self.variables['bus_abnormal_clear_org'].append(getattr(row, 'org_name_out'))

    def info_com_bus_alter(self, com_list):
        inter_com_str = '"' + '","'.join(com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select b.ent_name, a.* from info_com_bus_alter a inner join (
                    select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) 
                    and '%s' between create_time and expired_at group by credit_code
                ) b on a.basic_id = b.id
            """ % (inter_com_str, str(self.last_report_time))
            df1 = sql_to_df(sql1)
        sql2 = """
            select b.ent_name, a.* from info_com_bus_alter a inner join (
                select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at)
            ) b on a.basic_id = b.id
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        if df1 is None:
            df1 = pd.DataFrame(columns=df2.columns)
        for row in df2.itertuples():
            temp_name = getattr(row, 'ent_name')
            temp_alt_item = getattr(row, 'alt_item')
            temp_alt_date = getattr(row, 'alt_date')
            temp_df = df1[(df1['ent_name'] == temp_name) &
                          (df1['alt_item'] == temp_alt_item) &
                          (df1['alt_date'] == temp_alt_date)] if df1.shape[0] > 0 else df1
            if temp_df.shape[0] == 0:
                self.variables['bus_change_name'].append(temp_name)
                self.variables['bus_change_category'].append(temp_alt_item)
                self.variables['bus_change_date'].append(
                    format(pd.to_datetime(temp_alt_date), '%Y-%m-%d') if pd.notna(temp_alt_date) else None)
                self.variables['bus_change_content_before'].append(getattr(row, 'alt_be'))
                self.variables['bus_change_content_after'].append(getattr(row, 'alt_af'))

    def info_com_bus_entinvitem(self, com_list):
        inter_com_str = '"' + '","'.join(com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select b.ent_name, a.* from info_com_bus_entinvitem a inner join (
                    select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) 
                    and '%s' between create_time and expired_at group by credit_code
                ) b on a.basic_id = b.id 
                where a.ent_status != '注销'
            """ % (inter_com_str, str(self.last_report_time))
            df1 = sql_to_df(sql1)
        sql2 = """
            select b.ent_name, a.* from info_com_bus_entinvitem a inner join (
                select max(id) as id, max(ent_name) as ent_name from info_com_bus_basic where credit_code in (%s) and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at)
            ) b on a.basic_id = b.id 
            where a.ent_status != '注销'
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        if df1 is None:
            df1 = pd.DataFrame(columns=df2.columns)
        for row in df2.itertuples():
            temp_name = getattr(row, 'ent_name')
            temp_date = getattr(row, 'es_date')
            temp_df = df1[(df1['ent_name'] == temp_name) &
                          (df1['es_date'] == temp_date)] if df1.shape[0] > 0 else df1
            if temp_df.shape[0] == 0:
                self.variables['bus_invest_name'].append(temp_name)
                self.variables['bus_invest_code'].append(getattr(row, 'credit_code'))
                self.variables['bus_invest_legal_rep'].append(getattr(row, 'fr_name'))
                self.variables['bus_invest_regist'].append(getattr(row, 'reg_no'))
                self.variables['bus_invest_type'].append(getattr(row, 'ent_type'))
                self.variables['bus_invest_capital'].append(getattr(row, 'reg_cap'))
                self.variables['bus_invest_status'].append(getattr(row, 'ent_status'))
                self.variables['bus_invest_date'].append(
                    format(pd.to_datetime(temp_date), '%Y-%m-%d') if pd.notna(temp_date) else None)
                self.variables['bus_invest_com_cnt'].append(getattr(row, 'pinv_amount'))
                self.variables['bus_invest_proportion'].append(getattr(row, 'funded_ratio'))
                self.variables['bus_invest_form'].append(getattr(row, 'con_form'))

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        com_list = self.get_com_list()
        self.info_com_bus_exception(com_list)
        self.info_com_bus_alter(com_list)
        self.info_com_bus_entinvitem(com_list)
