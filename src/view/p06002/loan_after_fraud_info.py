from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LAFraudInfo(GroupedTransformer):
    """
    欺诈风险分析，此文件每个个人主体调用一次
    """

    def __init__(self):
        super().__init__()
        self.last_report_time = None
        self.variables = {
            "fraud_trace_act_day_avg_area": 0.5,  # 白天平均活动范围
            "fraud_trace_act_night_avg_area": 0.5,  # 夜间平均活动范围
            "fraud_trace_act_day_area": None,  # 贷后白天活动范围
            "fraud_trace_act_night_area": None,  # 贷后夜间活动范围
            "fraud_trace_act_day_area_laf": None,  # 贷后白天活动范围
            "fraud_trace_act_night_area_laf": None  # 贷后夜间活动范围
        }

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "fraud"

    def act_area(self):
        def value_transform(df, field):
            temp_df = df[df['field_name'] == field]
            if temp_df.shape[0] > 0:
                temp_value = float(temp_df['field_value'].tolist()[0])
                if temp_value < 1:
                    temp_value = 1
                elif temp_value > 10:
                    temp_value = 10
                temp_value = round(temp_value / 10, 1)
                return temp_value
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select * from info_risk_cts_item where risk_cts_id = (
                    select id from info_risk_cts where mobile = %(mobile)s AND 
                    %(last_report_time)s between create_time and expired_at order by id desc limit 1 )
            """
            df1 = sql_to_df(sql1, params={'mobile': self.phone,
                                          'last_report_time': self.last_report_time})
        sql2 = """
            select * from info_risk_cts_item where risk_cts_id = (
            select id from info_risk_cts where mobile = %(mobile)s AND 
            unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 )
        """
        df2 = sql_to_df(sql2, params={'mobile': self.phone})
        if df1 is not None and df1.shape[0] > 0:
            self.variables['fraud_trace_act_day_area'] = value_transform(df1, 'cts_lbs_004')
            self.variables['fraud_trace_act_night_area'] = value_transform(df1, 'cts_lbs_010')
        if df2.shape[0] > 0:
            self.variables['fraud_trace_act_day_area_laf'] = value_transform(df2, 'cts_lbs_004')
            self.variables['fraud_trace_act_night_area_laf'] = value_transform(df2, 'cts_lbs_010')

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        self.act_area()
