import pandas as pd

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from numpy import math

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)




class T27001(Transformer):
    """
    工商核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'main_ent_status_risk': 0,  # 该诊所经营状态异常
            'like_legal_invest': 0,
            'operator_years': 0
        }

    # 获取目标数据集1
    def _info_com_bus_face(self):
        sql = '''
            SELECT ent_status,open_from,open_to,reg_cap,ent_type,es_date,industry_phy_code,area_code,industry_code,
            province,city,industry_phyname,can_date
            FROM info_com_bus_face 
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='27001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_营业期限自
    def _com_bus_openfrom(self, df=None):
        med_inst_same_main = 0
        kept_year = None
        if df is not None and len(df) > 0:
            if not df['ent_status'].values[0] == '在营（开业）':
                self.variables['main_ent_status_risk'] = 1
            df1 = df.dropna(subset=['open_from'], how='any')
            if df1 is not None and len(df1) > 0:
                extra_param = self.full_msg['strategyParam']['extraParam']
                med_inst_lis_legal = extra_param["registerBasicInfo"].get('insLicencesLegal')
                med_inst_lis_head = extra_param["registerBasicInfo"].get('insLicencesHead')
                if med_inst_lis_legal is not None and med_inst_lis_head is not None:
                    if med_inst_lis_head == med_inst_lis_legal:
                        med_inst_same_main = 1
                if df['ent_status'].values[0] == '在营（开业）':
                    kept_year = math.floor((pd.datetime.now() - df['open_from'].tolist()[0]).days / 365.25)
                else:
                    kept_year = math.floor(
                        (df['can_date'].tolist()[0] - df['open_from'].tolist()[0]).days / 365.25)
            if kept_year is not None:
                if kept_year < 3 and med_inst_same_main == 1:
                    self.variables['like_legal_invest'] = 1
                self.variables['operator_years'] = kept_year

    #  执行变量转换
    def transform(self):
        info_com_bus_face = self._info_com_bus_face()
        if info_com_bus_face is not None and len(info_com_bus_face) > 0:
            self._com_bus_openfrom(info_com_bus_face)
