from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.common_util import get_industry_risk_level, get_industry_risk_tips
from util.mysql_reader import sql_to_df


class BusUnique(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "bus"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "bus_industry_ent_name": "",
            "bus_industry_industry": "",
            "bus_industry_grade": "",
            "bus_industry_hint": []
        }

    def _info_com_bus_face(self):
        sql = '''
            select ent_name,industry_phy_code,industry_code,industry_name from info_com_bus_face where basic_id = (
                select id from info_com_bus_basic where ent_name = %(ent_name)s and credit_code = %(credit_code)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1
            )
        '''
        df = sql_to_df(sql=sql,
                       params={"ent_name": self.user_name,
                               "credit_code": self.id_card_no})
        return df

    def clean_variables(self):
        df = self._info_com_bus_face()
        if not df.empty:
            self.variables['bus_industry_ent_name'] = df.loc[0, 'ent_name']
            self.variables['bus_industry_industry'] = df.loc[0, 'industry_name']
            df['industry_code_1'] = df.apply(lambda x: (x['industry_phy_code'] + x['industry_code'])[:4] if len(
                x['industry_phy_code'] + x['industry_code']) >= 4 else x['industry_phy_code'] + x['industry_code'],
                                           axis=1)
            self.variables['bus_industry_grade'] = get_industry_risk_level(df.loc[0, 'industry_phy_code'] + df.loc[0, 'industry_code'])
            self.variables['bus_industry_hint'] = get_industry_risk_tips(
                df.loc[0, 'industry_phy_code'] + df.loc[0, 'industry_code'])

    def transform(self):
        strategy = self.origin_data.get("extraParam")['strategy']
        if 'COMPANY' in self.base_type and strategy == "01":
            self.clean_variables()

