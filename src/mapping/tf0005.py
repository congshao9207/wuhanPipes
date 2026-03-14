import pandas as pd

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def _face_relent_indus_count_1(df):
    """
    联企照面_关联企业的行业类别数量
    :param df:
    :return:
    """
    sql = """
        SELECT count(DISTINCT industry_phy_code) as 'cnt' FROM info_com_bus_face
        WHERE  basic_id in %(ids)s;
    """
    df_out = sql_to_df(sql=sql, params={"ids": df['id'].unique().tolist()})
    return df_out['cnt'][0]


def _face_relent_indus_code1(df):
    """
    联企照面_关联企业的行业门类代码
    :param df:
    :return:
    """
    sql = """
        SELECT DISTINCT industry_phy_code FROM info_com_bus_face
        WHERE basic_id in %(ids)s;;
    """
    df_out = sql_to_df(sql=sql, params={"ids": df['id'].unique().tolist()})
    return ','.join(df_out['industry_phy_code'].tolist())


class Tf0005(Transformer):
    """
       工商照面
       """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'com_bus_face_outwardindusCode1': '',
            'com_bus_face_outwardindusCount1': 0
        }

    def _info_com_bus_entinvitem_df(self, ratio=0.2):
        sql = """
        SELECT DISTINCT(ent_name) as ent_name FROM info_com_bus_entinvitem a,
        (SELECT id FROM info_com_bus_basic WHERE ent_name=%(user_name)s and credit_code = %(id_card_no)s
        AND unix_timestamp(NOW()) < unix_timestamp(expired_at) and channel_api_no='24001' order by id desc limit 1) b
        WHERE a.basic_id = b.id
        and a.funded_ratio >= %(ratio)s
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "ratio": ratio})
        return df

    def _info_com_bus_frinv_df(self):
        sql = """
        SELECT DISTINCT(ent_name) as ent_name FROM info_com_bus_frinv a,
        (SELECT id FROM info_com_bus_basic WHERE ent_name=%(user_name)s and credit_code = %(id_card_no)s
        AND unix_timestamp(NOW()) < unix_timestamp(expired_at) and channel_api_no='24001' order by id desc limit 1) b
        WHERE a.basic_id = b.id
        and a.fr_name = %(user_name)s
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def _info_com_bus_basic(self, df=None):
        info_com_bus_basic = """
            SELECT id,ent_name FROM info_com_bus_basic WHERE 
            unix_timestamp(NOW()) < unix_timestamp(expired_at) and channel_api_no='27001'
            AND ent_name in %(ent_names)s;
        """
        com_bus_basic_df = sql_to_df(sql=info_com_bus_basic,
                                     params={"ent_names": df['ent_name'].unique().tolist()})
        if com_bus_basic_df is not None and len(com_bus_basic_df) > 0:
            com_bus_basic_groupby_df = com_bus_basic_df[['id', 'ent_name']].groupby(by='ent_name', as_index=False).max()
            com_bus_basic_merge_df = pd.merge(com_bus_basic_groupby_df, com_bus_basic_df, on=['id', 'ent_name'],
                                              how='left')
            return com_bus_basic_merge_df

    def transform(self):
        entinvitem_df = self._info_com_bus_entinvitem_df()
        frinv_df = self._info_com_bus_frinv_df()
        df = pd.concat([entinvitem_df, frinv_df])
        if df is not None and len(df) > 0 and df['ent_name'].shape[0] > 0:
            # 查出企业照面主表的ids
            court_merge_df = self._info_com_bus_basic(df=df)
            if court_merge_df is not None and len(court_merge_df) > 0:
                self.variables['com_bus_face_outwardindusCount1'] = _face_relent_indus_count_1(court_merge_df)
                self.variables['com_bus_face_outwardindusCode1'] = _face_relent_indus_code1(court_merge_df)
