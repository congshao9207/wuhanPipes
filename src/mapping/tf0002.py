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


class Tf0002(Transformer):
    """
    联企核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'per_face_relent_indusCount1': 0,
            'per_face_relent_indusCode1': ''
        }

    def _info_per_bus_legal_df(self, status):
        sql = """
            SELECT ent_name,ent_status FROM info_per_bus_legal a, 
            (SELECT id FROM info_per_bus_basic as inner_b 
                WHERE inner_b.name = %(user_name)s 
                AND inner_b.id_card_no = %(id_card_no)s 
                AND unix_timestamp(NOW()) < unix_timestamp(inner_b.expired_at) order by id desc limit 1) AS b
            WHERE a.basic_id = b.id
             AND a.ent_status in %(status)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "status": status})
        return df

    def _info_per_bus_shareholder_df(self, status, ratio=0.2):
        sql = """
            SELECT ent_name,funded_ratio,ent_status FROM info_per_bus_shareholder a, 
            (SELECT id FROM info_per_bus_basic as inner_b 
                WHERE inner_b.name = %(user_name)s 
                AND inner_b.id_card_no = %(id_card_no)s 
                AND unix_timestamp(NOW()) < unix_timestamp(inner_b.expired_at) order by id desc limit 1) AS b
            WHERE a.basic_id = b.id
             and a.funded_ratio >= %(ratio)s
             and a.ent_status in %(status)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "status": status,
                               "ratio": ratio})
        return df

    def _info_com_bus_basic(self, df=None):
        info_com_bus_basic = """
            SELECT id,ent_name FROM info_com_bus_basic WHERE 
            unix_timestamp(NOW()) < unix_timestamp(expired_at) 
            AND ent_name in %(ent_names)s
            AND channel_api_no = '27001';
        """
        com_bus_basic_df = sql_to_df(sql=info_com_bus_basic,
                                     params={"ent_names": df['ent_name'].unique().tolist()})
        if com_bus_basic_df is not None and len(com_bus_basic_df) > 0:
            com_bus_basic_groupby_df = com_bus_basic_df[['id', 'ent_name']].groupby(by='ent_name', as_index=False).max()
            com_bus_basic_merge_df = pd.merge(com_bus_basic_groupby_df, com_bus_basic_df, on=['id', 'ent_name'],
                                              how='left')
            return com_bus_basic_merge_df

    def transform(self):
        ent_on_status = ['在营（开业）', '存续（在营、开业、在册）', '在营', '在业', '存续', '开业']
        shareholder_df = self._info_per_bus_shareholder_df(status=ent_on_status)
        bus_legal_df = self._info_per_bus_legal_df(status=ent_on_status)
        df = pd.concat([shareholder_df, bus_legal_df], sort=False)
        if df is not None and len(df) > 0 and df['ent_name'].shape[0] > 0:
            # 查出企业照面主表的ids
            court_merge_df = self._info_com_bus_basic(df=df)
            if court_merge_df is not None and len(court_merge_df) > 0:
                self.variables['per_face_relent_indusCount1'] = _face_relent_indus_count_1(court_merge_df)
                self.variables['per_face_relent_indusCode1'] = _face_relent_indus_code1(court_merge_df)
