from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T71001(Transformer):
    """
    联企核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'per_bus_leg_entrevoke_cnt': 0,
            'per_bus_shh_entrevoke_cnt': 0,
            'per_bus_leg_ent_cnt': '',
            'per_bus_shh_ent_cnt': ''
        }
        self.out_decision_code = {}

    def _info_per_bus_legal_df(self, status):
        if status is not None and len(status) > 0:
            sql = """
                SELECT count(1) as 'cnt' FROM info_per_bus_legal a, 
                (SELECT id FROM info_per_bus_basic as inner_b 
                    WHERE inner_b.name = %(user_name)s 
                    AND inner_b.id_card_no = %(id_card_no)s 
                    AND unix_timestamp(NOW()) < unix_timestamp(inner_b.expired_at) order by id desc limit 1) AS b
                WHERE a.basic_id = b.id and a.ent_status in %(status)s;
            """
            df = sql_to_df(sql=sql,
                           params={"user_name": self.user_name,
                                   "id_card_no": self.id_card_no,
                                   "status": status})
            return df
        else:
            return None

    def _per_bus_leg_entrevoke_cnt(self, df=None):
        """
        联企核查_法人吊销企业个数
        """
        if df is not None and len(df) > 0:
            self.variables['per_bus_leg_entrevoke_cnt'] = df['cnt'][0]

    def _info_per_bus_shareholder_df(self, status, ratio=0.2):
        sql = """
            SELECT count(1) as 'cnt' FROM info_per_bus_shareholder a, 
            (SELECT id FROM info_per_bus_basic as inner_b 
                WHERE inner_b.name = %(user_name)s 
                AND inner_b.id_card_no = %(id_card_no)s 
                AND unix_timestamp(NOW()) < unix_timestamp(inner_b.expired_at) order by id desc limit 1) AS b 
            WHERE a.basic_id = b.id 
                  AND a.sub_conam/a.reg_cap >=%(ratio)s
                  AND a.ent_status in %(status)s
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no,
                               "status": status,
                               "ratio": ratio})
        return df

    def _per_bus_shh_entrevoke_cnt(self, df=None):
        """
        联企核查_股东吊销企业个数
        """
        if df is not None and len(df) > 0:
            if df['cnt'][0] > 0:
                self.variables['per_bus_shh_entrevoke_cnt'] = df['cnt'][0]

    def _per_bus_leg_ent_cnt(self, df=None):
        """
        联企核查_法人在营企业个数
        """
        if df is not None and len(df) > 0:
            if df['cnt'][0] > 0:
                self.variables['per_bus_leg_ent_cnt'] = df['cnt'][0]

    def _per_bus_shh_ent_cnt(self, df=None):
        """
        联企核查_股东吊销企业个数
        """
        if df is not None and len(df) > 0:
            self.variables['per_bus_shh_ent_cnt'] = df['cnt'][0]

    def transform(self):
        ent_revoke = ['吊销', '吊销，未注销']
        self._per_bus_leg_entrevoke_cnt(self._info_per_bus_legal_df(ent_revoke))
        self._per_bus_shh_entrevoke_cnt(self._info_per_bus_shareholder_df(ent_revoke))
        ent_on = ['在营（开业）', '存续（在营、开业、在册）', '在营', '在业', '存续', '开业']
        self._per_bus_leg_ent_cnt(self._info_per_bus_legal_df(ent_on))
        self._per_bus_shh_ent_cnt(self._info_per_bus_shareholder_df(ent_on))
