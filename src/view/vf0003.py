from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class Vf0003(Transformer):
    """
    联企工商
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'per_com_industryphyname': None  # 联企工商_行业门类名称
        }

    def _info_sql_shareholder_df(self):
        info_sql_df = """
             SELECT b.ent_name
             FROM info_per_bus_basic as a
             INNER JOIN info_per_bus_shareholder as b
             ON a.id=b.basic_id
             WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
             AND a.name = %(user_name)s AND a.id_card_no = %(id_card_no)s
             AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
             AND b.funded_ratio>=0.2
             ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1
            ;
         """
        df = sql_to_df(sql=info_sql_df,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _info_sql_legal_df(self):
        info_sql_df = """
             SELECT b.ent_name
             FROM info_per_bus_basic as a
             INNER JOIN info_per_bus_legal as b
             ON a.id=b.basic_id
             WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
             AND a.name = %(user_name)s AND a.id_card_no = %(id_card_no)s
             AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
             ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1
            ;
         """
        df = sql_to_df(sql=info_sql_df,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _info_sql_com_bus_face_df(self, ent_name):
        sql = """
            SELECT d.industry_phyname
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_face as d
            on c.id=d.basic_id 
            WHERE c.ent_name = %(ent_name)s 
            and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            ORDER BY c.id DESC LIMIT 1
           ;
        """
        df = sql_to_df(sql=sql, params={"ent_name": ent_name})
        return df

    def _per_com_industryphyname(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['per_com_industryphyname'] = df['industry_phyname'].values[0]

    def transform(self):
        """
        执行变量转换
        """
        ent_name_df = self._info_sql_shareholder_df()
        ent_name_df1 = self._info_sql_legal_df()

        if ent_name_df.shape[0] > 0:
            ent_name = ent_name_df['ent_name'][0]
            info_sql_com_bus_face_df = self._info_sql_com_bus_face_df(ent_name=ent_name)
            self._per_com_industryphyname(info_sql_com_bus_face_df)

        elif ent_name_df1.shape[0] > 0:
            ent_name = ent_name_df1['ent_name'][0]
            info_sql_com_bus_face_df = self._info_sql_com_bus_face_df(ent_name=ent_name)
            self._per_com_industryphyname(info_sql_com_bus_face_df)
