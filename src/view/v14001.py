# @author lixiaobo
# @date 2020-03-18
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from view.mapper_detail import round_max


class V14001(Transformer):
    """
    社交核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_gray_score': 0,  # 灰度分
            'contacts_class_1_blacklist_cnt': 0,  # 直接联系人在黑名单的数量
            'contacts_class_2_blacklist_cnt': 0,  # 间接联系人在黑名单的数量
            'contacts_router_cnt': 0,  # 引起二阶黑名单人数
            'contacts_router_ratio': 0,  # 引起占比=引起二阶黑名单人数/一阶联系人总数
            'contacts_class_1_cnt': 0  # 一阶联系人总数
        }

    def _info_df(self):
        sql = '''
                SELECT g.* FROM info_social_gray g INNER JOIN (
                    SELECT * FROM info_social WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s
                     AND unix_timestamp(NOW()) < unix_timestamp(expired_at) limit 1
                    ) tab WHERE tab.social_id = g.social_id limit 1;
                '''

        df = sql_to_df(sql=sql, params={"user_name": self.user_name,
                                        "id_card_no": self.id_card_no,
                                        "phone": self.phone})
        return df

    #  执行变量转换
    def transform(self):
        df = self._info_df()
        if df is not None and not df.empty:
            self.variables["phone_gray_score"] = df.iloc[0].phone_gray_score
            self.variables["contacts_class_1_blacklist_cnt"] = df.iloc[0].contacts_class_1_blacklist_cnt
            self.variables["contacts_class_2_blacklist_cnt"] = df.iloc[0].contacts_class_2_blacklist_cnt
            self.variables["contacts_router_cnt"] = df.iloc[0].contacts_router_cnt
            self.variables["contacts_router_ratio"] = df.iloc[0].contacts_router_ratio
            self.variables["contacts_class_1_cnt"] = df.iloc[0].contacts_class_1_cnt
