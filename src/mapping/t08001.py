from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T08001(Transformer):
    """
    欺诈咨询相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "fraudinfo_isMachdBlMakt": 0,  # 欺诈资讯_命中第三方标注黑名单
            "fraudinfo_isMachCraCall": 0,  # 欺诈资讯_命中骚扰电话
            "fraudinfo_isMachFraud": 0,  # 欺诈资讯_命中欺诈号码
            "fraudinfo_isMachEmpty": 0,  # 欺诈资讯_命中空号（非正常短信语音号码）
            "fraudinfo_isMachYZmobile": 0,  # 欺诈资讯_命中收码平台号码
            "fraudinfo_isMachSmallNo": 0,  # 欺诈资讯_命中小号
            "fraudinfo_isMachSZNo": 0  # 欺诈资讯_命中社工库号码
        }

    def _info_risk_anti_fraud_df(self):
        info_risk_anti_fraud = """
            SELECT user_name, id_card_no,phone,expired_at,match_blacklist,match_crank_call,match_fraud,
            match_empty_number,match_verification_mobile,match_small_no,match_sz_no
            FROM info_risk_anti_fraud 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_risk_anti_fraud,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    def _info_risk_anti_fraud(self, df=None):
        if df is not None and len(df) > 0:
            if df['match_blacklist'][0] == b'\x01':
                self.variables['fraudinfo_isMachdBlMakt'] = 1
            if df['match_crank_call'][0] == b'\x01':
                self.variables['fraudinfo_isMachCraCall'] = 1
            if df['match_fraud'][0] == b'\x01':
                self.variables['fraudinfo_isMachFraud'] = 1
            if df['match_empty_number'][0] == b'\x01':
                self.variables['fraudinfo_isMachEmpty'] = 1
            if df['match_verification_mobile'][0] == b'\x01':
                self.variables['fraudinfo_isMachYZmobile'] = 1
            if df['match_small_no'][0] == b'\x01':
                self.variables['fraudinfo_isMachSmallNo'] = 1
            if df['match_sz_no'][0] == b'\x01':
                self.variables['fraudinfo_isMachSZNo'] = 1

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._info_risk_anti_fraud(self._info_risk_anti_fraud_df())
