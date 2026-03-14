from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from util.common_util import exception
import re


class T06002(Transformer):
    """
    公安不良新接口变量清洗
    优分AF010 v1.0.0
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ps_cri_record': 0,
            'ps_involve_drug': 0,
            'ps_drug': 0,
            'ps_run': 0,
            'ps_amend': 0,
            'ps_level': "A",
            'ps_level_detail': "未命中公安不良记录"
        }

    @staticmethod
    def _crime_type_df(user_name, id_card_no):
        info_criminal_case = """
            SELECT crime_type  FROM info_criminal_case 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_criminal_case,
                       params={"user_name": user_name, "id_card_no": id_card_no})
        return df

    def _ps_crime_type(self, df=None):

        ps_code_dict = {
            "Q": "犯罪前科",
            "S": "涉毒人员",
            "X": "吸毒人员",
            "Z": "在逃人员"
        }

        if df is not None and 'crime_type' in df.columns and len(df) == 1:
            value = df['crime_type'][0]

            if value is not None:
                crime_type = [x.strip() for x in value.split(',')] if ',' in value else value

                if 'Q' in crime_type:
                    self.variables['ps_cri_record'] = 1
                if 'S' in crime_type:
                    self.variables['ps_involve_drug'] = 1
                if 'X' in crime_type:
                    self.variables['ps_drug'] = 1
                if 'Z' in crime_type:
                    self.variables['ps_run'] = 1
                if 'E' in crime_type:
                    self.variables['ps_amend'] = 1

                if self.variables['ps_amend'] == 1:
                    self.variables['ps_level'] = "B"
                    self.variables['ps_level_detail'] = "案件撤销或曾被列入嫌疑人后撤销"

                if re.search("Q|S|X|Z", value):
                    self.variables['ps_level'] = "C"
                    detail_list = []
                    for ps_code, ps_name in ps_code_dict.items():
                        if ps_code in crime_type:
                            detail_list.append(ps_name)

                    self.variables['ps_level_detail'] = "存在 " + "、".join(detail_list) + " 相关公安不良记录"

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._ps_crime_type(T06002._crime_type_df(self.user_name, self.id_card_no))
