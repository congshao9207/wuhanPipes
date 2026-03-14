from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from util.common_util import exception


class T06001(Transformer):
    """
    公安相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ps_run': 0,
            'ps_drug': 0,
            'ps_involve_drug': 0,
            'ps_seri_crim': 0,
            'ps_mali_crim': 0,
            'ps_econ_crim': 0,
            'ps_amend_staff': 0,
            'ps_illeg_crim': 0,
            'ps_illegal_record_time': 0,
            'ps_score': -1
        }

    @staticmethod
    def _crime_type_df(user_name, id_card_no):
        info_criminal_case = """
            SELECT crime_type, case_period, score FROM info_criminal_case 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY id  DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_criminal_case,
                       params={"user_name": user_name, "id_card_no": id_card_no})
        return df

    @exception('purpose= 公安核查&author=luokui')
    def _ps_crime_type(self, df=None):
        if df is not None and 'crime_type' in df.columns and len(df) == 1:
            value = df['crime_type'][0]
            score = df["score"][0]
            if value is not None:
                crime_type = [x.strip() for x in value.split(',')] if ',' in value else value
                if 'AT_LARGE' in crime_type:
                    self.variables['ps_run'] = 1
                if 'DRUG' in crime_type:
                    self.variables['ps_drug'] = 1
                if "DRUG_RELATED" in crime_type or "ILLEGAL_F" in crime_type:
                    self.variables['ps_involve_drug'] = 1
                if 'ILLEGAL_B' in crime_type:
                    self.variables['ps_seri_crim'] = 1
                if 'ILLEGAL_C' in crime_type:
                    self.variables['ps_mali_crim'] = 1
                if 'ILLEGAL_E' in crime_type:
                    self.variables['ps_econ_crim'] = 1
                if 'REVOKE' in crime_type:
                    self.variables['ps_amend_staff'] = 1
                if 'ILLEGAL_A' in crime_type:
                    self.variables['ps_illeg_crim'] = 1
                    cp = df['case_period'][0]
                    if cp is not None and ',' in cp:
                        r = cp.replace('[', '').replace(')', '')
                        max_month = int(r.split(',')[1])
                        year = max_month / 12
                        if year == 0:
                            self.variables['ps_illegal_record_time'] = 4
                        elif year <= 1:
                            self.variables['ps_illegal_record_time'] = 1
                        elif year <= 2:
                            self.variables['ps_illegal_record_time'] = 2
                        elif year <= 5:
                            self.variables['ps_illegal_record_time'] = 3
                        else:
                            self.variables['ps_illegal_record_time'] = 4
            if score is not None:
                self.variables['ps_score'] = score

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._ps_crime_type(T06001._crime_type_df(self.user_name, self.id_card_no))
