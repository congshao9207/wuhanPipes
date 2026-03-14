import re
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T36001(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'jgv5_hbm20': None
        }

    def _info_jg_v5(self):
        mobile = str(self.phone)
        sql = """
            select * 
            from info_audience_tag_item
            where audience_tag_id = (
                select id 
                from info_audience_tag 
                where mobile = %(mobile)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                order by id desc limit 1
            )
        """
        jg_df = sql_to_df(sql=sql, params={'mobile': mobile})
        if jg_df.shape[0] == 0:
            return
        hbm_df = jg_df[(jg_df['field_name'] == 'GBM_HBM_S') &
                       (jg_df['field_value'].str.contains('购物狂'))]
        if hbm_df.shape[0] == 0:
            return
        hbm_df['field_value'] = hbm_df['field_value'].apply(
            lambda x: re.search(r'(?<=购物狂, value=).*?(?=})', x).group())
        hbm_df['field_value'] = hbm_df['field_value'].fillna(0).apply(float)
        self.variables['jgv5_hbm20'] = hbm_df['field_value'].to_list()[0]

    def transform(self):
        self._info_jg_v5()
