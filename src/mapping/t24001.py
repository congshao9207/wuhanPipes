import pandas as pd

from mapping.tranformer import Transformer, subtract_datetime_col
from util.common_util import exception
from util.mysql_reader import sql_to_df

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class T24001(Transformer):
    """
    工商核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'com_bus_status': 0,  # 工商核查_企业登记状态异常
            'com_bus_endtime': None,  # 工商核查_营业期限至
            'com_bus_relent_revoke': 0,  # 工商核查_关联公司吊销个数
            'com_bus_case_info': 0,  # 工商核查_现在是否有行政处罚信息
            'com_bus_shares_frost': 0,  # 工商核查_现在是否有股权冻结信息
            'com_bus_shares_frost_his': 0,  # 工商核查_曾经是否有股权冻结信息
            'com_bus_shares_impawn': 0,  # 工商核查_现在是否有股权出质登记信息
            'com_bus_shares_impawn_his': 0,  # 工商核查_曾经是否有股权出质登记信息
            'com_bus_mor_detail': 0,  # 工商核查_现在是否有动产抵押登记信息
            'com_bus_mor_detail_his': 0,  # 工商核查_曾经是否有动产抵押登记信息
            'com_bus_liquidation': 0,  # 工商核查_是否有清算信息
            'com_bus_exception': 0,  # 工商核查_现在是否有经营异常信息
            'com_bus_exception_his': 0,  # 工商核查_曾经是否有经营异常信息
            'com_bus_illegal_list': 0,  # 工商核查_现在是否有严重违法失信信息
            'com_bus_illegal_list_his': 0,  # 工商核查_曾经是否有严重违法失信信息
            'com_bus_registered_capital': None,  # 工商核查_注册资本（万元）
            'com_bus_openfrom': None,  # 工商核查_营业期限自
            'com_bus_enttype': None,  # 工商核查_类型
            'com_bus_esdate': None,  # 工商核查_成立日期
            'com_bus_industryphycode': None,  # 工商核查_行业门类代码
            'com_bus_areacode': None,  # 工商核查_住所所在行政区划代码
            'com_bus_industrycode': None,  # 工商核查_国民经济行业代码
            'com_bus_saicChanLegal_5y': 0,  # 工商核查_法定代表人最近5年内变更次数
            'com_bus_saicChanInvestor_5y': 0,  # 工商核查_投资人最近5年内变更次数
            'com_bus_saicChanRegister_5y': 0,  # 工商核查_注册资本最近5年内变更次数
            'com_bus_saicAffiliated': 0,  # 工商核查_企业关联公司个数
            'com_bus_province': None,  # 工商核查_省
            'com_bus_city': None,  # 工商核查_市
            'com_bus_leg_not_shh': 0,  # 工商核查_法人非股东
            'com_bus_exception_result': 0,  # 工商核查_经营异常原因
            'com_bus_saicChanRunscope': 0,  # 工商核查_经营范围变更次数
            'com_bus_legper_relent_revoke': 0,  # 工商核查_企业和法人关联公司是否存在吊销
            'com_bus_legper_outwardCount1': 0,  # 工商核查_企业、法人对外投资的公司数量
            'com_bus_industryphyname': None  # 工商核查_行业门类名称
        }

    # 获取目标数据集1
    def _info_com_bus_face(self):
        sql = '''
            SELECT ent_status,open_from,open_to,reg_cap,ent_type,es_date,industry_phy_code,area_code,industry_code,
            province,city,industry_phyname
            FROM info_com_bus_face 
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_企业登记状态异常
    def _com_bus_status(self, df=None):
        if df is not None and len(df) > 0:
            if pd.notna(df['ent_status'].values[0]) and ('吊销' in df['ent_status'].values[0] or df['ent_status'].values[0] == '停业'):
                self.variables['com_bus_status'] = 1

     # 计算工商核查_营业期限自
    def _com_bus_openfrom(self, df=None):
        if df is not None and len(df) > 0:
            df1 = df.dropna(subset=['open_from'], how='any')
            if df1 is not None and len(df1) > 0:
                self.variables['com_bus_openfrom'] = str(df['open_from'].values[0])[0:10]

    # 计算工商核查_营业期限至
    def _com_bus_endtime(self, df=None):
        if df is not None and len(df) > 0:
            df1 = df.dropna(subset=['open_to'], how='any')
            if df1 is not None and len(df1) > 0:
                self.variables['com_bus_endtime'] = str(df['open_to'].values[0])[0:10]

    # 计算工商核查_注册资本（万元）
    def _com_bus_registered_capital(self, df=None):
        df = df.dropna(subset=['reg_cap'], how='any')
        if df is not None and len(df) > 0:
            self.variables['com_bus_registered_capital'] = round(df['reg_cap'].values[0] / 10000, 2)

    # 计算工商核查_类型
    def _com_bus_enttype(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_enttype'] = df['ent_type'].values[0]

    # 计算工商核查_成立日期
    def _com_bus_esdate(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_esdate'] = str(df['es_date'].values[0])[0:10]

    # 计算工商核查_行业门类代码
    def _com_bus_industryphycode(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_industryphycode'] = df['industry_phy_code'].values[0]

    # 计算工商核查_住所所在行政区划代码
    def _com_bus_areacode(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_areacode'] = df['area_code'].values[0]

    # 计算工商核查_国民经济行业代码
    def _com_bus_industrycode(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_industrycode'] = df['industry_code'].values[0]

    # 计算工商核查_省
    def _com_bus_province(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_province'] = df['province'].values[0]

    # 计算工商核查_市
    def _com_bus_city(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_city'] = df['city'].values[0]

    # 计算工商核查_行业门类名称
    def _com_bus_industryphyname(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_industryphyname'] = df['industry_phyname'].values[0]

    # 获取目标数据集2
    def _info_com_bus_entinvitem_frinv(self):
        sql1 = '''
            SELECT ent_name,ent_status
            FROM info_com_bus_entinvitem
            WHERE funded_ratio >= 0.2 
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        sql2 = '''
            SELECT ent_name,ent_status
            FROM info_com_bus_frinv
            WHERE fr_name 
            IN (
                SELECT cbb.ent_name 
                FROM (
                    SELECT ent_name
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            )
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df1 = sql_to_df(sql=sql1, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=sql2, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df = pd.concat([df1, df2], ignore_index=True)
        return df

    # 计算工商核查_关联公司吊销个数
    def _com_bus_relent_revoke(self, df=None):
        df = df.dropna(subset=['ent_status'], how='any')
        df = df[df.ent_status.str.contains('吊销')]
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['ent_name'], keep='first')
            self.variables['com_bus_relent_revoke'] = len(df)

    # 计算工商核查_企业关联公司个数
    def _com_bus_saicAffiliated(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['ent_name'], keep='first')
            self.variables['com_bus_saicAffiliated'] = len(df)

    # 获取目标数据集3
    def _info_com_bus_case(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_case
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s
                        AND credit_code = %(id_card_no)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有行政处罚信息
    def _com_bus_case_info(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_case_info'] = 1

    # 获取目标数据集4
    def _info_com_bus_shares_frost(self):
        sql = '''
            SELECT judicial_froz_state
            FROM info_com_bus_shares_frost
            WHERE basic_id 
            = (SELECT id
               FROM info_com_bus_basic
               WHERE ent_name = %(user_name)s 
                    AND credit_code = %(id_card_no)s 
                    AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    AND channel_api_no='24001'
                ORDER BY id DESC 
                LIMIT 1
                )    
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有股权冻结信息
    def _com_bus_shares_frost(self, df=None):
        df = df[df['judicial_froz_state'].str.contains('冻结') & ~df['judicial_froz_state'].str.contains('解冻') &
                ~df['judicial_froz_state'].str.contains('解除') & ~df['judicial_froz_state'].str.contains('失效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_frost'] = 1

    # 计算工商核查_曾经是否有股权冻结信息
    def _com_bus_shares_frost_his(self, df=None):
        df = df[df['judicial_froz_state'].str.contains('解冻') | df['judicial_froz_state'].str.contains('解除') |
                df['judicial_froz_state'].str.contains('失效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_frost_his'] = 1

    # 获取目标数据集5
    def _info_com_bus_shares_impawn(self):
        sql = '''
            SELECT imp_exe_state
            FROM info_com_bus_shares_impawn
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有股权出质登记信息
    def _com_bus_shares_impawn(self, df=None):
        df = df[df['imp_exe_state'].str.contains('有效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_impawn'] = 1

    # 计算工商核查_曾经是否有股权出质登记信息
    def _com_bus_shares_impawn_his(self, df=None):
        df = df[df['imp_exe_state'].str.contains('失效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_impawn_his'] = 1

    # 获取目标数据集6
    def _info_com_bus_mort_basic(self):
        sql = '''
            SELECT mort_status
            FROM info_com_bus_mort_basic
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有动产抵押登记信息
    def _com_bus_mor_detail(self, df=None):
        df = df[df['mort_status'].str.contains('有效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_mor_detail'] = 1

    # 计算工商核查_曾经是否有动产抵押登记信息
    def _com_bus_mor_detail_his(self, df=None):
        df = df[df['mort_status'].str.contains('失效')]
        if df is not None and len(df) > 0:
            self.variables['com_bus_mor_detail_his'] = 1

    # 获取目标数据集7
    def _info_com_bus_liquidation(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_liquidation
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_是否有清算信息
    def _com_bus_liquidation(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_liquidation'] = 1

    # 获取目标数据集8
    def _info_com_bus_exception(self):
        sql = '''
            SELECT result_in,result_out,date_out
            FROM info_com_bus_exception
            WHERE basic_id
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有经营异常信息
    def _com_bus_exception(self, df=None):
        # df1 = df.dropna(subset=['result_out'],how='any')
        # df2 = df.query('result_out == ""')
        if df is None or df.shape[0] == 0:
            return
        df_out = df[((pd.isna(df['result_out'])) | (df['result_out'].apply(lambda x: str(x).strip()) == '')) &
                    ((pd.isna(df['date_out'])) | (df['date_out'].apply(lambda x: str(x).strip()) == '')) &
                    (~df['result_in'].str.contains('年报|年度报告'))]
        if df_out.shape[0] > 0:
            self.variables['com_bus_exception'] = 1
        # if len(df) != len(df1):
        #     self.variables['com_bus_exception'] = 1
        # if len(df2) > 0:
        #     self.variables['com_bus_exception'] = 1

    # 计算工商核查_经营异常原因
    @exception('purpose= 工商核查&author=gulongwei')
    def _com_bus_exception_result(self, df=None):
        df = df[df['date_out'].isnull().values == True]
        if len(df) > 0:
            if True in df['result_in'].str.contains('弄虚作假').values:
                self.variables['com_bus_exception_result'] = 3
            elif (True in df['result_in'].str.contains('无法联系').values) or (
                    True in df.result_in.str.contains('无法取得联系').values):
                self.variables['com_bus_exception_result'] = 2
            elif True in df['result_in'].str.contains('年度报告').values:
                self.variables['com_bus_exception_result'] = 1
            else:
                self.variables['com_bus_exception_result'] = 0

    # 计算工商核查_曾经是否有经营异常信息
    def _com_bus_exception_his(self, df=None):
        df1 = df.dropna(subset=['result_out'],how='any')
        df2 = df.query('result_out == ""')
        if len(df1) > 0:
            self.variables['com_bus_exception_his'] = 1
        else:
            self.variables['com_bus_exception_his'] = 0
        if len(df) != len(df2):
            self.variables['com_bus_exception_his'] = 1
        else:
            self.variables['com_bus_exception_his'] = 0

    # 获取目标数据集9
    def _info_com_bus_illegal(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_illegal
            WHERE (illegal_rresult_out is NULL OR illegal_rresult_out = '')
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有严重违法失信信息
    def _com_bus_illegal_list(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_illegal_list'] = 1

    # 获取目标数据集10
    def _info_com_bus_illegal2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_illegal
            WHERE illegal_rresult_out is not NULL and illegal_rresult_out != ''
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 工商核查_曾经是否有严重违法失信信息
    def _com_bus_illegal_list_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_illegal_list_his'] = 1

    # 获取目标数据集11
    def _info_com_bus_alter(self):
        sql = '''
            SELECT a.id,a.alt_item,a.alt_date,b.create_time
            FROM info_com_bus_alter a
            LEFT JOIN info_com_bus_basic b
            ON a.basic_id = b.id
            WHERE a.basic_id
            IN (
                SELECT cbb.basic_id
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s
                        AND credit_code = %(id_card_no)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'alt_date', 'Y')]
        return df

    # 计算工商核查_法定代表人最近5年内变更次数
    def _com_bus_saicChanLegal_5y(self, df=None):
        if df is not None and len(df) > 0:
            df = df[(df.date_dif < 5) & df.alt_item.str.contains('法定代表人')]
            self.variables['com_bus_saicChanLegal_5y'] = len(df)

    # 计算工商核查_投资人最近5年内变更次数
    def _com_bus_saicChanInvestor_5y(self, df=None):
        if df is not None and len(df) > 0:
            df = df[(df.date_dif < 5) & df.alt_item.str.contains('投资人')]
            self.variables['com_bus_saicChanInvestor_5y'] = len(df)

    # 计算工商核查_注册资本最近5年内变更次数
    def _com_bus_saicChanRegister_5y(self, df=None):
        if df is not None and len(df) > 0:
            df = df[(df.date_dif < 5) & df.alt_item.str.contains('注册资本')]
            self.variables['com_bus_saicChanRegister_5y'] = len(df)

    # 计算工商核查_经营范围变更次数
    def _com_bus_saicChanRunscope(self, df=None):
        if df is not None and len(df) > 0:
            df = df[df.alt_item.str.contains('经营范围')]
            self.variables['com_bus_saicChanRunscope'] = len(df)

    # 获取目标数据集12
    def _info_com_bus_face_shareholder(self):
        sql1 = '''
            SELECT fr_name
            FROM info_com_bus_face
            WHERE basic_id
            IN (
                SELECT cbb.basic_id
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s
                        AND credit_code = %(id_card_no)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC
                    LIMIT 1
                ) cbb
            ); 
        '''
        sql2 = '''
            SELECT share_holder_name
            FROM info_com_bus_shareholder
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s
                        AND credit_code = %(id_card_no)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC
                    LIMIT 1
                ) cbb
            ); 
        '''
        df1 = sql_to_df(sql=sql1, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=sql2, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    # 计算工商核查_法人非股东
    def _com_bus_leg_not_shh(self, df=None):
        if df[0] is not None and len(df[0]) > 0 and df[1] is not None and len(df[1]) > 0 :
            if df[0].fr_name.values[0] not in df[1].share_holder_name.values:
                self.variables['com_bus_leg_not_shh'] = 1

    # 获取目标数据集14
    def _info_com_bus_entinvitem_frinv2(self):
        sql1 = '''
            SELECT ent_name,ent_status
            FROM info_com_bus_entinvitem
            WHERE funded_ratio >= 0.2 
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        sql2 = '''
            SELECT ent_name,ent_status
            FROM info_com_bus_frinv
            WHERE funded_ratio >= 0.2 
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        AND channel_api_no='24001'
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df1 = sql_to_df(sql=sql1, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=sql2, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df = pd.concat([df1, df2], ignore_index=True)
        return df

    # 计算工商核查_企业和法人关联公司是否存在吊销
    @exception('purpose= 工商核查&author=gulongwei')
    def _com_bus_legper_relent_revoke(self, df=None):
        if True in df['ent_status'].str.contains('吊销').values:
            self.variables['com_bus_legper_relent_revoke'] = 1

    # 计算工商核查_企业、法人对外投资的公司数量
    def _com_bus_legper_outwardCount1(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates(subset=['ent_name'], keep='first')
            self.variables['com_bus_legper_outwardCount1'] = len(df)

    #  执行变量转换
    def transform(self):
        info_com_bus_face = self._info_com_bus_face()
        if info_com_bus_face is not None and len(info_com_bus_face) > 0:
            self._com_bus_status(info_com_bus_face)
            self._com_bus_openfrom(info_com_bus_face)
            self._com_bus_endtime(info_com_bus_face)
            self._com_bus_registered_capital(info_com_bus_face)
            self._com_bus_enttype(info_com_bus_face)
            self._com_bus_esdate(info_com_bus_face)
            self._com_bus_industryphycode(info_com_bus_face)
            self._com_bus_areacode(info_com_bus_face)
            self._com_bus_industrycode(info_com_bus_face)
            self._com_bus_province(info_com_bus_face)
            self._com_bus_city(info_com_bus_face)
            self._com_bus_industryphyname(info_com_bus_face)
        info_com_bus_entinvitem_frinv = self._info_com_bus_entinvitem_frinv()
        if info_com_bus_entinvitem_frinv is not None and len(info_com_bus_entinvitem_frinv) > 0:
            self._com_bus_relent_revoke(info_com_bus_entinvitem_frinv)
            self._com_bus_saicAffiliated(info_com_bus_entinvitem_frinv)
        self._com_bus_case_info(self._info_com_bus_case())
        info_com_bus_shares_frost = self._info_com_bus_shares_frost()
        if info_com_bus_shares_frost is not None and len(info_com_bus_shares_frost) > 0:
            self._com_bus_shares_frost(info_com_bus_shares_frost)
            self._com_bus_shares_frost_his(info_com_bus_shares_frost)
        info_com_bus_shares_impawn = self._info_com_bus_shares_impawn()
        if info_com_bus_shares_impawn is not None and len(info_com_bus_shares_impawn) > 0:
            self._com_bus_shares_impawn(info_com_bus_shares_impawn)
            self._com_bus_shares_impawn_his(info_com_bus_shares_impawn)
        info_com_bus_mort_basic = self._info_com_bus_mort_basic()
        if info_com_bus_mort_basic is not None and len(info_com_bus_mort_basic) > 0:
            self._com_bus_mor_detail(info_com_bus_mort_basic)
            self._com_bus_mor_detail_his(info_com_bus_mort_basic)
        self._com_bus_liquidation(self._info_com_bus_liquidation())
        info_com_bus_exception = self._info_com_bus_exception()
        if info_com_bus_exception is not None and len(info_com_bus_exception) > 0:
            self._com_bus_exception(info_com_bus_exception)
            self._com_bus_exception_result(info_com_bus_exception)
            self._com_bus_exception_his(info_com_bus_exception)
        self._com_bus_illegal_list(self._info_com_bus_illegal())
        self._com_bus_illegal_list_his(self._info_com_bus_illegal2())
        info_com_bus_alter = self._info_com_bus_alter()
        if info_com_bus_alter is not None and len(info_com_bus_alter) > 0:
            self._com_bus_saicChanLegal_5y(info_com_bus_alter)
            self._com_bus_saicChanInvestor_5y(info_com_bus_alter)
            self._com_bus_saicChanRegister_5y(info_com_bus_alter)
            self._com_bus_saicChanRunscope(info_com_bus_alter)
        self._com_bus_leg_not_shh(self._info_com_bus_face_shareholder())
        info_com_bus_entinvitem_frinv2 = self._info_com_bus_entinvitem_frinv2()
        if info_com_bus_entinvitem_frinv2 is not None and len(info_com_bus_entinvitem_frinv2) > 0:
            self._com_bus_legper_relent_revoke(info_com_bus_entinvitem_frinv2)
            self._com_bus_legper_outwardCount1(info_com_bus_entinvitem_frinv2)
