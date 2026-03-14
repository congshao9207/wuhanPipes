import pandas as pd

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df


def dongjie(var):
    if ('冻结' in var) and ('解冻' not in var) and ('解除' not in var) and ('失效' not in var):
        result = True
    else:
        result = False
    return result


def jiedong(var):
    if ('解冻' in var) or ('解除' in var) or ('失效' in var):
        result = True
    else:
        result = False
    return result


class Tf0003(Transformer):
    """
    联企工商
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'per_com_case_info': 0,
            'per_com_shares_frost': 0,
            'per_com_shares_frost_his': 0,
            'per_com_shares_impawn': 0,
            'per_com_shares_impawn_his': 0,
            'per_com_mor_detail': 0,
            'per_com_mor_detail_his': 0,
            'per_com_liquidation': 0,
            'per_com_exception': 0,
            'per_com_exception_his': 0,
            'per_com_illegal_list': 0,
            'per_com_illegal_list_his': 0,
            'per_com_saicChanLegal': 0,
            'per_com_saicChanInvestor': 0,
            'per_com_exception_result': 0,
            'per_com_saicChanRunscope': 0,
            'per_com_legper_relent_revoke': 0,
            'per_com_legper_outwardCount1': 0,
            'per_com_industryphycode': '',
            'per_com_endtime': '',
            'per_com_openfrom': '',
            'per_com_esdate': '',
            'per_com_areacode': '',
            'per_com_industrycode': '',
            'per_com_saicChanRegister_5y': 0,
            'per_com_province': '',
            'per_com_city': ''
        }
        self.out_decision_code = {
            'IT004': None,
            'IT012': None,
            'IT010': None,
            'IT005': None,
            'IT016': None,
            'I006': None,
            'IM006': None,
            'IT017': None,
            'I007': None,
            'IM007': None,
            'IT018': None,
            'I005': None,
            'IM005': None,
            'I004': None,
            'IM004': None,
            'IT015': None,
            'IT019': None,
            'IT011': None,
            'IT007': None,
            'IT006': None,
            'IT008': None,
            'I002': None,
            'IM002': None,
            'IT013': None,
            'I003': None,
            'IM003': None,
            'IT014': None
        }

    def _info_sql_shareholder_df(self):
        info_sql_df = """
             SELECT b.ent_name,b.credit_code
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
             SELECT b.ent_name,b.credit_code
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

    def _info_case_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_case as d
            on c.id=d.basic_id 
            WHERE c.ent_name = %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _case_info(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['per_com_case_info'] = 1

    def _info_shares_frost_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.judicial_froz_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_frost as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _shares_frost(self, df=None):
        if df is not None and len(df) > 0:
            df['dongjie'] = df.apply(lambda x: dongjie(x['judicial_froz_state']), axis=1)
            df['jiedong'] = df.apply(lambda x: jiedong(x['judicial_froz_state']), axis=1)
            if df[df['dongjie']].shape[0] > 0:
                self.variables['per_com_shares_frost'] = 1
            if df[df['jiedong']].shape[0] > 0:
                self.variables['per_com_shares_frost_his'] = 1

    def _info_shares_impawn_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.imp_exe_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_impawn as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _shares_impawn(self, df=None):
        if df is not None and len(df) > 0:
            df['youxiao'] = df.apply(lambda x: True if '有效' in x['imp_exe_state'] else False, axis=1)
            df['shixiao'] = df.apply(lambda x: True if '失效' in x['imp_exe_state'] else False, axis=1)
            if df[df['youxiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn'] = 1
            if df[df['shixiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn_his'] = 1

    def _info_mor_detail_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.mort_status
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_mort_basic as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _mor_detail(self, df=None):
        if df is not None and len(df) > 0:
            df['youxiao'] = df.apply(lambda x: True if '有效' in x['mort_status'] else False, axis=1)
            df['shixiao'] = df.apply(lambda x: True if '失效' in x['mort_status'] else False, axis=1)
            if df[df['youxiao']].shape[0] > 0:
                self.variables['per_com_mor_detail'] = 1
            if df[df['shixiao']].shape[0] > 0:
                self.variables['per_com_mor_detail_his'] = 1

    def _info_liquidation_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_liquidation as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _liquidation_info(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['per_com_liquidation'] = 1

    def _info_exception_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.result_out,d.result_in,d.date_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_exception as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _exception_info(self, df=None):
        if df is not None and len(df) > 0:
            if df[df['result_out'] == ''].shape[0] > 0:
                self.variables['per_com_exception'] = 1
            if df[df['result_out'] == ''].shape[0] != df.shape[0]:
                self.variables['per_com_exception_his'] = 1
            if df[(df['date_out'].isna()) & (df['result_in'].str.contains('弄虚作假'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 3
            elif df[(df['date_out'].isna()) & (df['result_in'].str.contains('无法联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df[(df['date_out'].isna()) & (df['result_in'].str.contains('无法取得联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df[(df['date_out'].isna()) & (df['result_in'].str.contains('年度报告'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 1

    def _info_illegal_list_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.illegal_rresult_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_illegal as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _illegal_list_info(self, df=None):
        if df is not None and len(df) > 0:
            if df[df['illegal_rresult_out'].isna()].shape[0] > 0:
                self.variables['per_com_illegal_list'] = 1
            else:
                self.variables['per_com_illegal_list_his'] = 1

    def _info_saicChanLegal_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.alt_date,d.alt_item
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_alter as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _saicChanLegal_info(self, df=None):
        if df is not None and len(df) > 0:
            self.year1 = subtract_datetime_col(df, 'create_time', 'alt_date', 'Y')
            self.variables['per_com_saicChanLegal'] = \
                df[(df['alt_item'].str.contains('法定代表人')) & (df[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanInvestor'] = \
                df[(df['alt_item'].str.contains('投资人')) & (df[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanRunscope'] = df[df['alt_item'].str.contains('经营范围')].shape[0]
            self.variables['per_com_saicChanRegister_5y'] = \
                df[(df['alt_item'].str.contains('注册资本')) & (df[self.year1] < 5)].shape[0]

    def _info_legper_df(self, ent_name):
        info_per_bus_shareholder_entinvitem = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_entinvitem as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        info_per_bus_shareholder_frinv = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_frinv as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder_entinvitem,
                       params={"ent_name": ent_name})
        df1 = sql_to_df(sql=info_per_bus_shareholder_frinv,
                        params={"ent_name": ent_name})
        return df, df1

    def _legper_info(self, df=None, df1=None):
        if (df is not None and len(df) > 0) or (df1 is not None and len(df1) > 0):
            if df[(df['ent_status'].str.contains('吊销')) & (df['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            elif df1[(df1['ent_status'].str.contains('吊销')) & (df1['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            df5 = pd.concat([df[df['funded_ratio'] >= 0.2], df1[df1['funded_ratio'] >= 0.2]])
            df5.drop_duplicates(subset=['ent_name'], inplace=True)
            self.variables['per_com_legper_outwardCount1'] = df5.shape[0]

    def _info_industryphycode_df(self, ent_name):
        info_per_bus_shareholder = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.industry_phy_code,d.open_to,d.open_from,d.es_date,
            d.area_code,d.industry_code,d.province,d.city,d.expired_at
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_face as d
            on c.id=d.basic_id
            WHERE c.ent_name =  %(ent_name)s and unix_timestamp(NOW()) < unix_timestamp(c.expired_at)
            and c.channel_api_no = '24001'
            ORDER BY d.id DESC LIMIT 1
           ;
        """
        df = sql_to_df(sql=info_per_bus_shareholder,
                       params={"ent_name": ent_name})
        return df

    def _industryphycode_info(self, df=None):
        if df is not None and len(df) > 0:
            def transform_datetime(input_param):
                if pd.isna(input_param):
                    return ''
                try:
                    res = format(input_param, '%Y-%m-%d')
                except:
                    res = str(input_param)
                return res
            self.variables['per_com_industryphycode'] = df['industry_phy_code'][0]
            self.variables['per_com_endtime'] = transform_datetime(df['open_to'][0])
            self.variables['per_com_openfrom'] = transform_datetime(df['open_from'][0])
            self.variables['per_com_esdate'] = transform_datetime(df['es_date'][0])
            self.variables['per_com_areacode'] = df['area_code'][0]
            self.variables['per_com_industrycode'] = df['industry_code'][0]
            self.variables['per_com_province'] = df['province'][0]
            self.variables['per_com_city'] = df['city'][0]

    def transform(self):
        """
        执行变量转换
        :return:
        """
        ent_name_df = self._info_sql_shareholder_df()
        ent_name_df1 = self._info_sql_legal_df()

        if ent_name_df.shape[0] > 0:
            ent_name = ent_name_df['ent_name'][0]
            case_df = self._info_case_df(ent_name=ent_name)
            self._case_info(case_df)

            shares_fronts_df = self._info_shares_frost_df(ent_name=ent_name)
            self._shares_frost(shares_fronts_df)

            shares_impawn_df = self._info_shares_impawn_df(ent_name=ent_name)
            self._shares_impawn(shares_impawn_df)

            mor_detail_df = self._info_mor_detail_df(ent_name=ent_name)
            self._mor_detail(mor_detail_df)

            liquidation_df = self._info_liquidation_df(ent_name=ent_name)
            self._liquidation_info(liquidation_df)

            exception_df = self._info_exception_df(ent_name=ent_name)
            self._exception_info(exception_df)

            illegal_list_df = self._info_illegal_list_df(ent_name=ent_name)
            self._illegal_list_info(illegal_list_df)

            saic_chan_legal_df = self._info_saicChanLegal_df(ent_name=ent_name)
            self._saicChanLegal_info(saic_chan_legal_df)

            legper_df = self._info_legper_df(ent_name=ent_name)
            self._legper_info(legper_df[0], legper_df[1])

            industryphycode_df = self._info_industryphycode_df(ent_name=ent_name)
            self._industryphycode_info(industryphycode_df)

            df = ent_name_df.drop_duplicates()
            array = [{col: str(df[col][0]) for col in df.columns}]
            self.out_decision_code = {
                'IT004': array,
                'IT012': array,
                'IT010': array,
                'IT005': array,
                'IT016': array,
                 'I006': array,
                'IM006': array,
                'IT017': array,
                 'I007': array,
                'IM007': array,
                'IT018': array,
                 'I005': array,
                'IM005': array,
                 'I004': array,
                'IM004': array,
                'IT015': array,
                'IT019': array,
                'IT011': array,
                'IT007': array,
                'IT006': array,
                'IT008': array,
                 'I002': array,
                'IM002': array,
                'IT013': array,
                 'I003': array,
                'IM003': array,
                'IT014': array
            }
        elif ent_name_df1.shape[0] > 0:
            ent_name = ent_name_df1['ent_name'][0]
            case_df = self._info_case_df(ent_name=ent_name)
            self._case_info(case_df)

            shares_fronts_df = self._info_shares_frost_df(ent_name=ent_name)
            self._shares_frost(shares_fronts_df)

            shares_impawn_df = self._info_shares_impawn_df(ent_name=ent_name)
            self._shares_impawn(shares_impawn_df)

            mor_detail_df = self._info_mor_detail_df(ent_name=ent_name)
            self._mor_detail(mor_detail_df)

            liquidation_df = self._info_liquidation_df(ent_name=ent_name)
            self._liquidation_info(liquidation_df)

            exception_df = self._info_exception_df(ent_name=ent_name)
            self._exception_info(exception_df)

            illegal_list_df = self._info_illegal_list_df(ent_name=ent_name)
            self._illegal_list_info(illegal_list_df)

            saic_chan_legal_df = self._info_saicChanLegal_df(ent_name=ent_name)
            self._saicChanLegal_info(saic_chan_legal_df)

            legper_df = self._info_legper_df(ent_name=ent_name)
            self._legper_info(legper_df[0], legper_df[1])

            industryphycode_df = self._info_industryphycode_df(ent_name=ent_name)
            self._industryphycode_info(industryphycode_df)

            df = ent_name_df1.drop_duplicates()
            array = [{col: str(df[col][0]) for col in df.columns}]
            self.out_decision_code = {
                'IT004': array,
                'IT012': array,
                'IT010': array,
                'IT005': array,
                'IT016': array,
                 'I006': array,
                'IM006': array,
                'IT017': array,
                 'I007': array,
                'IM007': array,
                'IT018': array,
                 'I005': array,
                'IM005': array,
                 'I004': array,
                'IM004': array,
                'IT015': array,
                'IT019': array,
                'IT011': array,
                'IT007': array,
                'IT006': array,
                'IT008': array,
                 'I002': array,
                'IM002': array,
                'IT013': array,
                 'I003': array,
                'IM003': array,
                'IT014': array
            }
