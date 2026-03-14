import re
from functools import reduce

from mapping.biz_constants import CONTRACT_DISPUTES
from mapping.tranformer import Transformer, subtract_datetime_col
from util.common_util import exception
from util.mysql_reader import sql_to_df


def get_money(var):
    if var is not None and len(var) > 0:
        if len(list(map(float, re.findall(r"\d+\.?\d*", var)))) > 0:
            value = max(list(map(float, re.findall(r"\d+\.?\d*", var))))
        else:
            value = float(0)
        return value
    else:
        return 0


def get_spec_money1(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=万元\)\:)\d+\.?\d*").search(var) != None:
            value = float(re.compile(r"(?<=万元\)\:)\d+\.?\d*").search(var).group(0)) * 10000
        else:
            value = float(0)
        return value
    else:
        return 0


def get_spec_money2(var):
    if var is not None and len(var) > 0:
        if re.compile(r"(?<=执行标的\:)\d+\.?\d*").search(var) != None:
            value = float(re.compile(r"(?<=执行标的\:)\d+\.?\d*").search(var).group(0))
        elif re.compile(r"(?<=金额\:)\d+\.?\d*").search(var) != None:
            value = float(re.compile(r"(?<=金额\:)\d+\.?\d*").search(var).group(0))
        else:
            value = float(0)
        return value
    else:
        return 0


class T16001(Transformer):
    """
    法院核查个人
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'court_admi_vio': 0,
            'court_judge': 0,
            'court_trial_proc': 0,
            'court_tax_pay': 0,
            'court_owed_owe': 0,
            'court_tax_arrears': 0,
            'court_dishonesty': 0,
            'court_limit_entry': 0,
            'court_high_cons': 0,
            'court_pub_info': 0,
            'court_cri_sus': 0,
            'court_tax_arrears_amt_3y': 0,
            'court_pub_info_amt_3y': 0,
            'court_admi_vio_amt_3y': 0,
            'court_judge_amt_3y': 0,
            'court_docu_status': 0,
            'court_proc_status': 0,
            'court_fin_loan_con': 0,
            'court_loan_con': 0,
            'court_pop_loan': 0,
            'court_tax_arrears_max': 0,
            'court_pub_info_max': 0,
            'court_admi_violation_max': 0,
            'court_judge_max': 0
        }

    def _info_admi_vio_df(self):
        info_admi_vio = """
            SELECT a.unique_name, a.unique_id_no,a.create_time,b.court_id,b.specific_date,b.execution_result
            FROM info_court as a
            inner join info_court_administrative_violation as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_admi_vio,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    @exception('purpose= 个人法院核查&author=Allen')
    def _admi_vio(self, df=None):
        if df is not None and len(df) > 0:
            self.year = subtract_datetime_col(df, 'create_time', 'specific_date', 'Y')
            df['amt'] = df.apply(lambda x: get_money(x['execution_result']), axis=1)
            self.variables['court_admi_vio'] = df.shape[0]
            self.variables['court_admi_vio_amt_3y'] = df[df[self.year] < 3].fillna(0)['amt'].sum()
            self.variables['court_admi_violation_max'] = df.fillna(0)['amt'].max()

    def _info_judge_df(self):
        info_judge = """
            SELECT a.unique_name, a.unique_id_no,a.create_time,b.court_id,b.closed_time,b.case_amount,b.legal_status
            FROM info_court as a
            inner join info_court_judicative_pape as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_judge,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    @exception('purpose= 个人法院核查&author=Allen')
    def _judge(self, df=None):
        if df is not None and len(df) > 0:
            self.year = subtract_datetime_col(df, 'create_time', 'closed_time', 'Y')
            self.variables['court_judge'] = df.shape[0]
            self.variables['court_judge_amt_3y'] = df[df[self.year] < 3].fillna(0)['case_amount'].sum()
            self.variables['court_judge_max'] = round(df.fillna(0)['case_amount'].max(), 2)
            df1 = df.dropna(subset=['legal_status'], how='any')
            defendant_df = df1[df1['legal_status'].str.contains('被告')]
            plaintiff_df = df1[df1['legal_status'].str.contains('原告')]
            if df[df['legal_status'] == ''].shape[0] == df.shape[0]:
                self.variables['court_docu_status'] = 0
            elif plaintiff_df.shape[0] > 0 and plaintiff_df.shape[0] == df1.shape[0]:
                self.variables['court_docu_status'] = 1
            elif plaintiff_df.shape[0] < df1.shape[0] and \
                    defendant_df.shape[0] == 0:
                self.variables['court_docu_status'] = 3
            elif defendant_df.shape[0] > 0:
                self.variables['court_docu_status'] = 2

    def _info_trial_proc_df(self):
        info_trial_proc = """
            SELECT a.unique_name, a.unique_id_no,b.court_id,b.legal_status
            FROM info_court as a
            inner join info_court_trial_process as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_trial_proc,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    @exception('purpose= 个人法院核查&author=Allen')
    def _trial_proc(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_trial_proc'] = df.shape[0]
            df1 = df.dropna(subset=['legal_status'], how='any')
            defendant_df = df1[df1['legal_status'].str.contains('被告')]
            plaintiff_df = df1[df1['legal_status'].str.contains('原告')]
            if df[df['legal_status'] == ''].shape[0] == df.shape[0]:
                self.variables['court_proc_status'] = 0
            elif plaintiff_df.shape[0] > 0 and plaintiff_df.shape[0] == df1.shape[0]:
                self.variables['court_proc_status'] = 1
            elif plaintiff_df.shape[0] < df1.shape[0] and \
                    defendant_df.shape[0] == 0:
                self.variables['court_proc_status'] = 3
            elif defendant_df.shape[0] > 0:
                self.variables['court_proc_status'] = 2

    def _info_tax_pay_df(self):
        info_tax_pay = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_taxable_abnormal_user as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_tax_pay,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _tax_pay(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_tax_pay'] = df.shape[0]

    def _info_owed_owe_df(self):
        info_owed_owe = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_arrearage as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_owed_owe,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _owed_owe(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_owed_owe'] = df.shape[0]

    def _info_tax_arrears_df(self):
        info_tax_arrears = """
            SELECT a.unique_name, a.unique_id_no,a.create_time,b.court_id,b.taxes,b.taxes_time
            FROM info_court as a
            inner join info_court_tax_arrears as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_tax_arrears,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _tax_arrears(self, df=None):
        if df is not None and len(df) > 0:
            self.year = subtract_datetime_col(df, 'create_time', 'taxes_time', 'Y')
            self.variables['court_tax_arrears'] = df.shape[0]
            self.variables['court_tax_arrears_amt_3y'] = df[df[self.year] < 3].fillna(0)['taxes'].sum()
            self.variables['court_tax_arrears_max'] = df.fillna(0)['taxes'].max()

    def _info_dishonesty_df(self):
        info_dishonesty = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_deadbeat as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_dishonesty,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _dishonesty(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_dishonesty'] = df.shape[0]

    def _info_limit_entry_df(self):
        info_limit_entry = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_limited_entry_exit as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_limit_entry,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _limit_entry(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_limit_entry'] = df.shape[0]

    def _info_high_cons_df(self):
        info_high_cons = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_limit_hignspending as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_high_cons,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _high_cons(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_high_cons'] = df.shape[0]

    def _info_pub_info_df(self):
        info_pub_info = """
            SELECT a.unique_name, a.unique_id_no,a.create_time,b.court_id,b.filing_time,b.execute_content
            FROM info_court as a
            inner join info_court_excute_public as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_pub_info,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    @exception('purpose= 个人法院核查&author=Allen')
    def _pub_info(self, df=None):
        if df is not None and len(df) > 0:
            self.year = subtract_datetime_col(df, 'create_time', 'filing_time', 'Y')
            df['amt'] = df.apply(
                lambda x: max(get_spec_money1(x['execute_content']), get_spec_money2(x['execute_content'])), axis=1)
            self.variables['court_pub_info'] = df.shape[0]
            self.variables['court_pub_info_amt_3y'] = df[df[self.year] < 3].fillna(0)['amt'].sum()
            self.variables['court_pub_info_max'] = df.fillna(0)['amt'].max()

    def _info_cri_sus_df(self):
        info_cri_sus = """
            SELECT a.unique_name, a.unique_id_no,b.court_id
            FROM info_court as a
            inner join info_court_criminal_suspect as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_cri_sus,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _cri_sus(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_cri_sus'] = df.shape[0]

    def _info_court_loan_df(self):
        info_court_loan = """
            SELECT a.unique_name, a.unique_id_no,b.case_reason,b.legal_status,
            c.case_reason as trial_reason,c.legal_status as trial_status 
            FROM info_court as a
            left join info_court_judicative_pape as b on a.id=b.court_id
            left join info_court_trial_process as c on a.id=c.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.unique_name = %(user_name)s AND a.unique_id_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=info_court_loan,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _court_loan(self, df=None):
        contract_disputes = reduce(lambda x, y: x + "|" + y, CONTRACT_DISPUTES)
        if df is not None and len(df) > 0:
            if df[(df['legal_status'].str.contains('被告')) & (df['case_reason'].str.contains('金融借款合同纠纷'))].shape[0] > 0:
                self.variables['court_fin_loan_con'] = 1
            elif df[(df['trial_status'].str.contains('被告')) & (df['trial_reason'].str.contains('金融借款合同纠纷'))].shape[
                0] > 0:
                self.variables['court_fin_loan_con'] = 1
            if df[(df['legal_status'].str.contains('被告')) & (df['case_reason'].str.contains(contract_disputes))].shape[0] > 0:
                self.variables['court_loan_con'] = 1
            elif df[(df['trial_status'].str.contains('被告')) & (df['trial_reason'].str.contains(contract_disputes))].shape[0] > 0:
                self.variables['court_loan_con'] = 1
            if df[(df['legal_status'].str.contains('被告')) & (df['case_reason'].str.contains('民间借贷纠纷'))].shape[0] > 0:
                self.variables['court_pop_loan'] = 1
            elif df[(df['trial_status'].str.contains('被告')) & (df['trial_reason'].str.contains('民间借贷纠纷'))].shape[0] > 0:
                self.variables['court_pop_loan'] = 1

    # 以下为灰名单相关变量
    # 法院核查_个人_失信老赖名单命中的最新时间
    # court_dishonesty_time
    # select max(execute_date) from info_court_deadbeat where court_id in(select id from info_court where
    # unique_name="施网明" and unique_id_no="310108196610024859");
    def _court_dishonesty_time_df(self):
        str_sql = '''
            select max(execute_date) as hit_time from info_court_deadbeat where court_id in(select id from 
            info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s); 
            '''
        df = sql_to_df(sql=str_sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 法院核查_个人_限制高消费名单命中的最新时间
    # court_high_cons_time
    # select max(specific_date) from info_court_limit_hignspending where court_id in(select id from info_court where
    # unique_name='上海乾享机械设备有限公司' and unique_id_no='913101207747584215');
    def _court_high_cons_time_df(self):
        str_sql = '''
                    select max(specific_date) as hit_time from info_court_limit_hignspending where court_id in(select id from 
                    info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s) 
                    '''
        df = sql_to_df(sql=str_sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 法院核查_个人_借款合同纠纷命中的最新时间
    # court_loan_con_time
    def _court_loan_con_time_df(self):
        return self._loan_time_df("借款合同纠纷", "被告")

    # 法院核查_个人_民间借贷纠纷命中的最新时间
    # court_pop_loan_time
    def _court_pop_loan_time_df(self):
        return self._loan_time_df("民间借贷纠纷", "被告")

    def _loan_time_df(self, case_reason, legal_status):
        str_sql1 = '''
                        select max(closed_time) as hit_time from info_court_judicative_pape where court_id in(select id 
                        from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s) and
                         case_reason like %(case_reason)s and legal_status like %(legal_status)s ; 
                        '''

        str_sql2 = '''
                        select max(specific_date) as hit_time from info_court_trial_process where court_id in(select id 
                        from info_court where unique_name=%(user_name)s and unique_id_no=%(id_card_no)s) and 
                        case_reason like %(case_reason)s and legal_status like %(legal_status)s
                        '''

        df1 = sql_to_df(sql=str_sql1,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no,
                                'case_reason': "%" + case_reason + "%",
                                "legal_status": "%" + legal_status + "%"})

        df2 = sql_to_df(sql=str_sql2,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no,
                                "case_reason": "%" + case_reason + "%",
                                "legal_status": "%" + legal_status + "%"})

        df1 = df1.dropna()
        df2 = df2.dropna()
        if not df1.empty and not df2.empty:
            if df1.hit_time.iloc[0] > df2.hit_time.iloc[0]:
                return df1
            else:
                return df2

        return df1 if not df1.empty else df2

    def _parse_time_df(self, var_name, df=None):
        if df is not None:
            df = df.dropna()
            if not df.empty:
                self.variables[var_name] = str(df.hit_time.iloc[0])
                return

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._admi_vio(self._info_admi_vio_df())
        self._judge(self._info_judge_df())
        self._trial_proc(self._info_trial_proc_df())
        self._tax_pay(self._info_tax_pay_df())
        self._owed_owe(self._info_owed_owe_df())
        self._tax_arrears(self._info_tax_arrears_df())
        self._dishonesty(self._info_dishonesty_df())
        self._limit_entry(self._info_limit_entry_df())
        self._high_cons(self._info_high_cons_df())
        self._pub_info(self._info_pub_info_df())
        self._cri_sus(self._info_cri_sus_df())
        self._court_loan(self._info_court_loan_df())

        # 灰名单变量
        self._parse_time_df("court_dishonesty_time", self._court_dishonesty_time_df())
        self._parse_time_df("court_high_cons_time", self._court_high_cons_time_df())
        self._parse_time_df("court_loan_con_time", self._court_loan_con_time_df())
        self._parse_time_df("court_pop_loan_time", self._court_pop_loan_time_df())
