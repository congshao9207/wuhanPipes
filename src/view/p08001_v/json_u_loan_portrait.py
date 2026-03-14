import pandas as pd

from view.TransFlow import TransFlow
from pandas.tseries.offsets import *


class JsonUnionLoanPortrait(TransFlow):

    def process(self):
        self.process_form_detail()

    def process_form_detail(self):
        # 表单信息
        bank_form_detail = []
        wxzfb_form_detail = []
        # 专家经验
        bank_risk = ""
        wxzfb_risk = ""
        df = self.trans_u_flow_portrait.copy()
        if df.shape[0] > 0:
            # 获取一年前的日期
            year_ago = pd.to_datetime(df['trans_time']).max() - DateOffset(months=12)
            # 筛选近一年经营性流水，计算近一年经营性收入
            normal_df = df.loc[pd.isna(df.relationship) & pd.isna(df.loan_type) & pd.isna(df.unusual_trans_type)]
            normal_income_amt = normal_df.query('trans_amt >= 0')['trans_amt'].sum()
            # 筛选近一年多头数据
            df = df.loc[(pd.to_datetime(df.trans_date) >= year_ago) & (pd.notna(df.loan_type))]
            # 汇总数据
            if df.shape[0] > 0:
                bank_df = df.loc[df.trans_flow_src_type == 0]
                wxzfb_df = df.loc[df.trans_flow_src_type == 1]
                if bank_df.shape[0] > 0:
                    bank_risk, bank_form_detail = self.get_risk_detail(self, bank_df, normal_income_amt)
                if wxzfb_df.shape[0] > 0:
                    wxzfb_risk, wxzfb_form_detail = self.get_risk_detail(self, wxzfb_df, normal_income_amt)

        self.variables['bull_credit_risk'] = {
            'bank_flow': {
                'form_detail': bank_form_detail,
                'risk_tips': bank_risk
            },
            'wxzfb_flow': {
                'form_detail': wxzfb_form_detail,
                'risk_tips': wxzfb_risk
            }
        }

    @staticmethod
    def get_risk_detail(self, df, normal_income_amt):
        def func1(x): return x[x >= 0].sum()
        def func2(x): return int(x[x >= 0].count())
        def func3(x): return x[x < 0].abs().sum()
        def func4(x): return int(x[x < 0].count())
        u_df = df.groupby('loan_type', as_index=False).agg({
            'trans_amt': [func1, func2, func3, func4, 'sum', 'count']})
        u_df.columns = ['loan_type', 'loan_amt', 'loan_cnt', 'repay_amt', 'repay_cnt',
                        'net_loan_amt', 'net_loan_cnt']
        que_df = pd.DataFrame({'loan_type': ['银行', '消金', '融资租赁', '担保', '保理', '小贷', '信托', '第三方支付',
                                             '民间借贷', '其他金融']})
        u_df = pd.merge(que_df, u_df, how='inner', on='loan_type')
        # u_df.columns = ['loan_type', 'loan_amt', 'loan_cnt', 'repay_amt', 'repay_cnt',
        #                 'net_loan_amt', 'net_loan_cnt']
        u_df['loan_cnt'] = u_df['loan_cnt'].astype(int)
        u_df['repay_cnt'] = u_df['repay_cnt'].astype(int)
        # 专家经验
        # 1、贷款发放额>=经营性收入*20%，提示"贷款发放额xx万，与可核实的经营规模相比，负债规模偏大"
        # 2、贷款发放额>=经营性收入，提示"贷款发放额xx万，负债规模较大，建议谨慎授信"
        # 7、还款总额>经营性收入*80%，提示"还款总额xx万，收支不平衡，建议谨慎授信"
        risk = ""
        if normal_income_amt != 0:
            if u_df.loan_amt.sum() >= normal_income_amt:
                risk = f"贷款发放额{u_df.loan_amt.sum() / 10000:.2f}万，负债规模较大，建议谨慎授信;"
            elif u_df.loan_amt.sum() >= normal_income_amt * 0.2:
                risk = f"贷款发放额{u_df.loan_amt.sum() / 10000:.2f}万，与可核实的经营规模相比，负债规模偏大;"
            if u_df.repay_amt.sum() > normal_income_amt * 0.8:
                risk = risk + f"还款总额{u_df.repay_amt.sum() / 10000:.2f}万，收支不平衡，建议谨慎授信;"
        # 3、若非银机构数>3家，提示"申请人存在非银机构融资，隐形负债风险较大"
        # 4、若非银机构数>5家，提示"存在非银机构融资家数较多，建议谨慎授信"
        not_bank_cnt = u_df.loc[~(u_df.loan_type == '银行')]['loan_type'].nunique()
        if not_bank_cnt > 5:
            risk = risk + f"申请人存在{not_bank_cnt}家非银机构融资，建议谨慎授信;"
        elif not_bank_cnt > 3:
            risk = risk + f"申请人存在{not_bank_cnt}家非银机构融资，隐形负债风险较大;"
        # 5、发放额/日均>50，提示"相对于负债规模日均较少，资金紧张程度较高"
        # 6、发放金额<10万，日均>7笔，提示"小额度放款较多，融资能力较弱，资金紧张程度较高"
        days = (df.trans_date.max() - df.trans_date.min()).days + 1
        half_balance = self.variables['trans_report_overview']['trans_general_info']['trans_scale']['risk_tips']
        half_balance = int(''.join([_ for _ in half_balance if _ in '0123456789'])) if half_balance != '' else 0
        # avg_loan_amt = u_df.loan_amt.sum() / days
        avg_loan_cnt = u_df.loan_cnt.sum() / days
        if half_balance > 0 and u_df.loan_amt.sum() / half_balance > 50000:
            risk = risk + f"贷款发放额{u_df.loan_amt.sum() / 10000:.2f}万，日均{half_balance / 100:.2f}万，" \
                          f"日均相对负债较少，资金紧张程度较高;"
        if u_df.loan_amt.sum() < 10 * 10000 and avg_loan_cnt > 7:
            risk = risk + f"小额发放笔数{avg_loan_cnt}笔，发放金额{u_df.loan_amt.sum() / 10000:.2f}万，" \
                          f"融资能力较弱，资金紧张程度较高;"
        # 表单
        # float类型保留两位小数
        for col in u_df.select_dtypes(include=['float64', 'float32', 'float']).columns.tolist():
            u_df[col] = u_df[col].apply(lambda x: '%.2f' % x)
        form_detail = u_df.to_dict('records')
        # 表单详情
        u_detail_col = ['bank', 'account_no', 'trans_time', 'opponent_name', 'trans_amt',
                        'trans_use', 'remark', 'loan_type']
        u_detail_df = df[u_detail_col]
        u_detail_df['account_no'] = u_detail_df['account_no'].fillna("").astype(str)
        u_detail_df['account_no'] = u_detail_df['account_no'].apply(lambda x: self.flow_account_clean(x))
        u_detail_df['trans_time'] = u_detail_df.trans_time.astype(str).apply(lambda x: x.replace(" 00:00:00", ""))
        u_detail_df['trans_amt'] = u_detail_df.trans_amt.apply(lambda x: '%.2f' % x)
        u_detail_df['remark'] = u_detail_df[['remark', 'trans_use']].fillna("").apply(
            lambda x: ",".join([x['remark'], x['trans_use']]) if len(x['trans_use']) > 0 and len(
                x['remark']) > 0 else "".join([x['remark'], x['trans_use']]), axis=1)
        u_detail_df.drop(['trans_use'], inplace=True, axis=1)
        for i in range(0, len(form_detail)):
            temp_df = u_detail_df.loc[u_detail_df.loan_type == form_detail[i]['loan_type']]
            temp_df.drop('loan_type', axis=1, inplace=True)
            form_detail[i]['detail'] = temp_df.to_dict('records')
        return risk, form_detail
