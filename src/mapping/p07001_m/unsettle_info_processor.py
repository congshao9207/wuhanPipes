from mapping.module_processor import ModuleProcessor
import pandas as pd

# unsettled开头的变量


class UnSettleInfoProcessor(ModuleProcessor):
    def process(self):
        self._unsettled_consume_total_cnt()
        self._unsettled_busLoan_agency_number()
        self._unsettled_consume_agency_cnt()
        self._uncancelled_credit_organization_number()
        self._unsettled_busLoan_total_cnt()
        self._unsettled_loan_agency_number()
        self._unsettled_consume_total_amount()
        self._unsettled_loan_number()  # 未结清贷款笔数
        self._unsettled_house_loan_number()  # 未结清房贷笔数

    # 有经营性贷款在贷余额的合作机构数
    def _unsettled_busLoan_agency_number(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99,15,16,22或者包含“融资租赁”，或者(account_type=04且loan_amount>200000且在贷余额大于放款金额的20%（不含）))的account_org
        # 2.统计1中不同的account_org数目
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df[(credit_loan_df['account_type'].isin(['01','02','03'])) &
                                        (
                                            (credit_loan_df['loan_type'].isin(['01','07','99','15','16','22'])) |
                                            (credit_loan_df['loan_type'].str.contains('融资租赁')) |
                                            ((credit_loan_df['loan_type'] == '04') & (credit_loan_df['loan_amount'] >= 200000) & (credit_loan_df['loan_balance']>credit_loan_df['loan_amount']*0.2))
                                        ) &
                                        (credit_loan_df['loan_balance'] > 0)]
        if credit_loan_df.empty:
            return
        count = credit_loan_df.dropna(subset=["account_org"])["account_org"].unique().size
        self.variables["unsettled_busLoan_agency_number"] = count

    # 未结清消费性贷款机构数
    def _unsettled_consume_agency_cnt(self):
        '''
        1.在贷机构中，同一机构有多笔，业务种类中含经营性和消费性，此机构算作经营性机构，不计入消费性机构
        2.在贷机构中，同一机构有多笔，业务种类中仅含消费性，借款金额合计>=20万且余额合计>（借款金额合计*20%），此机构算作经营性机构，不计入消费性机构
        3.在贷机构中，同一机构有多笔，业务种类中仅含消费性，借款金额合计<20万 或 借款金额合计>=20万且余额合计<=（借款金额合计*20%），此机构算作消费性机构
        '''
        df = self.cached_data['pcredit_loan']
        loan_df = df[
            (pd.notnull(df['loan_amount'])) & (df['account_type'].isin(['01', '02', '03'])) & (
                        df['loan_balance'] > 0)]
        if loan_df is None or loan_df.shape[0] == 0:
            return 0
        # 在贷经营性机构
        bus_loan_df = loan_df[(loan_df['account_type'].isin(['01', '02', '03'])) &
                              (
                                  ((loan_df['loan_type'].isin(['01', '07', '99', '15', '16', '22'])) | (
                                      loan_df['loan_type'].str.contains('融资租赁')) |
                                   ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] >= 200000)))
                              )]
        bus_account_org = bus_loan_df["account_org"].unique()
        # 按照机构合并借款金额、余额，排除借款金额合计>=20万且余额合计>（借款金额合计*20%）的机构
        consume_loan_df = loan_df[(loan_df['loan_type'] == '04') & (
                loan_df['loan_balance'] > 0) & (loan_df['loan_amount'] < 200000)]
        group_df = consume_loan_df.groupby(["account_org"]).agg({"loan_amount": "sum", "loan_balance": "sum"})
        group_df = group_df.reset_index()
        ex_bus_account_org = \
            group_df[
                (group_df['loan_amount'] >= 200000) & (group_df["loan_balance"] > group_df["loan_amount"] * 0.2)][
                "account_org"].to_list()
        org_cnt = len(set(consume_loan_df['account_org']) - set(bus_account_org) - set(ex_bus_account_org))
        self.variables["unsettled_consume_agency_cnt"] = org_cnt

    # 未销户贷记卡发卡机构数
    def _uncancelled_credit_organization_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05且loan_status不等于07的account_org
        # 2.统计1中不同account_org的数目
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type in ["04", "05"] and loan_status != "07"')
        org_list = list(set(credit_loan_df.dropna(subset=["account_org"])['account_org'].to_list()))
        self.variables["uncancelled_credit_organization_number"] = len(org_list)

    # 未结清经营性贷款笔笔数
    def _unsettled_busLoan_total_cnt(self):
        # count(pcredit_loan中report_id=report_id且account_type=01,02,03且(loan_type=01,07,99,15,16，22或者包含融资租赁或者(loan_type=04且loan_amount>200000))且loan_balance>0的记录)
        df = self.cached_data["pcredit_loan"]

        df = df[(df['account_type'].isin(['01', '02', '03'])) &
                                        (
                                                (df['loan_type'].isin(['01', '07', '99','15','16','22'])) |
                                                (df['loan_type'].str.contains('融资租赁'))|
                                                ((df['loan_type'] == '04') & (
                                                        df['loan_amount'] >= 200000))
                                        ) &
                                        (df['loan_balance'] > 0)]

        self.variables["unsettled_busLoan_total_cnt"] = df.shape[0]

    # 未结清贷款机构数
    def _unsettled_loan_agency_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_balance>0的account_org
        # 2.统计1中不同account_org数目
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_balance > 0')
        count = credit_loan_df.dropna(subset=["account_org"])["account_org"].unique().size
        self.variables["unsettled_loan_agency_number"] = count

    # 未结清消费性贷款总额
    def _unsettled_consume_total_amount(self):
        # "1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=04且loan_amount<200000且loan_balance>0的loan_amount
        # 2.将1中所有loan_amount加总"
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                              'and loan_type == "04" and loan_amount<200000 and loan_balance>0')

        amt = credit_loan_df["loan_amount"].sum()
        self.variables["unsettled_consume_total_amount"] = amt

    # 未结清贷款笔数
    def _unsettled_loan_number(self):
        # count(pcredit_loan中report_id=report_id且account_type=01,02,03且loan_balance>0的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"] and loan_balance > 0')
        self.variables["unsettled_loan_number"] = loan_df.shape[0]

    # 未结清房贷笔数
    def _unsettled_house_loan_number(self):
        # count(pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=05,06且loan_balance>0的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"] and loan_type in ["05", "06"] and loan_balance > 0')
        self.variables["unsettled_house_loan_number"] = loan_df.shape[0]

    # 未结清消费性贷款笔数(去除房贷车贷)
    def _unsettled_consume_total_cnt(self):
        # count(pcredit_loan中report_id=report_id且account_type=01,02,03且loan_type=04且principal_amount<200000且loan_balance>0的记录)
        loan_df = self.cached_data["pcredit_loan"]
        loan_df = loan_df.query('account_type in ["01", "02", "03"] and loan_type == "04" '
                                'and loan_amount < 200000 and loan_balance > 0')
        self.variables["unsettled_consume_total_cnt"] = loan_df.shape[0]
