# @Time : 2020/4/28 3:08 PM 
# @Author : lixiaobo
# @File : single_info_processor.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# single开头的相关的变量
from product.date_time_util import after_ref_date
import pandas as pd
import copy


class SingleInfoProcessor(ModuleProcessor):
    def process(self):
        self._single_house_overdue_2year_cnt()
        self._single_car_overdue_2year_cnt()
        self._single_consume_overdue_2year_cnt()
        self._single_credit_overdue_cnt_2y()
        self._single_house_loan_overdue_cnt_2y()
        self._single_car_loan_overdue_cnt_2y()
        self._single_consume_loan_overdue_cnt_2y()
        self._single_credit_or_loan_3year_overdue_max_month()
        self._single_credit_overdue_2year_cnt()
        self._single_loan_overdue_2year_cnt()
        self._single_bus_loan_overdue_2year_cnt()  # 单笔经营性贷款近2年内最大逾期次数
        self._single_consume_loan_overdue_2year_cnt()  # 单笔消费性贷款近2年内最大逾期次数
        self._single_semi_credit_card_3year_overdue_max_month()  # 准贷记卡逾期最大连续期数
        self._single_semi_credit_overdue_2year_cnt()  # 单张准贷记卡近2年内最大逾期次数

    # 单笔房贷近2年内最大逾期次数
    def _single_house_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=03,05,06的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type in ["03", "05", "06"]')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 1000:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["single_house_overdue_2year_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 单笔车贷近2年内最大逾期次数
    def _single_car_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type == "02"')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 1000:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["single_car_overdue_2year_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 单笔消费性贷款近2年内最大逾期次数
    def _single_consume_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=04且loan_amount<200000的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] and loan_type == "04" '
                                              'and loan_amount < 200000')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        count = 0
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 1000:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        count = count + 1
        self.variables["single_consume_overdue_2year_cnt"] = count

    # 单张贷记卡2年内逾期次数
    def _single_credit_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04,05的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["04", "05"], var_name="single_credit_overdue_cnt_2y")

    # 单笔房贷2年内逾期次数
    def _single_house_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=03,05,06的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["03", "05", "06"],
                               var_name="single_house_loan_overdue_cnt_2y")

    # 单笔车贷2年内逾期次数
    def _single_car_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=02的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["02"],
                               var_name="single_car_loan_overdue_cnt_2y")

    # 单笔消费贷2年内逾期次数
    def _single_consume_loan_overdue_cnt_2y(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03且loan_type=04的id;
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录);
        # 3.从2中所有结果中选取最大值
        self._max_overdue_cacl(account_type=["01", "02", "03"], loan_type=["04"],
                               loan_amount=200000, var_name="single_consume_loan_overdue_cnt_2y")

    def _max_overdue_cacl(self, account_type=None, loan_type=None, loan_amount=None, var_name=None, within_year=2):
        if not account_type and not loan_type:
            return

        loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if loan_df.empty or repayment_df.empty:
            return

        if account_type and loan_type:
            loan_df = loan_df.query('account_type in ' + str(account_type) + 'and loan_type in ' + str(loan_type))
        elif account_type:
            loan_df = loan_df.query('account_type in ' + str(account_type))
        elif loan_type:
            loan_df = loan_df.query('loan_type in ' + str(loan_type))

        if loan_amount:
            loan_df = loan_df[loan_df['loan_amount'] <= 20000]

        if loan_df.empty:
            return

        # repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)) +
        # ' and (repayment_amt > 0 or status.str.isdigit())')
        report_time = self.cached_data["report_time"]
        repayment_df = repayment_df[(repayment_df['jhi_year'] > report_time.year - within_year) &
                                    (repayment_df['month'] >= report_time.month) &
                                    (repayment_df['record_id'].isin(list(loan_df['id']))) &
                                    (repayment_df['repayment_amt'] > 1000) &
                                    (repayment_df['status'].str.isdigit())
                                    ]
        if len(repayment_df) > 0:
            self.variables[var_name] = repayment_df.groupby(by='record_id').size().max()
        # if not repayment_df.empty:
        #     report_time = self.cached_data["report_time"]
        #     status_list = []
        #     for index, row in repayment_df.iterrows():
        #         if row["status"] and row["status"].isdigit():
        #             if after_ref_date(row.jhi_year, row.month, report_time.year - within_year, report_time.month):
        #                 status_list.append(int(row["status"]))
        #     self.variables[var_name] = 0 if len(status_list) == 0 else max(status_list)

    # 单张贷记卡（信用卡）、单笔贷款3年内出现连续90天以上逾期记录（年费及手续费等逾期金额在1000元下的除外）
    def _single_credit_or_loan_3year_overdue_max_month(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04的id
        # 2.对每一个id,max(pcredit_payment中record_id=id且repayment_amt>1000且应还年份、月份在报告申请日期3年内且status是数字的status)
        # 3.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的id
        # 4.对步骤3中每一个id,max(pcredit_payment中record_id=id且应还年份、月份在报告申请日期3年内且status是数字的status)
        # 5.从步骤2和步骤4中所有结果中选取最大值"
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = copy.deepcopy(self.cached_data.get("pcredit_repayment"))
        report_time = self.cached_data['report_time']
        # 筛选近3年还款记录
        for index, row in repayment_df.iterrows():
            if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month) == False:
                repayment_df.drop(index, axis=0, inplace=True)
        credit_overdue_max_month = 0
        if credit_loan_df.empty or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04"]')
        credit_repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        credit_repayment_df = credit_repayment_df[credit_repayment_df['repayment_amt'] > 1000]

        if not credit_repayment_df.empty:
            status_list = []
            for index, row in credit_repayment_df.iterrows():
                if row["status"] and row["status"].isdigit():
                    status_list.append(int(row["status"]))
            credit_overdue_max_month = 0 if len(status_list) == 0 else max(status_list)

        loan_overdue_max_month = 0
        loan_df = credit_loan_df[credit_loan_df['account_type'].isin(['01', '02', '03'])]
        loan_repayment_df = repayment_df.query('record_id in ' + str(list(loan_df.id)))
        if loan_repayment_df.empty:
            loan_overdue_max_month = 0
        if not loan_repayment_df.empty:
            status_list = []
            for index, row in loan_repayment_df.iterrows():
                if pd.notna(row["status"]) and row["status"].isdigit() and \
                        pd.notna(row['repayment_amt']) and row['repayment_amt'] > 0:
                    status_list.append(int(row["status"]))
            loan_overdue_max_month = 0 if len(status_list) == 0 else max(status_list)
        self.variables["single_credit_or_loan_3year_overdue_max_month"] = max(credit_overdue_max_month,
                                                                              loan_overdue_max_month)

    # 单张贷记卡（信用卡）近2年内存在5次以上逾期（年费及手续费等逾期金额在1000元下的除外）
    def _single_credit_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=04且loan_type=11,12的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且还款时间在report_time两年内且逾期金额>1000的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["04"]')
        if credit_loan_df.empty:
            return
        if not credit_loan_df.empty:
            report_time = self.cached_data["report_time"]
            status_cnt = []
            id_list = list(credit_loan_df.id)
            for i in id_list:
                temp_df = repayment_df[(repayment_df['record_id'] == i) & (repayment_df['repayment_amt'] > 1000)]
                cnt = 0
                for index, row in temp_df.iterrows():
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        cnt += 1
                status_cnt.append(cnt)
            self.variables["single_credit_overdue_2year_cnt"] = 0 if len(status_cnt) == 0 else max(status_cnt)

    # 单笔贷款近2年内最大逾期次数
    def _single_loan_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的id
        # 2.对每一个id,count(pcredit_payment中record_id=id且status是数字且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"]')
        if credit_loan_df.empty:
            return

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        if repayment_df is not None and not repayment_df.empty:
            report_time = self.cached_data["report_time"]
            status_list = []
            for index, row in repayment_df.iterrows():
                if row["status"] and row["status"].isdigit() and row['repayment_amt'] and row['repayment_amt'] > 0:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["single_loan_overdue_2year_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 单笔经营性贷款近2年内最大逾期次数
    def _single_bus_loan_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03(01非循环贷账户，02循环额度下分账户，03循环贷账户)
        # 且(loan_type=01,07,99,15,16，22或者包含融资租赁（01个人经营性贷款，07农户贷款，99其他贷款）或者(loan_type=04（04个人消费贷款）且principal_amount>=200000))的id
        # 2.对每一个id, count(pcredit_payment中record_id=id且(status是数字或者repayment_amt>0)且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data["pcredit_repayment"]
        if credit_loan_df.empty or repayment_df.empty:
            return
        credit_loan_df = credit_loan_df[(credit_loan_df['account_type'].isin(['01', '02', '03'])) &
                                        (
                                            ((credit_loan_df['loan_type'].isin(
                                                ['01', '07', '99', '15', '16', '22'])) | (
                                                 credit_loan_df['loan_type'].str.contains('融资租赁')) |
                                             ((credit_loan_df['loan_type'] == '04') & (
                                                     credit_loan_df['loan_amount'] >= 200000)))
                                        )
                                        ]
        if credit_loan_df.empty:
            return

        report_time = self.cached_data["report_time"]
        status_cnt = []
        for i in credit_loan_df.id:
            temp_df = repayment_df[(repayment_df['record_id'] == i) & ((repayment_df['repayment_amt'] > 0) | (
                repayment_df['status'].str.isdigit()))]
            cnt = 0
            for index, row in temp_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    cnt += 1
            status_cnt.append(cnt)
        self.variables["single_bus_loan_overdue_2year_cnt"] = 0 if len(status_cnt) == 0 else max(status_cnt)

    # 单笔消费性贷款近2年内最大逾期次数
    def _single_consume_loan_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03(01非循环贷账户，02循环额度下分账户，03循环贷账户)
        # 且(loan_type=01,07,99（01个人经营性贷款，07农户贷款，99其他贷款）或者(loan_type=04（04个人消费贷款）且principal_amount>=200000))的id
        # 2.对每一个id, count(pcredit_payment中record_id=id且(status是数字或者repayment>0)且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        repayment_df = self.cached_data.get("pcredit_repayment")
        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return
        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                              'and (loan_type in ["02", "03", "05", "06"]'
                                              ' or (loan_type == "04" and loan_amount < 200000))')
        if credit_loan_df.empty:
            return
        report_time = self.cached_data["report_time"]
        status_cnt = []
        for i in credit_loan_df.id:
            temp_df = repayment_df[(repayment_df['record_id'] == i) & ((repayment_df['repayment_amt'] > 0) | (
                repayment_df['status'].str.isdigit()))]
            cnt = 0
            for index, row in temp_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    cnt += 1
            status_cnt.append(cnt)
        self.variables["single_consume_loan_overdue_2year_cnt"] = 0 if len(status_cnt) == 0 else max(status_cnt)

    # 3年内准贷记卡逾期最大连续期数
    def _single_semi_credit_card_3year_overdue_max_month(self):
        # 1.从pcredit_loan中选取所有report_id = report_id且account_type = 05的id，根据id在pcredit_repayment中筛选记录，取repayment_amt > 1000的记录，
        # 2.返回status的最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df[credit_loan_df['account_type'] == '05']
        repayment_df = self.cached_data.get("pcredit_repayment")
        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        repayment_df = repayment_df[(repayment_df['repayment_amt'] > 1000) ]
        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return
        report_time = self.cached_data["report_time"]
        max_month = 0
        if not repayment_df.empty:
            for index, row in repayment_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month):
                    if row['status'].isdigit():
                        max_month = max(max_month, int(row['status']))
        self.variables['single_semi_credit_card_3year_overdue_max_month'] = max_month

    # 单张准贷记卡近2年内最大逾期次数
    def _single_semi_credit_overdue_2year_cnt(self):
        # 1.从pcredit_loan中选取所有report_id = report_id且account_type = 05的id
        # 2.对每一个id, count(pcredit_payment中record_id=id且status > 2且repayment_amt > 1000且还款时间在report_time两年内的记录)
        # 3.从2中所有结果中选取最大值
        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_loan_df = credit_loan_df[credit_loan_df['account_type'] == '05']
        repayment_df = self.cached_data.get("pcredit_repayment")
        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        repayment_df = repayment_df[(repayment_df['repayment_amt'] > 1000) ]
        if credit_loan_df is None or credit_loan_df.empty or repayment_df is None or repayment_df.empty:
            return
        report_time = self.cached_data["report_time"]
        status_cnt = []
        for i in credit_loan_df.id:
            temp_df = repayment_df[(repayment_df['record_id'] == i) & ((repayment_df['repayment_amt'] > 0) | (
                repayment_df['status'].str.isdigit()))]
            cnt = 0
            for index, row in temp_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                    if int(row['status'])>2:
                        cnt += 1
            status_cnt.append(cnt)
        self.variables["single_semi_credit_overdue_2year_cnt"] = 0 if len(status_cnt) == 0 else max(
            status_cnt)
