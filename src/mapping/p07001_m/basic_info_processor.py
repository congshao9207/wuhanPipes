# @Time : 2020/4/23 7:06 PM 
# @Author : lixiaobo
# @File : basic_info_handle.py.py 
# @Software: PyCharm
import pandas as pd

from mapping.module_processor import ModuleProcessor

# 基本信息处理
from product.date_time_util import after_ref_date
from util.id_card_info import GetInformation
import copy

def get_repay_period_temp(loan_amount, repay_period, loan_date, loan_end_date):
    if pd.isnull(repay_period) or pd.isna(repay_period):
        year_start = loan_date.year
        month_start = loan_date.month
        day_start = loan_date.day
        year_end = loan_end_date.year
        month_end = loan_end_date.month
        day_end = loan_end_date.day
        if day_start >= day_end:
            month_diff = (year_end - year_start) * 12 + month_end - month_start
        else:
            month_diff = (year_end - year_start) * 12 + month_end - month_start
        return loan_amount / month_diff
    else:
        return loan_amount / repay_period


class BasicInfoProcessor(ModuleProcessor):

    def process(self):
        # print("BasicInfoProcessor process")
        self.variables["report_id"] = self.cached_data["report_id"]

        self._rhzx_business_loan_3year_overdue_cnt()  # 3年内经营性贷款逾期笔数
        self._public_sum_count()
        self._business_loan_average_3year_overdue_cnt()  # 3年内还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数
        self._large_loan_2year_overdue_cnt()
        self._divorce_50_female()
        self._extension_number()
        self._enforce_record()
        self._marriage_status()
        self._credit_marriage_status()
        self._judgement_record()
        self._guarantee_amont()
        self._tax_record()
        self._ad_penalty_record()
        self._business_loan_overdue_money()
        self._no_loan()  # 名下无贷款无贷记卡
        self._house_loan_pre_settle()  # 存在房贷提前结清
        self._guar_2times_apply()  # 担保金额是借款金额2倍
        self._all_house_car_loan_reg_cnt()  # 所有房屋汽车贷款机构数
        self._rhzx_business_loan_3year_ago_overdue_cnt()  # 3年前经营性贷款逾期笔数
        self._business_loan_average_3year_ago_overdue_cnt()  # 3年前还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数

    # 3年内经营性贷款逾期笔数
    def _rhzx_business_loan_3year_overdue_cnt(self):
        loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")
        if loan_df is None or loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        loan_df = loan_df[(pd.notnull(loan_df['loan_amount'])) &
                          (loan_df['account_type'].isin(['01', '02', '03'])) &
                          (
                                  (loan_df['loan_type'].isin(['01', '07', '99','15','16','22']) ) |
                                  (loan_df['loan_type'].str.contains('融资租赁'))|
                                  ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] >= 200000))
                          )
                          ]

        if loan_df.empty:
            return
        report_time = self.cached_data['report_time']
        count = 0
        for item_df in loan_df.itertuples():
            # 借款金额=0，担保方式为抵质押、组合的，逾期金额>=5w作为本金逾期；借款金额=0，担保方式非抵质押、非组合的，逾期金额>=2w作为本金逾期
            if item_df.loan_amount == 0 and item_df.loan_guarantee_type in ('1', '2', '5', '6'):
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                # 筛选还款记录表中的年份、月份在报告查询日期3年内的记录
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3,
                                          report_time.month) and row.repayment_amt >= 50000:
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
            elif item_df.loan_amount == 0 and item_df.loan_guarantee_type in ('3', '4', '7', '99'):
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                # 筛选还款记录表中的年份、月份在报告查询日期3年内的记录
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3,
                                          report_time.month) and row.repayment_amt >= 20000:
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
            else:
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                # 筛选还款记录表中的年份、月份在报告查询日期3年内的记录
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month):
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
        self.variables["rhzx_business_loan_3year_overdue_cnt"] = count

    # 呆账、资产处置、保证人代偿笔数
    def _public_sum_count(self):
        # 1.从pcredit_default_info(信贷交易违约信息概要)选择所有report_id = report_id且((default_type=01且default_subtype=0103)或者default_type = 02)的default_count加总
        # 2.求count(pcredit_special（特殊交易）中record_id=id且special_type = 2（担保人（第三方）代偿）的记录),
        # 3.将步骤1和步骤2的数量相加
        default_info_df = self.cached_data.get("pcredit_default_info")
        special_df = self.cached_data.get("pcredit_special")
        special_df = special_df[special_df['special_type'] == '2']

        default_info_df = default_info_df.query('(default_type == "01" and default_subtype == "0103") or default_type == "02"')
        public_sum_cnt =default_info_df.shape[0]+special_df.shape[0]
        self.variables["public_sum_count"] = public_sum_cnt

    # 3年内还款方式为等额本息分期偿还的经营性贷款最大连续逾期期数
    def _business_loan_average_3year_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99，15,16，22或者包含融资租赁 或者(loan_type=04且loan_amount>=200000))且(repay_period>0或者(loan_date非空且loan_end_date非空))且loan_amount非空的记录,
        # 2.对于每一条记录,计算loan_amount/repay_period(若repay_period为空则计算loan_end_date和loan_date之间间隔的月份数当做repay_period,间隔月份数对月对日且向上取整)
        # 3.对于2中每一个id,从pcredit_repayment中选取所有record_id=id且repayment_amt>2中对应的loan_amount/repay_period且repayment_amt<loan_amount/3的记录
        # 4.取3中每一个id对应的记录中最大连续月份数,逾期状态递增或者金额倍增视为连续,即要么逾期状态为1,2。。。，要么连续两个1，但金额倍增
        # 5.取4中所有最大连续月份的最大值
        df = self.cached_data["pcredit_loan"]
        overdue_df = self.cached_data["pcredit_repayment"]
        loan_df = df[((
                      (pd.notna(df['repay_period']) & df['repay_period'] > 0) |
                      (
                          (pd.notna(df['loan_date'])) & (pd.notna(df['loan_end_date']))
                       )
                      ) & (pd.notna(df['loan_amount']))) &
                     (df['account_type'].isin(['01', '02', '03'])) &
                     ((df['loan_type'].isin(['01', '07', '99','15','16','22'])) |(df['loan_type'].str.contains('融资租赁'))|
                      ((df['loan_type'] == '04') &
                       (df['loan_amount'] >= 200000)))]
        if loan_df.shape[0] == 0:
            self.variables["business_loan_average_3year_overdue_cnt"] = 0
            return
        loan_df['loan_date'] = loan_df['loan_date']
        loan_df['loan_end_date'] = loan_df['loan_end_date']
        loan_df['repay_period'] = loan_df.apply(
            lambda x: x['repay_period'] if pd.notna(x['repay_period']) else
            (x['loan_end_date'].year - x['loan_date'].year) * 12 + x['loan_end_date'].month - x['loan_date'].month
            + (x['loan_end_date'].day - x['loan_date'].day - 1) // 100 + 1, axis=1)
        loan_df['avg_loan_amount'] = loan_df.apply(
            lambda x: x['loan_amount'] / x['repay_period'] if pd.notna(x['repay_period']) else None, axis=1)
        loan_overdue_df = copy.deepcopy(overdue_df[overdue_df['record_id'].isin(list(set(loan_df['id'].tolist())))])
        loan_overdue_df = pd.merge(loan_overdue_df, loan_df[['id', 'loan_amount', 'avg_loan_amount']], how='left',
                                   left_on='record_id', right_on='id', sort=False)
        # 筛选还款记录表中的年份、月份在报告查询日期3年内的记录
        report_time = self.cached_data["report_time"]
        if not loan_overdue_df.empty:
            for index, row in loan_overdue_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month)==False:
                    loan_overdue_df.drop(index, axis=0,inplace=True)
        temp_overdue_df = loan_overdue_df[(loan_overdue_df['repayment_amt'] > loan_overdue_df['avg_loan_amount']) &
                                          (loan_overdue_df['repayment_amt'] < loan_overdue_df['loan_amount'] / 3)]
        if temp_overdue_df.shape[0] == 0:
            self.variables["business_loan_average_3year_overdue_cnt"] = 0
            return
        temp_overdue_df.sort_values(by=['record_id', 'jhi_year', 'month'], inplace=True)
        temp_overdue_df.reset_index(drop=True, inplace=True)
        temp_overdue_df.loc[0, 'conti_month'] = 1
        if temp_overdue_df.shape[0] > 1:
            last_repayment_year = temp_overdue_df.loc[0, 'jhi_year']
            last_repayment_month = temp_overdue_df.loc[0, 'month']
            last_status = temp_overdue_df.loc[0, 'status']
            last_amt = temp_overdue_df.loc[0, 'repayment_amt']
            last_record_id = temp_overdue_df.loc[0, 'record_id']
            for index in temp_overdue_df.index[1:]:
                this_repayment_year = temp_overdue_df.loc[index, 'jhi_year']
                this_repayment_month = temp_overdue_df.loc[index, 'month']
                this_status = temp_overdue_df.loc[index, 'status']
                this_amt = temp_overdue_df.loc[index, 'repayment_amt']
                this_record_id = temp_overdue_df.loc[index, 'record_id']
                diff_month = (int(this_repayment_year) - int(last_repayment_year)) * 12 + int(this_repayment_month) - \
                             int(last_repayment_month)
                # 此处表示必须满足条件：1.同一笔贷款；2.间隔一个月份；3.逾期状态递增且只增加1或者逾期金额近乎倍增；才被视为连续逾期
                if this_record_id == last_record_id and diff_month == 1 and \
                        (this_amt > last_amt * 1.9 or (last_status.isdigit() and this_status.isdigit() and
                                                       int(this_status) - int(last_status) == 1)):
                    temp_overdue_df.loc[index, 'conti_month'] = temp_overdue_df.loc[index - 1, 'conti_month'] + 1
                else:
                    temp_overdue_df.loc[index, 'conti_month'] = 1
                last_repayment_year = this_repayment_year
                last_repayment_month = this_repayment_month
                last_status = this_status
                last_amt = this_amt
                last_record_id = this_record_id
        business_loan_average_3year_overdue_cnt = temp_overdue_df[temp_overdue_df['conti_month'] == 2].shape[0]

        self.variables["business_loan_average_3year_overdue_cnt"] = business_loan_average_3year_overdue_cnt

    # 经营性贷款（经营性+个人消费大于等于20万+农户+其他）2年内最大连续逾期期数
    def _large_loan_2year_overdue_cnt(self):
        '''
        "1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99,15,16，22或者包含融资租赁或者(loan_type=04且loan_amount>=200000))的id,
        2.对于每一个id,max(pcredit_payment中所有record_id=id且status是数字且还款时间在report_time两年内的status),
        3.从2中所有结果中选取最大的一个"
        '''
        credit_loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")

        if credit_loan_df.empty or repayment_df.empty:
            return

        credit_loan_df = credit_loan_df[(credit_loan_df['account_type'].isin(['01', '02', '03'])) &
                                        ((credit_loan_df['loan_type'].isin(['01', '07', '99','15','16','22'])) | (credit_loan_df['loan_type'].str.contains('融资租赁')) |
                                         ((credit_loan_df['loan_type'] == '04') &
                                          (credit_loan_df['loan_amount'] >= 200000)))
                                        ]

        repayment_df = repayment_df.query('record_id in ' + str(list(credit_loan_df.id)))
        report_time = self.cached_data["report_time"]
        if not repayment_df.empty:
            status_list = []
            for index, row in repayment_df.iterrows():
                if pd.notna(row["status"]) and row["status"].isdigit() and \
                        pd.notna(row['repayment_amt']) and row['repayment_amt'] > 0:
                    if after_ref_date(row.jhi_year, row.month, report_time.year - 2, report_time.month):
                        status_list.append(int(row["status"]))
            self.variables["large_loan_2year_overdue_cnt"] = 0 if len(status_list) == 0 else max(status_list)

    # 年龄>=50,离异或者丧偶，女
    def _divorce_50_female(self):
        # 1.count(pcredit_person_info中report_id=report_id且sex=2且marriage_status=3,4的记录)
        # 2.count(ccs.cus_indiv中cus_name=name且indiv_sex=2且cert_code=certificate_no且marital_status=12,13的记录)
        # 3.从credit_base_info中找到report_id对应的certificate_no,用身份证号第7到14位出生日期算出年龄
        # 4.若(1中count+2中count)>0且3中年龄>=40则变量=1,否则=0
        id_card_no = self.cached_data["id_card_no"]
        information = GetInformation(id_card_no)
        if information.get_sex() != 2:
            return
        if information.get_age() < 50:
            return

        marry_state = self.cached_data.get("basicMarryState")
        if marry_state and marry_state in ["DIVORCE", "WIDOWHOOD"]:
            self.variables["divorce_50_female"] = 1
            return

        credit_person_info = self.cached_data["pcredit_person_info"]
        credit_person_info = credit_person_info[
            (credit_person_info['sex'] == '2') & (credit_person_info['marriage_status'].isin(['3', '4']))]
        self.variables["divorce_50_female"] = 0 if credit_person_info.empty else 1

    # 年龄>55,离异,男
    def _divorce_55_male(self):
        # 1.count(pcredit_person_info中report_id=report_id且sex = 1
        # 且marriage_status = 3, 4的记录)
        # 2.count(ccs.cus_indiv中cus_name = name且indiv_sex = 1
        # 且cert_code = certificate_no且marital_status = 12, 13的记录)
        # 3.从credit_base_info中找到report_id对应的certificate_no, 用身份证号第7到14位出生日期算出年龄
        # 4.若(1中count + 2中count) > 0且3中年龄 > 55则变量 = 1, 否则 = 0
        id_card_no = self.cached_data["id_card_no"]
        information = GetInformation(id_card_no)
        if information.get_sex() != 1:
            return
        if information.get_age() <= 55:
            return

        marry_state = self.cached_data.get("basicMarryState")
        if marry_state and marry_state in ["DIVORCE"]:
            self.variables["divorce_55_male"] = 1
            return

        credit_person_info = self.cached_data["pcredit_person_info"]
        credit_person_info = credit_person_info[
            (credit_person_info['sex'] == '1') & (credit_person_info['marriage_status'] == '3')]
        self.variables["divorce_55_male"] = 0 if credit_person_info.empty else 1

    # 展期笔数
    def _extension_number(self):
        # 1.从pcredit_loan中选取所有report_id=report_id且account_type=01,02,03的id
        # 2.对每一个id,若count(pcredit_special中record_id=id且special_type=1的记录)>0则变量+1

        credit_loan_df = self.cached_data["pcredit_loan"]
        credit_special_df = self.cached_data["pcredit_special"]

        if credit_loan_df.empty or credit_special_df.empty:
            return

        credit_loan_df = credit_loan_df.query('account_type in ["01", "02", "03"]')
        if credit_loan_df.empty:
            return

        count = 0
        for index, row in credit_loan_df.iterrows():
            df = credit_special_df.query('record_id == ' + str(row.id) + ' and special_type == "1"')
            if not df.empty:
                count = count + 1
        self.variables["extension_number"] = count

    # 强制执行记录条数
    def _enforce_record(self):
        # count(pcredit_force_execution_record中report_id=report_id的记录)
        df = self.cached_data["pcredit_force_execution_record"]
        self.variables["enforce_record"] = df.shape[0]

    # 离婚
    def _marriage_status(self):
        # 1.count(pcredit_person_info中report_id=report_id且marriage_status=3的记录)
        # 2.count(ccs.cus_indiv中cus_name=name且cert_code=certificate_no且marital_status=12的记录)
        # 3.若(1中count+2中count)>0则变量=1,否则=0

        marry_state = self.cached_data.get("basicMarryState")
        if marry_state and marry_state == "DIVORCE":
            self.variables["marriage_status"] = 1
            return

        credit_person_df = self.cached_data["pcredit_person_info"]
        count = credit_person_df[credit_person_df['marriage_status']=='3'].shape[0]
        self.variables["marriage_status"] = 1 if count > 0 else 0

    # 获取征信报告中的婚姻状态
    def _credit_marriage_status(self):
        credit_person_df = self.cached_data["pcredit_person_info"]
        if credit_person_df.empty:
            return
        marriage_status = credit_person_df['marriage_status'].tolist()[0]
        if marriage_status is None or pd.isna(marriage_status):
            return
        self.variables["credit_marriage_status"] = marriage_status

    # 民事判决记录数
    def _judgement_record(self):
        # count(pcredit_civil_judgments_record中report_id=report_id的记录)
        judgments_record_df = self.cached_data["pcredit_civil_judgments_record"]
        self.variables["judgement_record"] = judgments_record_df.shape[0]

    # 对外担保金额
    def _guarantee_amont(self):
        # "1.从pcredit_loan中选取所有report_id=report_id且account_type=06的loan_balance（余额）
        # 2.将1中结果加总"
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type == "06"')
        amt = df['loan_balance'].sum()
        self.variables["guarantee_amont"] = amt

    # 欠税记录数
    def _tax_record(self):
        # count(pcredit_credit_tax_record中report_id=report_id的记录)
        df = self.cached_data["pcredit_credit_tax_record"]
        self.variables["tax_record"] = df.shape[0]

    # 行政处罚记录数
    def _ad_penalty_record(self):
        # count(pcredit_punishment_record中report_id=report_id的记录)
        df = self.cached_data["pcredit_punishment_record"]
        self.variables["ad_penalty_record"] = df.shape[0]

    # 经营性贷款逾期金额
    def _business_loan_overdue_money(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99或者包含"融资租赁"或者(loan_type=04且loan_amount>200000))且loan_amount非空的记录,
        # 2.对于每一条记录,计算loan_amount/3
        # 3.对于2中每一个id,max(pcredit_repayment中所有record_id=id且repayment_amt>2中对应的loan_amount/3的repayment_amt)
        # 4.将3中所有结果加总起来
        loan_df = self.cached_data["pcredit_loan"]
        pcredit_repayment = self.cached_data['pcredit_repayment']
        busi_loan = loan_df[(((loan_df['loan_type'].isin(['01', '07', '99'])) |
                              (loan_df['loan_type'].str.contains('融资租赁'))) &
                             (loan_df['loan_amount'] >= 10000)) |
                            ((loan_df['loan_type'] == '04') &
                             (loan_df['loan_amount'] >= 200000)) &
                            (pd.notna(loan_df['loan_amount']))]
        if busi_loan.shape[0] == 0:
            amt = 0
        else:
            busi_loan_overdue = pcredit_repayment[
                pcredit_repayment['record_id'].isin(list(set(busi_loan['id'].to_list())))]
            if busi_loan_overdue.shape[0] == 0:
                amt = 0
            else:
                busi_loan_overdue = pd.merge(busi_loan_overdue, loan_df[['id', 'loan_amount']], how='left',
                                             left_on='record_id', right_on='id', sort=False)
                amt = busi_loan_overdue[
                    busi_loan_overdue['repayment_amt'] > busi_loan_overdue['loan_amount'] / 3
                    ]['repayment_amt'].sum()
        self.variables["business_loan_overdue_money"] = amt

    # 名下无贷款无贷记卡
    def _no_loan(self):
        # count(pcredit_loan中report_id=report_id且account_type=01,02,03,04,05的记录),若结果=0,则变量=1,否则=0s
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_loan_df.query('account_type in ["01", "02", "03", "04", "05"]')
        self.variables["no_loan"] = 1 if df.empty else 0

    # 存在房贷提前结清
    def _house_loan_pre_settle(self):
        # count(pcredit_loan中report_id=report_id且account_type=01,02,03且loan_type=03,05,06且loan_status=04且expire_date<end_date),若结果=0,则变量=1,否则=0
        credit_loan_df = self.cached_data["pcredit_loan"]
        df = credit_loan_df.query('account_type in ["01", "02", "03"] '
                                  'and loan_type in ["03", "05", "06"] '
                                  'and loan_status == "04" '
                                  'and loan_status_time < end_date')
        self.variables["house_loan_pre_settle"] = 1 if not df.empty else 0

    # 担保金额是借款金额2倍
    def _guar_2times_apply(self):
        # 1.从pcredit_loan中选取所有account_type=06的loan_amount;
        # 2.若1中任意结果>入参apply_amt*2,则变量=1,否则=0

        apply_amt = self.origin_data.get("applyAmo")
        if apply_amt is None:
            return

        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type == "06" ')
        amt_serial = df.loc[:, "loan_amount"]
        amt_serial = amt_serial.fillna(0)
        result = filter(lambda x: x > apply_amt * 2, amt_serial.to_list())

        self.variables["guar_2times_apply"] = 1 if len(list(result)) > 0 else 0

    # 所有房屋汽车贷款机构数
    def _all_house_car_loan_reg_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且loan_type=02,03,05,06的account_org
        # 2.统计1中不同的account_org数目
        df = self.cached_data["pcredit_loan"]
        df = df.query('account_type in ["01", "02", "03"] and loan_type in ["02", "03", "05" ," 06"]')
        series = df.loc[:, "account_org"]
        series = series.dropna()
        size = series.unique().size
        self.variables["all_house_car_loan_reg_cnt"] = size

    # 3年前经营性贷款存在本金逾期
    def _rhzx_business_loan_3year_ago_overdue_cnt(self):
        loan_df = self.cached_data.get("pcredit_loan")
        repayment_df = self.cached_data.get("pcredit_repayment")
        if loan_df is None or loan_df.empty or repayment_df is None or repayment_df.empty:
            return

        loan_df = loan_df[(pd.notnull(loan_df['loan_amount'])) &
                          (loan_df['account_type'].isin(['01', '02', '03'])) &
                          (
                                  ((loan_df['loan_type'].isin(['01', '07', '99','15','16','22'])) ) | (loan_df['loan_type'].str.contains('融资租赁')) |
                                  ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] >= 200000))
                          )
                          ]

        if loan_df.empty:
            return

        report_time = self.cached_data['report_time']
        count = 0
        for item_df in loan_df.itertuples():
            # 借款金额=0，担保方式为抵质押、组合的，逾期金额>=5w作为本金逾期；借款金额=0，担保方式非抵质押、非组合的，逾期金额>=2w作为本金逾期
            if item_df.loan_amount == 0 and item_df.loan_guarantee_type in ('1', '2', '5', '6'):
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3,
                                          report_time.month) == False and row.repayment_amt >= 50000:
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
            elif item_df.loan_amount == 0 and item_df.loan_guarantee_type in ('3', '4', '7', '99'):
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3,
                                          report_time.month) == False and row.repayment_amt >= 20000:
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
            else:
                df = repayment_df.query(
                    'record_id == ' + str(item_df.id) + ' and repayment_amt > ' + str(item_df.loan_amount / 3))
                # 筛选还款记录表中的年份、月份在报告查询日期3年前的记录
                if not df.empty:
                    report_id_list = []
                    for index, row in df.iterrows():
                        if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month) == False:
                            report_id_list.append(row["record_id"])
                    if len(report_id_list) > 0:
                        count = count + 1
        self.variables["rhzx_business_loan_3year_ago_overdue_cnt"] = count

    # 3年前还款方式为等额本息分期偿还的经营性贷款连续逾期2期
    def _business_loan_average_3year_ago_overdue_cnt(self):
        # 1.从pcredit_loan中选择所有report_id=report_id且account_type=01,02,03且(loan_type=01,07,99,15,16，22，或者包含融资租赁或者(loan_type=04且loan_amount>=200000))且(repay_period>0或者(loan_date非空且loan_end_date非空))且loan_amount非空的记录,
        # 2.对于每一条记录,计算loan_amount/repay_period(若repay_period为空则计算loan_end_date和loan_date之间间隔的月份数当做repay_period,间隔月份数对月对日且向上取整)
        # 3.对于2中每一个id,从pcredit_repayment中选取所有record_id=id且repayment_amt>2中对应的loan_amount/repay_period且repayment_amt<loan_amount/3的记录
        # 4.取3中每一个id对应的记录中最大连续月份数,逾期状态递增或者金额倍增视为连续,即要么逾期状态为1,2。。。，要么连续两个1，但金额倍增
        # 5.取4中所有最大连续月份的最大值
        df = self.cached_data["pcredit_loan"]
        overdue_df = copy.deepcopy(self.cached_data["pcredit_repayment"])
        # 筛选还款记录表中的年份、月份在报告查询日期3年前的记录
        report_time = self.cached_data["report_time"]
        if not overdue_df.empty:
            for index, row in overdue_df.iterrows():
                if after_ref_date(row.jhi_year, row.month, report_time.year - 3, report_time.month):
                    overdue_df.drop(index, axis=0,inplace=True)
        loan_df = df[(((pd.notna(df['repay_period']) & df['repay_period']>0) |
                      ((pd.notna(df['loan_date'])) & (pd.notna(df['loan_end_date'])))
                       ) ) &  (pd.notna(df['loan_amount'])) &
                     (df['account_type'].isin(['01', '02', '03'])) &
                     ((df['loan_type'].isin(['01', '07', '99','15','16','22'])) | (df['loan_type'].str.contains('融资租赁')) |
                      ((df['loan_type'] == '04') &
                       (df['loan_amount'] >= 200000)))]
        if loan_df.shape[0] == 0:
            self.variables["business_loan_average_3year_ago_overdue_cnt"] = 0
            return
        loan_df['loan_date'] = loan_df['loan_date'].apply(pd.to_datetime)
        loan_df['loan_end_date'] = loan_df['loan_end_date'].apply(pd.to_datetime)
        loan_df['repay_period'] = loan_df.apply(
            lambda x: x['repay_period'] if pd.notna(x['repay_period']) else
            (x['loan_end_date'].year - x['loan_date'].year) * 12 + x['loan_end_date'].month - x['loan_date'].month
            + (x['loan_end_date'].day - x['loan_date'].day - 1) // 100 + 1, axis=1)
        loan_df['avg_loan_amount'] = loan_df.apply(
            lambda x: x['loan_amount'] / x['repay_period'] if pd.notna(x['repay_period']) else None, axis=1)
        loan_overdue_df = overdue_df[overdue_df['record_id'].isin(list(set(loan_df['id'].tolist())))]
        loan_overdue_df = pd.merge(loan_overdue_df, loan_df[['id', 'loan_amount', 'avg_loan_amount']], how='left',
                                   left_on='record_id', right_on='id', sort=False)

        temp_overdue_df = loan_overdue_df[(loan_overdue_df['repayment_amt'] > loan_overdue_df['avg_loan_amount']) &
                                          (loan_overdue_df['repayment_amt'] < loan_overdue_df['loan_amount'] / 3)]
        if temp_overdue_df.shape[0] == 0:
            self.variables["business_loan_average_3year_ago_overdue_cnt"] = 0
            return
        temp_overdue_df.sort_values(by=['record_id', 'jhi_year', 'month'], inplace=True)
        temp_overdue_df.reset_index(drop=True, inplace=True)
        temp_overdue_df.loc[0, 'conti_month'] = 1
        if temp_overdue_df.shape[0] > 1:
            last_repayment_year = temp_overdue_df.loc[0, 'jhi_year']
            last_repayment_month = temp_overdue_df.loc[0, 'month']
            last_status = temp_overdue_df.loc[0, 'status']
            last_amt = temp_overdue_df.loc[0, 'repayment_amt']
            last_record_id = temp_overdue_df.loc[0, 'record_id']
            for index in temp_overdue_df.index[1:]:
                this_repayment_year = temp_overdue_df.loc[index, 'jhi_year']
                this_repayment_month = temp_overdue_df.loc[index, 'month']
                this_status = temp_overdue_df.loc[index, 'status']
                this_amt = temp_overdue_df.loc[index, 'repayment_amt']
                this_record_id = temp_overdue_df.loc[index, 'record_id']
                diff_month = (int(this_repayment_year) - int(last_repayment_year)) * 12 + int(this_repayment_month) - \
                             int(last_repayment_month)
                # 此处表示必须满足条件：1.同一笔贷款；2.间隔一个月份；3.逾期状态递增且只增加1或者逾期金额近乎倍增；才被视为连续逾期
                if this_record_id == last_record_id and diff_month == 1 and \
                        (this_amt > last_amt * 1.9 or (last_status.isdigit() and this_status.isdigit() and
                                                       int(this_status) - int(last_status) == 1)):
                    temp_overdue_df.loc[index, 'conti_month'] = temp_overdue_df.loc[index - 1, 'conti_month'] + 1
                    print('3年前this_record_id：',this_record_id)
                else:
                    temp_overdue_df.loc[index, 'conti_month'] = 1
                last_repayment_year = this_repayment_year
                last_repayment_month = this_repayment_month
                last_status = this_status
                last_amt = this_amt
                last_record_id = this_record_id
        business_loan_average_3year_ago_overdue_cnt = temp_overdue_df[temp_overdue_df['conti_month'] == 2].shape[0]

        self.variables["business_loan_average_3year_ago_overdue_cnt"] = business_loan_average_3year_ago_overdue_cnt
