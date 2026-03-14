import pandas as pd
from mapping.tranformer import Transformer
import time
from datetime import datetime


def trans_float_time_to_datetime(order_time):
    b = time.localtime(order_time)
    c = time.strftime("%Y-%m-%d %H:%M:%S", b)
    dt = datetime.strptime(c, '%Y-%m-%d %H:%M:%S')
    return dt


def trans_string_time_to_datetime(order_time):
    dt = datetime.strptime(order_time, '%Y-%m-%d %H:%M:%S')
    return dt


class Tp0005(Transformer):
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'med_inst_same_main': 0,  # 医疗执业机构证上法人与负责人同一人
            'operate_years': 0,  # 诊所经营年限
            'trans_mon_num_his': 0,  # 历史交易月份个数
            'legal_age': 0,  # 年龄
            'is_native_place': 0,  # 经营地与法人籍贯是否符合
            'is_customer_reco': 0,  # 存在客户推荐人
            'avg_purchase_num_half_year': 0,  # 近半年月均采购次数
            'segment_name': "loan_amt"
        }

    def basic_info_compare(self):
        extra_param = self.full_msg['strategyParam']['extraParam']
        # 医疗执业机构证上法人与负责人同一人
        ins_licences_legal = extra_param["registerBasicInfo"].get("insLicencesLegal")
        ins_licences_head = extra_param["registerBasicInfo"].get("insLicencesHead")
        if ins_licences_legal is not None and ins_licences_head is not None and ins_licences_legal == ins_licences_head:
            self.variables['med_inst_same_main'] = 1
        # 年龄
        self.variables['legal_age'] = self.origin_data[1].get("legal_age")
        # 存在客户推荐人
        cus_referrer = extra_param["registerBasicInfo"].get("cusReferrer")
        if cus_referrer is not None:
            self.variables['is_customer_reco'] = 1
        # 历史交易月份个数
        historyOrders = extra_param["historyOrders"]
        time_list = []
        half_year_trans_num = 0
        half_year_trans_month_list = []
        if historyOrders is not None and len(historyOrders) > 0:
            for history in historyOrders:
                orderDateTime = history.get("orderDateTime")
                if orderDateTime is not None and orderDateTime != "":
                    trans_date = trans_string_time_to_datetime(orderDateTime)
                    year = trans_date.year
                    month = trans_date.month
                    time_list.append(str(year) + "-" + str(month))
                    day_diff = (pd.datetime.now() - trans_date).days
                    if day_diff <= 180:
                        half_year_trans_num = half_year_trans_num + 1
                        half_year_trans_month_list.append(month)
            self.variables['trans_mon_num_his'] = len(list(set(time_list)))
        # 经营地与法人籍贯是否符合
        optAddr = extra_param["registerBasicInfo"].get("optAddr")
        legal_native_place = self.origin_data[1].get("legal_native_place")
        if legal_native_place is not None and len(legal_native_place) > 0 and legal_native_place in optAddr:
            self.variables['is_native_place'] = 1
        # 近半年月均采购次数
        if len(list(set(half_year_trans_month_list))) >= 4:
            self.variables['avg_purchase_num_half_year'] = round(half_year_trans_num / 6, 2)
        # 经营年限
        self.variables['operate_years'] = self.origin_data[0].get("operator_years")

    def transform(self):
        self.basic_info_compare()
