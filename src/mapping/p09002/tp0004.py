from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def get_diff_day(apply_date):
    current_time = pd.datetime.now()
    diff_day = (current_time - apply_date).days
    return diff_day


class Tp0004(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'account_cnt_same_imei_1day': 0,  # 一天内同一设备登录账号数量
            'idno_cnt_same_imei_1week': 0,  # 一周内同一设备关联的身份证号数
            'phone_cnt_same_imei_1week': 0,  # 一周内同一设备关联的手机号数
            'idno_cnt_same_imei_1month': 0,  # 一个月内同一设备关联的身份证数
            'phone_cnt_same_imei_1month': 0,  # 一个月内同一设备关联的手机号数
            'app_cnt_same_idno_1day': 0,  # 一天内同一身份证申请贷款数
            'phone_cnt_same_idno_2month': 0,  # 60天内同一身份证号对应手机号数
            'idno_cnt_same_phone_2month': 0,  # 60天内同一手机号对应身份证号数
            'app_cnt_same_idno_1week': 0,  # 一周内同一身份证申请贷款数
            'app_cnt_same_imei_1day': 0,  # 一天内同一设备申请贷款数
            'app_cnt_same_ip_1day': 0,  # 一天内同一IP申请贷款数
            'app_cnt_same_imei_1week': 0,  # 一周内同一设备申请贷款数
            'app_cnt_same_ip_1week': 0,  # 一周内同一IP申请贷款数
            'imei_cnt_same_idno_2month': 0,  # 60天内同一身份证号对应设备数
            'lbs_delta_app_same_imei_1hour': None,  # 同一设备一小时内两次申请登录距离
            'lbs_delta_app_same_phone_1hour': None,  # 同一手机号一小时内两次申请登录距离
            'lbs_delta_pay_same_imei_1hour': None,  # 同一设备一小时内两次支付距离
            'lbs_delta_pay_same_phone_1hour': None,  # 同一手机号一小时内两次支付距离
            'new_imei_pay': 0,  # 登录新设备支付
            'account_cnt_new_imei_24hour': 0,  # 24小时内该设备(授信+支付)账号
            'ip_cnt_new_imei_24hour': 0,  # 24小时内该设备(授信+支付)IP
            'midnight_pay': 0,  # 凌晨支付
            'new_imei_pay_today': 0,  # 当天使用新设备支付
            'pay_cnt_today': 0,  # 当天支付次数
            'base_black': 0,
            'stageIndex': 1

        }

    def transform(self):

        extra_param = self.full_msg['strategyParam']['extraParam']

        self.cached_data["clinic_id"] = extra_param["registerBasicInfo"].get("clinicNo")
        self.cached_data["id_no"] = extra_param["personalBasicInfo"].get("legalIdNo")
        self.cached_data["phone_no"] = extra_param["personalBasicInfo"].get("legalPhone")
        self.cached_data["IMEI"] = extra_param["stageBasicInfo"].get("imei")
        self.cached_data["LBS"] = extra_param["stageBasicInfo"].get("gps")
        self.cached_data["IP"] = extra_param["stageBasicInfo"].get("ip")

        self._base_black()

        sql = """
            select *
            from gamt_apply_device_info
            where clinic_id = %(clinic_id)s
            or imei = %(imei)s
            or id_no = %(id_no)s
            or phone_no = %(phone_no)s
            or ip = %(ip)s
        """

        # 设备信息包含 所有授信+支付
        device_info = sql_to_df(sql,
                                params={
                                    "clinic_id": self.cached_data.get("clinic_id"),
                                    "imei": self.cached_data.get("imei"),
                                    "id_no": self.cached_data.get('id_no'),
                                    "phone_no": self.cached_data.get("phone_no"),
                                    "ip": self.cached_data.get("ip")
                                })

        if not device_info.empty:
            device_info['apply_date'] = pd.to_datetime(device_info['apply_date'])
            device_info['diff_day'] = device_info.apply(lambda x: get_diff_day(x['apply_date']), axis=1)

            if not device_info[device_info.stage_index == 1].empty:
                self.anti_fraud_var_clean(device_info[device_info.stage_index == 1])

                self.app_location_rule(device_info[device_info.stage_index == 1])

            # if self.origin_data["stageIndex"] == 1:
            #     self.app_location_rule(device_info[device_info.stage_index == 1])

            # if self.origin_data["stageIndex"] == 2:
            #     self.pay_stolen_rule(device_info, extra_param)

    def _base_black(self):
        sql = """
        SELECT count(1) as "base_black" FROM info_black_list
            WHERE valid > 0 AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        if df is not None and len(df) > 0:
            if df['base_black'][0] > 0:
                self.variables['base_black'] = 1

    def anti_fraud_var_clean(self, apply_device_info):
        """
            授信阶段/支付阶段 共用的策略变量
        """
        current_time = pd.datetime.now()
        self.variables["account_cnt_same_imei_1day"] = \
            apply_device_info[(apply_device_info.diff_day <= 1)
                              & (apply_device_info.imei == self.cached_data.get("imei"))]['clinic_id'].nunique()

        self.variables["idno_cnt_same_imei_1week"] = \
            apply_device_info[(apply_device_info.diff_day <= 7)
                              & (apply_device_info.imei == self.cached_data.get("imei"))]['id_no'].nunique()

        self.variables["phone_cnt_same_imei_1week"] = \
            apply_device_info[(apply_device_info.diff_day <= 7)
                              & (apply_device_info.imei == self.cached_data.get("imei"))]['phone_no'].nunique()

        self.variables["idno_cnt_same_imei_1month"] = \
            apply_device_info[(apply_device_info.diff_day <= 30)
                              & (apply_device_info.imei == self.cached_data.get("imei"))]['id_no'].nunique()

        self.variables["phone_cnt_same_imei_1month"] = \
            apply_device_info[(apply_device_info.diff_day <= 30)
                              & (apply_device_info.imei == self.cached_data.get("imei"))]['phone_no'].nunique()

        self.variables["app_cnt_same_idno_1day"] = \
            apply_device_info[(apply_device_info.diff_day <= 1)
                              & (apply_device_info.id_no == self.cached_data.get("id_no"))].shape[0]

        self.variables["phone_cnt_same_idno_2month"] = \
            apply_device_info[(apply_device_info.diff_day <= 60)
                              & (apply_device_info.id_no == self.cached_data.get("id_no"))]['phone_no'].nunique()

        self.variables["idno_cnt_same_phone_2month"] = \
            apply_device_info[(apply_device_info.diff_day <= 60)
                              & (apply_device_info.phone_no == self.cached_data.get("phone_no"))]['id_no'].nunique()

        self.variables["app_cnt_same_idno_1week"] = \
            apply_device_info[(apply_device_info.diff_day <= 7)
                              & (apply_device_info.id_no == self.cached_data.get("id_no"))].shape[0]

        self.variables["app_cnt_same_imei_1day"] = \
            apply_device_info[(apply_device_info.diff_day <= 1)
                              & (apply_device_info.imei == self.cached_data.get("imei"))].shape[0]

        self.variables["app_cnt_same_ip_1day"] = \
            apply_device_info[(apply_device_info.diff_day <= 1)
                              & (apply_device_info.ip == self.cached_data.get("ip"))].shape[0]

        self.variables["app_cnt_same_imei_1week"] = \
            apply_device_info[(apply_device_info.diff_day <= 7)
                              & (apply_device_info.imei == self.cached_data.get("imei"))].shape[0]

        self.variables["app_cnt_same_ip_1week"] = \
            apply_device_info[(apply_device_info.diff_day <= 7)
                              & (apply_device_info.ip == self.cached_data.get("ip"))].shape[0]

        self.variables["imei_cnt_same_idno_2month"] = \
            apply_device_info[(apply_device_info.diff_day <= 60)
                              & (apply_device_info.id_no == self.cached_data.get("id_no"))]['imei'].nunique()

    def app_location_rule(self, apply_device_info):
        """
            授信阶段调用，地理位置相关信息
        """
        return

    def pay_stolen_rule(self, device_info, extra_param):
        """
            支付阶段调用，账号失窃相关信息
        """
        report_req_no = self.origin_data['preReportReqNo']
        pay_device_info = device_info[device_info.stage_index == 2]

        same_acc_pay_hist = pay_device_info[(pay_device_info.clinic_id == self.cached_data.get("clinic_id"))
                                            & (pay_device_info.report_req_no != report_req_no)]
        if self.cached_data["clinic_id"] not in same_acc_pay_hist.clinic_id.tolist() and not same_acc_pay_hist.empty:
            self.variables["new_imei_pay"] = 1

        same_acc_pay_hist_2 = pay_device_info[(pay_device_info.clinic_id == self.cached_data.get("clinic_id"))
                                              & (pay_device_info.apply_date < pd.datetime.today())]
        if self.cached_data[
            "clinic_id"] not in same_acc_pay_hist_2.clinic_id.tolist() and not same_acc_pay_hist_2.empty:
            self.variables["new_imei_pay_cnt_today"] = 1

        self.variables["pay_cnt_today"] = pay_device_info[(pay_device_info.apply_date >= pd.datetime.today())
                                                          & (pay_device_info.clinic_id == self.cached_data.get(
            "clinic_id"))].shape[0]

        same_imei_dev_info_24h = device_info[(device_info.imei == self.cached_data.get("imei"))
                                             & (device_info.apply_date >= pd.datetime.now() - pd.Timedelta(hours=24))]
        if not same_imei_dev_info_24h.empty:
            self.variables['account_cnt_new_imei_24hour'] = same_imei_dev_info_24h.clinic_id.nunique()
            self.variables['ip_cnt_new_imei_24hour'] = same_imei_dev_info_24h.ip.nunique()

        pay_time = pd.to_datetime(extra_param["stageBasicInfo"].get("optDateTime"))
        if 1 <= pay_time.hour <= 5:
            self.variables["midnight_pay"] = 1
