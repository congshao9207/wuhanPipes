from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class EcTitle(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_title"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "cus_name": "",
            "e_name": "",
            "soci_credict_code": "",
            "ori_report_no": "",
            "ori_report_date": ""
        }

    def transform(self):
        ecredit_base_info = self.cached_data['ecredit_base_info']
        # print(ecredit_base_info)

        self.variables['e_name'] = ecredit_base_info.ix[0,'ent_name']
        self.variables['soci_credict_code'] = ecredit_base_info.ix[0,'unify_credit_code']
        self.variables['ori_report_no'] = ecredit_base_info.ix[0,'report_no']
        self.variables['ori_report_date'] = str(ecredit_base_info.ix[0,'report_time'])