from mapping.module_processor import ModuleProcessor


class PcreditPhoneHisView(ModuleProcessor):

    def process(self):
        self._get_phone_his_msg()

    def _get_phone_his_msg(self):
        df = self.cached_data.get("pcredit_phone_his")
        if df is None or df.empty:
            return
        self.variables["phone"] = df[df['no'] == 1].loc[:, 'phone'].tolist()[0]
