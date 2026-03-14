from mapping.module_processor import ModuleProcessor
import pandas as pd


class PcreditLiveView(ModuleProcessor):

    def process(self):
        self._get_live_msg()

    def _get_live_msg(self):
        df = self.cached_data.get("pcredit_live")
        if df is None or df.empty:
            return
        live_address_list=df.sort_values(by='no').loc[:, 'live_address'].tolist()
        live_address_list=[x for x in live_address_list if x != '--' and pd.notnull(x)]
        if len(live_address_list) > 0:
            self.variables["live_address"] = live_address_list[0]
        self.variables["live_address_type"] = df[df['live_address_type'].isin(['01', '02', '06', '11'])].shape[0]
