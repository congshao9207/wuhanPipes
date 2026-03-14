from mapping.module_processor import ModuleProcessor


class PcreditBizInfoView(ModuleProcessor):

    def process(self):
        self._get_biz_info_msg()

    def _get_biz_info_msg(self):
        biz_info_df = self.cached_data.get("pcredit_biz_info")
        if biz_info_df is None or biz_info_df.empty:
            return
        biz_type_df = biz_info_df[(biz_info_df['biz_type'].isin(["01", "03"])) & (biz_info_df['biz_first_month'] != "--")]
        if biz_type_df is None or biz_type_df.empty:
            return
        biz_type_df = biz_type_df.sort_values(by='biz_first_month')
        self.variables["biz_firstmonth"] = biz_type_df.loc[:, 'biz_first_month'].tolist()[0]
