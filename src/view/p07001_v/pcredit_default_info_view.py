from mapping.module_processor import ModuleProcessor


class PcreditDefaultInfoView(ModuleProcessor):

    def process(self):
        self._get_default_info_msg()

    def _get_default_info_msg(self):
        df = self.cached_data.get("pcredit_default_info")
        if df is None or df.empty:
            return
        type_list = None
        df_type_01 = df[df['default_type'] == '01']
        if not df_type_01.empty:
            type_list = df_type_01.loc[:, 'default_subtype'].tolist()
        df_type_02 = df[df['default_type'] == '02']
        if not df_type_02.empty:
            if type_list is None:
                type_list = ["呆账"]
            else:
                type_list.append("呆账")
        if type_list is None:
            return
        self.variables["default_type"] = list(set(type_list))
