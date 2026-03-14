# @Time : 2020/4/23 7:43 PM 
# @Author : lixiaobo
# @File : basic_info_view.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# 基本信息view模块
from util.common_util import format_timestamp


class BasicInfoView(ModuleProcessor):
    def process(self):
        self._get_basic_info_msg()

    def _get_basic_info_msg(self):
        basic_info_df = self.cached_data.get("credit_base_info")
        # 姓名
        self.variables["name"] = self.user_name
        # 证件号
        self.variables["certificate_no"] = self.id_card_no
        if basic_info_df is None or basic_info_df.empty:
            return
        # 报告编号
        self.variables["report_no"] = basic_info_df.loc[0, 'report_no']
        # 报告时间
        self.variables["report_time"] = basic_info_df.loc[0, 'report_time'].strftime('%Y-%m-%d %H:%M:%S')

