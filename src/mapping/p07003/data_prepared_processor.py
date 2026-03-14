# @Time : 2020/4/24 9:48 AM 
# @Author : lixiaobo
# @File : data_prepared_processor.py 
# @Software: PyCharm

# 数据准备阶段， 避免同一数据多次IO交互
from exceptions import DataPreparedException
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df


class DataPreparedProcessor(ModuleProcessor):

    def process(self):
        # print("DataPreparedProcessor process")
        # credit_parse_request表
        report_id = self._credit_parse_request_extract()
        # 表数据转换为DataFrame
        self.table_record_to_df("ecredit_base_info", report_id)
        self.table_record_to_df("ecredit_assets_outline", report_id)
        self.table_record_to_df("ecredit_civil_judgments", report_id)
        self.table_record_to_df("ecredit_controls_person", report_id)
        self.table_record_to_df("ecredit_credit_biz", report_id)
        self.table_record_to_df("ecredit_credit_info", report_id)
        self.table_record_to_df("ecredit_credit_outline", report_id)
        self.table_record_to_df("ecredit_debt_histor", report_id)
        self.table_record_to_df("ecredit_draft_lc", report_id)
        self.table_record_to_df("ecredit_force_execution", report_id)
        self.table_record_to_df("ecredit_generalize_info", report_id)
        self.table_record_to_df("ecredit_histor_perfo", report_id)
        self.table_record_to_df("ecredit_house_fund", report_id)
        self.table_record_to_df("ecredit_info_outline", report_id)
        self.table_record_to_df("ecredit_investor_info", report_id)
        self.table_record_to_df("ecredit_loan", report_id)
        self.table_record_to_df("ecredit_person_constitute_info", report_id)
        self.table_record_to_df("ecredit_punishment", report_id)
        self.table_record_to_df("ecredit_repay_duty_biz", report_id)
        self.table_record_to_df("ecredit_repay_duty_discount", report_id)
        self.table_record_to_df("ecredit_repay_duty_outline", report_id)
        self.table_record_to_df("ecredit_settle_outline", report_id)
        self.table_record_to_df("ecredit_superior_org", report_id)
        self.table_record_to_df("ecredit_uncleared_outline", report_id)


        # 入参base_info的信息
        self._basic_info_extract()

    # 基本入参
    def _basic_info_extract(self):
        extra_param = self.origin_data["extraParam"]
        manage_address = extra_param.get("manageAddress")
        registered_capital = extra_param.get("registeredCapital")
        mamage_amt = extra_param.get("mamageAmt")

        self.cached_data["manage_address"] = manage_address
        self.cached_data["registered_capital"] = registered_capital
        self.cached_data["mamage_amt"] = mamage_amt

        credit_base_df = self.cached_data["ecredit_base_info"]
        self.cached_data["report_time"] = credit_base_df.iloc[0].report_time
        self.cached_data["id_card_no"] = credit_base_df.iloc[0].unify_credit_code

    # credit_parse_request表信息提取
    def _credit_parse_request_extract(self):
        pre_report_req_no = self.origin_data["preReportReqNo"]
        if pre_report_req_no is None:
            raise DataPreparedException(description="入参数字段preReportReqNo为空")

        sql = "select * from credit_parse_request where out_req_no = %(pre_report_req_no)s"

        df = sql_to_df(sql=sql, params={"pre_report_req_no": pre_report_req_no})
        if df.empty:
            raise DataPreparedException(description="没有查得解析记录:" + pre_report_req_no)

        record = df.iloc[0]
        if "DONE" != record.process_status:
            raise DataPreparedException(description="报告解析状态不正常，操作失败：" + pre_report_req_no + " Status:" + record.process_status)

        report_id = df.iloc[0]["report_id"]

        self.cached_data["credit_parse_request"] = record
        self.cached_data["report_id"] = df.iloc[0]["report_id"]
        return report_id

    # report_id对应的各表的记录获取
    def table_record_to_df(self, table_name, report_id):
        sql = "select * from " + table_name + " where report_id = %(report_id)s"
        df = sql_to_df(sql, params={"report_id": report_id})
        self.cached_data[table_name] = df


