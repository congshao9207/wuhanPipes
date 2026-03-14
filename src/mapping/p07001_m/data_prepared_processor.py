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
        self.table_record_to_df("credit_base_info", report_id)
        self.table_record_to_df("pcredit_loan", report_id)
        self.table_record_to_df("pcredit_repayment", report_id)
        self.table_record_to_df("pcredit_default_info", report_id)
        self.table_record_to_df("pcredit_query_record", report_id)
        self.table_record_to_df("pcredit_person_info", report_id)
        self.table_record_to_df("pcredit_biz_info", report_id)
        self.table_record_to_df("pcredit_default_info", report_id)
        self.table_record_to_df("pcredit_live", report_id)
        self.table_record_to_df("pcredit_phone_his", report_id)
        self.table_record_to_df("pcredit_loan", report_id)
        self.table_record_to_df("pcredit_large_scale", report_id)
        self.table_record_to_df("pcredit_repayment", report_id)
        self.table_record_to_df("pcredit_info", report_id)
        self.table_record_to_df("pcredit_special", report_id)
        self.table_record_to_df("pcredit_force_execution_record", report_id)
        self.table_record_to_df("pcredit_profession", report_id)
        self.table_record_to_df("pcredit_civil_judgments_record", report_id)
        self.table_record_to_df("pcredit_credit_tax_record", report_id)
        self.table_record_to_df("pcredit_punishment_record", report_id)
        self.table_record_to_df("pcredit_acc_speculate", report_id)
        # 获取配偶征信贷款信息
        self._spouse_loan_df()
        # 获取企业信息
        self._ent_code()

        # 入参base_info的信息
        self._basic_info_extract()

    # 基本入参
    def _basic_info_extract(self):
        extra_param = self.origin_data["extraParam"]
        marry_state = extra_param.get("marryState")
        postal_address = extra_param.get("postalAddress")
        house_address = extra_param.get("houseAddress")
        live_address = extra_param.get("liveAddress")
        spouse_name = extra_param.get("spouseName")
        spouse_id_no = extra_param.get("spouseIdNo")
        work_unit = extra_param.get("workUnit")

        self.cached_data["basicMarryState"] = marry_state
        self.cached_data["basicPostalAddress"] = postal_address
        self.cached_data["basicHouseAddress"] = house_address
        self.cached_data["basicLiveAddress"] = live_address
        self.cached_data["basicWorkUnit"] = work_unit

        self.cached_data["spouseName"] = spouse_name
        self.cached_data["spouseIdNo"] = spouse_id_no

        credit_base_df = self.cached_data["credit_base_info"]
        self.cached_data["report_time"] = credit_base_df.iloc[0].report_time
        self.cached_data["id_card_no"] = credit_base_df.iloc[0].certificate_no

    # credit_parse_request表信息提取
    def _credit_parse_request_extract(self):
        pre_report_req_no = self.obtain_credit_parse_req_no()

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

    def obtain_credit_parse_req_no(self):
        pre_report_req_no = self.origin_data.get("preReportReqNo")
        if pre_report_req_no is None:
            extra_param = self.origin_data.get("extraParam")
            if extra_param:
                passthrough_msg = extra_param.get("passthroughMsg")
                if passthrough_msg:
                    pre_report_req_no = passthrough_msg.get("creditParseReqNo")

        if pre_report_req_no:
            return pre_report_req_no
        raise DataPreparedException(description="入参数字段preReportReqNo为空")

    # report_id对应的各表的记录获取
    def table_record_to_df(self, table_name, report_id):
        sql = "select * from " + table_name + " where report_id = %(report_id)s"
        df = sql_to_df(sql, params={"report_id": report_id})
        self.cached_data[table_name] = df

    def _spouse_loan_df(self):
        query_data = self.cached_data.get('query_data_array')
        if query_data is None:
            return
        spouse_data = None
        for data in query_data:
            if data is None:
                continue
            if data and data.get('relation') == 'SPOUSE':
                spouse_data = data
                break
        if spouse_data and spouse_data is None:
            return
        extra_param = spouse_data.get('extraParam')
        self.cached_data['spouseName'] = spouse_data.get('name')
        if extra_param is None:
            return
        pass_msg = extra_param.get('passthroughMsg')
        if pass_msg is None:
            return
        spouse_credit_parse_req_no = pass_msg.get('creditParseReqNo')
        if spouse_credit_parse_req_no is None:
            return
        sql = "select * from credit_parse_request where out_req_no = %(pre_report_req_no)s and process_status = 'DONE'"
        df = sql_to_df(sql=sql, params={"pre_report_req_no": spouse_credit_parse_req_no})
        if df.shape[0] == 0:
            return
        report_id = df.loc[0, 'report_id']

        sql = "select * from pcredit_loan where report_id = %(report_id)s"
        df = sql_to_df(sql, params={"report_id": report_id})
        self.cached_data['spouse_pcredit_loan'] = df

    def _ent_code(self):
        query_data = self.cached_data.get('query_data_array')
        ent_code = []
        if query_data is None:
            self.cached_data['ent_code'] = ent_code
        else:
            if query_data:
                for data in query_data:
                    if data is None:
                        continue
                    if data and data.get('userType') == 'COMPANY':
                        ent_code.append(data.get('idno'))
                self.cached_data['ent_code'] = ent_code
