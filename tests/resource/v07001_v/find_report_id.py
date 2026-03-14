from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from util.mysql_reader import sql_to_df
from view.v41001 import V41001
import numpy as np

def find(param):
    sql = '''
           select * from credit_parse_request
           '''
    df = sql_to_df(sql=sql)
    variables_list = []
    processed_count = 0
    for row in df.itertuples():
        if row.process_status != "DONE":
            continue
        if "重复报告" in row.process_memo:
            continue
        out_req_no = row.out_req_no
        out_apply_no = row.out_apply_no
        report_id = row.report_id

        processed_count = processed_count + 1
        ps = V41001()
        cached_data = {}
        origin_data = {
            "reqNo": "Q388676688483090432",
            "stepReqNo": "S388676688512450560",
            "preReportReqNo": out_req_no,
            "productCode": "07001",
            "versionNo": "1.0",
            "extraParam": {
                "marryState": "DIVORCE",
                "postalAddress": "上海市",
                "houseAddress": "上海市",
                "liveAddress": "上海市",
                "spouseName": "配偶姓名",
                "spouseIdNo": "78572847328943"
            }
        }
        handler = DataPreparedProcessor()
        handler.init(ps.variables, '施网明', '310108196610024859', origin_data=origin_data, cached_data=cached_data)
        handler.process()
        ps.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data=origin_data,
                   cached_data=cached_data)
        if isinstance(ps.variables[param],int) and ps.variables[param] >0:
            print("out_req_no:", out_req_no, " out_apply_no", out_apply_no, " report_id", report_id)
            print("结果----" + str(ps.variables[param]))
            break
        elif isinstance(ps.variables[param],str) and ps.variables[param]!= '' and ps.variables[param] is not None:
            print("out_req_no:", out_req_no, " out_apply_no", out_apply_no, " report_id", report_id)
            print("结果----" + str(ps.variables[param]))
            break
        elif isinstance(ps.variables[param],float) and ps.variables[param] >0:
            print("out_req_no:", out_req_no, " out_apply_no", out_apply_no, " report_id", report_id)
            print("结果----" + str(ps.variables[param]))
            break
        elif isinstance(ps.variables[param],list) and len(ps.variables[param]) >0:
            print("out_req_no:", out_req_no, " out_apply_no", out_apply_no, " report_id", report_id)
            print("结果----" + str(ps.variables[param]))
            break



