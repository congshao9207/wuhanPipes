# @Time : 2020/4/23 7:27 PM
# @Author : lixiaobo
# @File : test_t41001.py.py
# @Software: PyCharm
from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from util.mysql_reader import sql_to_df
from view.v41001 import V41001


def test_v41001_batch():
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

        # print("out_req_no:", out_req_no, " out_apply_no", out_apply_no, " report_id", report_id)
        processed_count = processed_count + 1
        t41001 = V41001()
        cached_data = {}
        origin_data = {
            "reqNo": "Q388676688483090432",
            "stepReqNo": "S388676688512450560",
            "preReportReqNo": out_req_no,
            "productCode": "07001",
            "applyAmount": 100000,
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
        handler.init(t41001.variables, '施网明', '310108196610024859', origin_data=origin_data, cached_data=cached_data)
        handler.process()
        t41001.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data=origin_data,
                   cached_data=cached_data)
        variables_list.append(t41001.variables)

    # Analyze
    titled_variables = variables_list[0]
    not_matched_var = []
    matched_dict = {}
    matched_var = []
    for key in titled_variables:
        find = False
        for variables in variables_list:
            value = variables.get(key)
            report_no = variables.get("report_no")
            if isinstance(value, int) and value > 0 and not matched_dict.__contains__(key):
                matched_dict[key] = value
                matched_var.append(key)
                print(key)
                print(value)
                print(report_no)
            elif isinstance(value, str) and value != '' and value is not None and not matched_dict.__contains__(key):
                matched_dict[key] = value
                matched_var.append(key)
                print(key)
                print(value)
                print(report_no)
            elif isinstance(value, float) and value > 0 and not matched_dict.__contains__(key):
                matched_dict[key] = value
                matched_var.append(key)
                print(key)
                print(value)
                print(report_no)
            elif isinstance(value, list) and len(value) > 0 and not matched_dict.__contains__(key):
                matched_dict[key] = value
                matched_var.append(key)
                print(key)
                print(value)
                print(report_no)
            # if variables.get(key) != 0:
            #     find = True
            #     break
        # if not find:
        #     not_matched_var.append(key)

    not_matched_var = list(set(titled_variables).difference(set(matched_var)))

    print("_________________________________RESULT_________________________________")
    print("总报告数：", processed_count, ", 总共变量为：", len(titled_variables), ", 未匹配到的变量个数：", len(not_matched_var))
    for v in not_matched_var:
        print("---- ", v)
