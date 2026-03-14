# @Time : 2020/4/23 7:27 PM 
# @Author : lixiaobo
# @File : test_t41001.py.py 
# @Software: PyCharm
from mapping.t41001 import T41001
from util.mysql_reader import sql_to_df


def test_t41001_mapping_by_report_id():
    t41001 = T41001()
    df = sql_to_df(sql='select * from credit_parse_request where report_id = %(report_id)s', params={"report_id": "705779274614968320"})
    if df.empty:
        print("没查找到数据.")
        return

    out_req_no = df.iloc[0].out_req_no

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
    t41001.run(user_name='胡文文', id_card_no='340406199005021652', phone='15800426764', origin_data=origin_data,
               cached_data={})
    print(t41001.variables['credit_min_payed_number'])
    print(t41001.variables['credit_now_overdue_money'])
