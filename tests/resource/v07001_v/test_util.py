import json

from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from util.mysql_reader import sql_to_df


def read_file(url):
    f = open(url,'r', encoding='UTF-8')
    content = f.read()
    f.close()
    return json.loads(content)

def run(ps,id_card_no):
    sql='''
        select * from credit_base_info where certificate_no=%(certificate_no)s
    '''
    credit_base_info_df=sql_to_df(sql=sql,params={"certificate_no":id_card_no})
    report_id=credit_base_info_df.loc[0,'report_id']

    sql1 = '''
               select * from credit_parse_request where report_id=%(report_id)s
               '''
    df = sql_to_df(sql=sql1,params={"report_id":report_id})
    out_req_no=df.loc[0,'out_req_no']
    user_name = credit_base_info_df.loc[0,'name']
    id_card_no = id_card_no
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
    handler.init(ps.variables, user_name, id_card_no, origin_data=origin_data, cached_data=cached_data)
    handler.process()
    ps.run(user_name=user_name, id_card_no=id_card_no, phone=None, origin_data=origin_data, cached_data=cached_data)


def run_case_by_report_id(ps, report_id):
    sql = '''
        select * from credit_base_info where report_id=%(report_id)s
    '''
    credit_base_info_df = sql_to_df(sql=sql, params={"report_id": report_id})

    sql1 = '''
               select * from credit_parse_request where report_id=%(report_id)s
               '''
    df = sql_to_df(sql=sql1, params={"report_id": report_id})
    out_req_no = df.loc[0, 'out_req_no']
    user_name = credit_base_info_df.loc[0, 'name']
    id_card_no = credit_base_info_df.loc[0, 'certificate_no']
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
    handler.init(ps.variables, user_name, id_card_no, origin_data=origin_data, cached_data=cached_data)
    handler.process()
    ps.run(user_name=user_name, id_card_no=id_card_no, phone=None, origin_data=origin_data, cached_data=cached_data)
