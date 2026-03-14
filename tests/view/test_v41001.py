import json

from mapping.p07001_m.data_prepared_processor import DataPreparedProcessor
from view.v41001 import V41001


def test_v41001():
    ps = V41001()
    user_name = "施网明"
    id_card_no = "310108196610024859"
    phone = "15921664870"
    cached_data = {}
    origin_data = {
        "preReportReqNo": "45698789654278123890529002",
        "productCode": "07001",
        "versionNo": "1.0",
        "name": "施网明",
        "idno": "310108196610024859",
        "phone": "15921664870",
        "userType": "PERSONAL",
        "relation": "MAIN",
        "applyAmo": 222,
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

    ps.run(user_name=user_name, id_card_no=id_card_no, phone=phone, origin_data=origin_data, cached_data=cached_data)
    print(json.dumps(ps.variables))

