# @Time : 2020/7/2 9:51 AM 
# @Author : lixiaobo
# @File : parser_001_test.py.py 
# @Software: PyCharm


def test_parser_001(client):
    print("begin test parser 001", client)
    files = {'file': open('../resource/base_type_test_01.json', 'rb')}
    data = {
        "parseCode":"001",
        "param": '''{
                "appId":"0000000000",
                "cusType":"PERSONAL",
                "cusName":"xxxx",
                "idNo":"329489338948483",
                "idType":"ID_CARD_NO",
                "bankAccount":"39283492342",
                "bankName":"招行",
                "outApplyNo":"IPQ2838439439",
                "attachmentId":"88384",
                "outReqNo":"48348395854",
                "bizReqNo":"32i343234242432",
                "accountId":"3424342"
            }'''
    }
    data.update(files)
    resp = client.post("/parse", data=data)

    print("resp:", resp.get_json())
