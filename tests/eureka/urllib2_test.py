import json
import urllib
import urllib.request as urllib2

from py_eureka_client import eureka_client


def test_urllib2():
    payload = {"login": "admin", "password": "admin111"}
    data = json.dumps(payload)
    data = data.encode("UTF-8")

    request = urllib2.Request('http://192.168.1.15:100/gateway/api/users/login')
    request.add_header("Content-Type", "application/json")

    print("data type:", type(data), " data:", data)
    doc = urllib2.urlopen(request, data=data)
    print(doc.read().decode("UTF-8"))


# Eureka 使用PostPayload的方式
def test_urllib2_eureka():
    eureka_client.init(eureka_server="http://192.168.1.27:8030/eureka/",
                       app_name="PIPES",
                       instance_port=8010)

    payload = {"reportReqNo": "PR389856545250377728", "appId": "9999999999", "timestamp": 1573115853517,
               "requestType": "0"}
    data = json.dumps(payload)
    data = data.encode("UTF-8")

    headers = {"Content-Type": "application/json"}

    res = eureka_client.do_service("DEFENSOR",
                                   "/api/open/risk-interception/get", data=data, headers=headers)
    print("result of other service" + res)
