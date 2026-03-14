import requests


def test_batch_gen_credit_report():
    url = 'http://192.168.1.32/api/gears/open/credit/parse'
    files = {'file': open('/Users/xiaoboli/Doc/2020 征信报告/二代报告样例/0212_翁永花_332502196612155325.html', 'rb')}
    data = {
        'appId': "0000000000",
        'outReqNo': "787492432432",
        "outApplyNo":"4727492948298432",
        "provider": "1",
        "type":"1",
        "version": "2"
    }

    response = requests.post(url, files=files, data=data)
    json = response.json()
    print(json)
