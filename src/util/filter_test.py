# @Time : 12/15/21 1:39 PM 
# @Author : lixiaobo
# @File : filter_test.py 
# @Software: PyCharm
from jsonpath import jsonpath


def test_filter():
    data = {
            "strategyParam":{
            "appId":"0000000000",
            "reqNo":"Q665285702447169536",
            "stepReqNo":"S665285724651814912",
            "productCode":"09003",
            "queryData":[
                {
                    "id":21956,
                    "parentId":0,
                    "name":"孙洋洋",
                    "idno":"340321198704272796",
                    "phone":"15366830155",
                    "userType":"PERSONAL",
                    "authorStatus":"AUTHORIZED",
                    "fundratio":0,
                    "applyAmo":0,
                    "baseType":"U_PERSONAL",
                    "relation":"MAIN",
                    "bizType":[
                        "00000",
                        "05002"
                    ],
                    "rules":[

                    ],
                    "extraParam":{
                        "accounts":[
                        ],
                    },
                    "segmentName":"HAND_SHAKE",
                    "nextSegmentName":"policy"
                },
                {
                    "id":21957,
                    "parentId":0,
                    "name":"王颖",
                    "idno":"360222199108290524",
                    "phone":"13597623003",
                    "userType":"PERSONAL",
                    "authorStatus":"AUTHORIZED",
                    "fundratio":0,
                    "applyAmo":0,
                    "baseType":"U_PER_SP_PERSONAL",
                    "relation":"SPOUSE",
                    "bizType":[
                        "00000",
                        "05002"
                    ],
                    "rules":[

                    ]
                },
                {
                    "id":21958,
                    "parentId":0,
                    "name":"上海怡顺建设发展有限公司",
                    "idno":"91310115798977004Q",
                    "userType":"COMPANY",
                    "fundratio":0.35,
                    "applyAmo":0,
                    "baseType":"U_PER_SH_M_COMPANY",
                    "relation":"SHAREHOLDER",
                    "bizType":[
                        "00000",
                        "05002"
                    ],
                    "rules":[

                    ],
                    "extraParam":{
                    },
                    "segmentName":"HAND_SHAKE",
                    "nextSegmentName":"policy"
                }
            ],
            "preReportReqNo":"PR665285676455067648"
        }
    }
    info = jsonpath(data, "$.strategyParam.queryData")
    print(info[0])
    print(type(info[0]))
    main_data = list(filter(lambda e: e["relation"] == "MAIN", info[0]))[0]
    print(main_data)
