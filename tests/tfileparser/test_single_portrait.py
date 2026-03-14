from app import sql_db
from portrait.transflow.single_portrait import SinglePortrait


query_data_array = [
    {
        "applyAmo": 66600,
        "authorStatus": "AUTHORIZED",
        "extraParam": {
            "accounts": [
                {
                    "bankAccount": "6228450438020639272",
                    "bankName": "工商银行"
                }
                # {
                #     "bankAccount": "66666688888888",
                #     "bankName": "农业银行"
                # },
                # {
                #     "bankAccount": "88888888666666",
                #     "bankName": "建设银行"
                # }
            ],
            "industry": "E20",
            "industryName": "xx行业",
            "seasonOffMonth": "2,3",
            "seasonOnMonth": "9,10",
            "seasonable": "1",
            "totalSalesLastYear": 23232
        },
        "fundratio": 0,
        "id": 11879,
        "idno": "666666888888",
        "name": "娄远超",
        "parentId": 0,
        "phone": "13611647802",
        "relation": "MAIN",
        "userType": "PERSONAL",
        "baseType": "U_PERSONAL",
        "baseTypeDetail": "U_PERSONAL"
    },
    {
        "applyAmo": 66600,
        "extraParam": {
            "accounts": [
                # {
                #     "bankAccount": "6222620110014694302",
                #     "bankName": "交通银行",
                # }
            ],
            "industry": "E20",
            "industryName": "xx行业",
            "seasonOffMonth": "2,3",
            "seasonOnMonth": "9,10",
            "seasonable": "1",
            "totalSalesLastYear": 23232
        },
        "fundratio": 0,
        "id": 11880,
        "idno": "202007001406374532",
        "name": "王伟芳",
        "parentId": 0,
        "phone": "021-1234567",
        "relation": "SPOUSE",
        "userType": "PERSONAL",
        "baseType": "U_PERSONAL",
        "baseTypeDetail": "U_PER_SP_PERSONAL"
    }
]
public_param = {
    "reqNo": '',
    "reportReqNo": 'PQR202007091645',
    "productCode": '',
    "isUnion": '',
    "outApplyNo": 'IQP202011090123456',
    "applyAmt": '666000',
    "renewLoans": '',
    "historicalBiz": '',
}


def test_single():
    s = SinglePortrait()
    s.query_data_array = query_data_array
    s.public_param = public_param
    s.user_name = '娄远超'
    s.sql_db = sql_db()
    s.process()
