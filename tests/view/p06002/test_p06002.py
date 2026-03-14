
from view.p06002.loan_after_basic_info import LABasicInfo
from view.p06002.loan_after_black_list_info import LABlackListInfo
from view.p06002.loan_after_business_risk_info import LABusInfo
from view.p06002.loan_after_financial_info import LAFinInfo
from view.p06002.loan_after_fin_info import LABFinInfo
from view.p06002.loan_after_fraud_info import LAFraudInfo
from view.p06002.loan_after_owner_info import LAOwnerInfo


class Obj:

    def __init__(self):
        self.name = '徐静'
        self.idno = '34112619870910382X'
        self.mobile = '13803570221'
        self.last_report_time = '2019-10-01'
        self.loan_before_obj = [
            {'name': '大冶市慈和石品有限公司',
             'idno': '91420281667658935W',
             'phone': '',
             'userType': 'COMPANY'},
            {'name': '上海鑫思众餐饮有限公司',
             'idno': '913101105964975982',
             'phone': '',
             'userType': 'COMPANY'}]
        self.loan_after_obj = [
            {'name': '大冶市慈和石品有限公司',
             'idno': '91420281667658935W',
             'phone': '',
             'userType': 'COMPANY'},
            {'name': '上海鑫思众餐饮有限公司',
             'idno': '913101105964975982',
             'phone': '',
             'userType': 'COMPANY'},
            {'name': '杭州证客信息科技有限公司',
             'idno': '91330108352461046C',
             'phone': '',
             'userType': 'COMPANY'},
        ]


obj = Obj()


def test_basic():
    ps = LABasicInfo(obj)
    ps.transform()
    print(ps.variables)


def test_black():
    ps = LABlackListInfo()
    ps.transform()
    print(ps.variables)


def test_business():
    ps = LABusInfo()
    ps.transform()
    print(ps.variables)


def test_financial():
    ps = LAFinInfo()
    ps.transform()
    print(ps.variables)


def test_fin():
    ps = LABFinInfo()
    ps.transform()
    print(ps.variables)


def test_fraud():
    ps = LAFraudInfo()
    ps.transform()
    print(ps.variables)


def test_owner():
    ps = LAOwnerInfo()
    ps.transform()
    print(ps.variables)
