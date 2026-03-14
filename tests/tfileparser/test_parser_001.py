from app import sql_db
from fileparser.Parser001 import Parser001


file_0718_list = [
    r"../resource/trans_flow/0718文件类型错误校验失败.xls",
    r"../resource/trans_flow/0718空文件校验失败.xlsx",
    r"../resource/trans_flow/0718姓名不匹配校验失败.xlsx",
    r"../resource/trans_flow/0718账户不匹配校验失败.xlsx",
    r"../resource/trans_flow/0718时间格式不符合校验失败.xlsx"
    r"../resource/trans_flow/0718删除一行数据验真失败.xlsx",
    r"../resource/trans_flow/0718修改一个时间验真失败.xlsx"
]
param_0718 = {
    'cusType': "PERSONAL",
    'cusName': "张三",
    'idNo': "202202198604064532",
    'idType': "ID_CARD_NO",
    'bankAccount': "6222620110014694302",
    'bankName': "xx银行",
}

file = r"../resource/trans_flow/test20201126.xlsx"
param = {
    'bankAccount': '6228450438020639272',
    'cusName': '娄远超',
    'idNo': '666666888888'
}

file_list = [r"../resource/trans_flow/王伟明.xlsx", r"../resource/trans_flow/农行流水.xlsx",
             r"../resource/trans_flow/建行流水.xlsx", r"../resource/trans_flow/王伟芳.xlsx"]
param_list = [
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "6228480038574396172",
        'bankName': "工商银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "66666688888888",
        'bankName': "农业银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟明",
        'idNo': "202007091402563453",
        'idType': "ID_CARD_NO",
        'bankAccount': "88888888666666",
        'bankName': "建设银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    },
    {
        # 'appId': '商户端ID',
        'cusType': "PERSONAL",
        'cusName': "王伟芳",
        'idNo': "202007001406374532",
        'idType': "ID_CARD_NO",
        'bankAccount': "6222620110014694302",
        'bankName': "交通银行",
        # 'outApplyNo': "申请业务号",
        # 'outReqNo': "外部请求编号",
        # 'bizReqNo': "回执编号",
        # 'accountId': "账户编号"
    }
]


def test_001():
    p = Parser001()
    p.file = file
    p.param = param
    p.sql_db = sql_db()
    resp = p.process()
    print(resp)


def test_002():
    p = Parser001()
    p.file = file_list[2]
    p.param = param_list[2]
    p.sql_db = sql_db()
    resp = p.process()
    print(resp)


def test_003():
    p = Parser001()
    p.sql_db = sql_db()
    for i in range(4):
        p.file = file_list[i]
        p.param = param_list[i]
        resp = p.process()
        print(resp)


def test_004():
    p = Parser001()
    p.sql_db = sql_db()
    for i in range(len(file_0718_list)):
        p.file = file_0718_list[i]
        p.param = param_0718
        resp = p.process()
        print(resp)
