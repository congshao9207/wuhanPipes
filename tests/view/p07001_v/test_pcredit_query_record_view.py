from resource.v07001_v.find_report_id import find
from resource.v07001_v.test_util import run
from view.v41001 import V41001


def test_find_report_id():
    find('self_query_cnt')

def test_01():
    ps = V41001()
    run(ps,"430102197401124574")
    assert ps.variables["jhi_time_3m"]==['2020-01-10', '2019-12-11']
    assert ps.variables["operator_3m"] == ['小额贷款公司"DK"', '商业银行"IJ"']
    assert ps.variables["reason_3m"] == ['01', '01']
    assert ps.variables["jhi_time_1y"] == ['2020-01-10', '2019-12-11', '2019-10-18', '2019-09-18', '2019-09-03', '2019-06-13', '2019-06-05', '2019-05-20', '2019-05-15']
    assert ps.variables["operator_1y"] == ['小额贷款公司"DK"', '商业银行"IJ"', '商业银行"IJ"', '消费金融公司"CI"', '商业银行"SG"', '融资担保公司"TB"', '商业银行"GS"', '商业银行"YU"', '商业银行"FJ"']
    assert ps.variables["reason_1y"] == ['01', '01', '02', '01', '01', '01', '01', '02', '01']
    assert ps.variables["if_loan"] == ['是', '否', '是', '是', '否', '否', '否', '是', '是']
    assert ps.variables["credit_query_cnt"] == 2
    assert ps.variables["bank_query_cnt"] == 7
    assert ps.variables["operator_3m"] == ['小额贷款公司"DK"', '商业银行"IJ"']
    assert ps.variables["reason_3m"] == ['01', '01']
    assert ps.variables["bank_query_loan_cnt"] == 4



def test_02():
    ps = V41001()
    run(ps, "342622198105288176")
    assert ps.variables["guar_query_cnt"]==2


#todo 找不到有值的report_id
def test_null():
    ps = V41001()
    run(ps,  "350583197301156659")
    assert ps.variables["loan_query_cnt"] == ''
    assert ps.variables["self_query_cnt"] == ''
    assert ps.variables["credit_query_loan_cnt"] == ''


