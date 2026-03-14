
from resource.v07001_v.test_util import run
from view.v41001 import V41001


def test_loan_now_overdue_money():
    ps = V41001()
    run(ps, "430626198611165115")
    assert ps.variables["loan_now_overdue_money"]==4469.0
    assert ps.variables["loan_now_overdue_cnt"] == 1
    assert ps.variables["single_car_loan_overdue_cnt_2y"] == '9'


def test_business_loan_overdue_money():
    ps = V41001()
    run(ps, "422325196901280558")
    assert ps.variables["business_loan_overdue_money"] == 4714.0


def test_single_house_loan_overdue_cnt_2y():
    ps = V41001()
    run(ps, "342201197604010429")
    assert ps.variables["single_house_loan_overdue_cnt_2y"] == '2'
    assert ps.variables["single_credit_overdue_cnt_2y"] == '1'

def test_single_consume_loan_overdue_cnt_2y():
    ps = V41001()
    run(ps, "430102197401124574")
    assert ps.variables["single_consume_loan_overdue_cnt_2y"] == '1'
    assert ps.variables["total_consume_loan_overdue_cnt_5y"] == 1
    assert ps.variables["loan_total_overdue_cnt"] == 1
    assert ps.variables["loan_max_overdue_month"] == '1'

def test_credit_now_overdue_money():
    ps = V41001()
    run(ps, "350181198401242271")
    assert ps.variables["credit_now_overdue_money"] == 2.0
    assert ps.variables["credit_now_overdue_cnt"] == 1

def test_credit_total_overdue_cnt():
    ps = V41001()
    run(ps,"350583197301156659")
    assert ps.variables["credit_total_overdue_cnt"] == 4
    assert ps.variables["credit_overdue_max_month"] == '1'
    assert ps.variables["total_bank_credit_limit"] == 137000.00

#todo 找不到有值的report_id
def test_null():
    ps = V41001()
    run(ps, "350583197301156659")
    assert ps.variables["loan_overdue_2times_cnt"] == ''
    assert ps.variables["total_consume_loan_overdue_money_5y"] == ''
    assert ps.variables["total_bank_loan_balance"] == 0
