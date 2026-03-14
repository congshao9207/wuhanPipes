from resource.v07001_v.test_util import run
from resource.v07001_v.test_util import run_case_by_report_id
from view.v41001 import V41001


def test_01():
    ps = V41001()
    run(ps, "350583197301156659")
    assert ps.variables["hint_account_org"]==['商业银行"FJ"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"NY"', '商业银行"FJ"', '商业银行"NY"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"NY"', '商业银行"NY"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"FJ"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"FJ"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"']
    assert ps.variables["hint_loan_date"] == ['2019-04-23', '2019-08-25', '2019-09-22', '2019-11-19', '2019-11-20', '2019-11-24', '2019-12-29', '2020-01-03', '2020-01-05', '2020-01-21', '2020-02-04', '2020-02-05', '2020-02-23', '2020-02-25', '2009-07-13', '2009-08-01', '2009-09-15', '2009-10-16', '2010-01-21', '2010-03-02', '2010-03-30', '2010-06-11', '2010-07-16', '2010-07-16', '2010-07-16', '2010-07-30', '2010-10-22', '2010-12-19', '2011-01-11', '2011-08-18', '2011-10-27', '2011-11-06', '2011-11-19', '2011-12-15', '2012-01-19', '2012-04-28', '2012-07-21', '2012-09-06', '2012-11-02', '2012-11-22', '2012-11-28', '2013-07-16', '2013-11-16', '2014-03-22', '2014-08-07', '2014-10-08', '2014-11-07', '2015-10-04', '2016-09-22', '2017-07-04', '2017-07-16', '2017-08-16', '2017-08-24', '2017-09-01', '2017-09-15', '2017-09-28', '2017-10-15', '2017-10-26', '2017-11-08', '2017-11-14', '2017-12-04', '2017-12-11', '2017-12-15', '2018-01-10', '2018-02-07', '2018-02-09', '2018-02-14', '2018-04-02', '2018-05-24', '2018-06-29', '2018-07-02', '2018-07-02', '2018-07-08', '2018-07-08', '2018-07-15', '2018-07-22', '2018-07-22', '2018-08-10', '2018-09-02', '2018-09-28', '2018-10-15', '2018-10-24', '2018-11-09', '2018-11-23', '2018-11-24', '2018-11-26', '2018-11-28', '2018-12-31', '2019-01-07', '2019-01-09', '2019-01-11', '2019-01-17', '2019-02-09', '2019-02-10', '2019-04-26', '2019-04-28', '2019-04-29', '2019-04-30', '2019-05-31', '2019-06-01', '2019-06-02', '2019-06-03', '2019-06-06', '2019-06-06', '2019-06-20', '2019-06-29', '2019-06-29', '2019-07-06', '2019-07-08', '2019-07-10', '2019-07-15', '2019-07-22', '2019-08-05', '2019-08-15', '2019-08-17', '2019-08-19', '2019-08-19', '2019-10-18', '2019-10-19', '2019-10-24', '2019-10-25', '2019-10-27', '2019-10-29', '2019-11-06', '2019-11-06', '2019-11-11', '2019-11-12', '2019-11-17', '2019-11-20', '2019-11-22', '2019-12-18', '2019-12-27', '2020-01-03', '2020-01-13', '2020-01-19', '2020-01-19']
    assert ps.variables["hint_principal_amount"] == [50000.0, 40000.0, 15000.0, 5000.0, 10000.0, 69000.0, 15000.0, 35000.0, 30000.0, 60000.0, 13000.0, 18000.0, 10000.0, 10000.0, 20000.0, 10000.0, 10000.0, 30000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 10000.0, 20000.0, 20000.0, 10000.0, 5000.0, 10000.0, 20000.0, 50000.0, 50000.0, 50000.0, 20000.0, 30000.0, 100000.0, 50000.0, 100000.0, 50000.0, 50000.0, 70000.0, 100000.0, 50000.0, 50000.0, 50000.0, 120000.0, 100000.0, 28000.0, 50000.0, 34000.0, 15000.0, 46000.0, 30000.0, 100000.0, 81000.0, 55000.0, 15000.0, 20000.0, 120000.0, 10000.0, 10000.0, 60000.0, 40000.0, 50000.0, 55000.0, 55000.0, 30000.0, 15000.0, 5000.0, 40000.0, 5000.0, 5000.0, 43000.0, 13000.0, 10000.0, 60000.0, 55000.0, 70000.0, 20000.0, 3000.0, 35000.0, 12000.0, 5000.0, 30000.0, 15000.0, 10000.0, 40000.0, 10000.0, 84000.0, 15000.0, 10000.0, 6000.0, 19000.0, 35000.0, 7000.0, 5000.0, 15000.0, 20000.0, 21000.0, 31000.0, 150000.0, 150000.0, 40000.0, 20000.0, 40000.0, 15000.0, 15000.0, 20000.0, 30000.0, 30000.0, 30000.0, 10000.0, 35000.0, 20000.0, 4000.0, 50000.0, 5000.0, 15000.0, 13000.0, 33000.0, 20000.0, 120000.0, 9000.0, 5000.0, 20000.0, 10000.0, 81000.0, 50000.0, 86000.0, 16000.0, 10000.0]



def test_02():
    ps = V41001()
    run(ps, "342422198208101115")
    assert ps.variables["guar_acc_org"] == ['商业银行"OK"', '商业银行"OK"', '商业银行"OK"', '商业银行"OK"', '商业银行"TR"', '商业银行"OK"', '商业银行"OK"']
    assert ps.variables["guar_loan_type"] == ['11', '11', '11', '01', '贷款', '贷款', '贷款']
    assert ps.variables["guar_end_date"] == ['2099-12-12', '2099-12-12', '2099-12-12', '2020-04-23', '2020-10-20', '2022-06-06', '2020-06-01']
    assert ps.variables["guar_principal_amount"] == [1000.0, 200000.0, 300000.0, 100000.0, 2500000.0, 5500000.0, 1200000.0]
    assert ps.variables["guar_loan_balance"] == [0.0, 0.0, 0.0, 100000.0, 2000000.0, 2000000.0, 1000000.0]
    assert ps.variables["guar_latest_category"] == ['--', '--', '--', '1', '1', '1', '1']
    assert ps.variables["guar_acc_org_cnt"] == 2
    assert ps.variables["total_guar_principal_amount"] == 9801000.0
    assert ps.variables["total_guar_loan_balance"] == 5100000.0


def test_03():
    ps = V41001()
    run(ps,"430102197401124574")
    assert ps.variables["loan_principal_20_50w_cnt"] == 2
    assert ps.variables["mort_no_settle_loan_date"] == ['2008-03-31', '2008-03-31', '2008-03-31']

def test_04():
    ps = V41001()
    run(ps, "362322198410268478")
    # assert ps.variables["loan_principal_50_100w_cnt"] == 3
    print(ps.variables['new_org_3m_ago'])
    print(ps.variables['new_org_6m_ago'])
    print(ps.variables['new_org_12m_ago'])

def test_05():
    ps = V41001()
    run(ps, "421223199302020513")
    print("new_org_6m_ago:"+str(ps.variables["new_org_6m_ago"]))
    print("new_org_12m_ago:" + str(ps.variables["new_org_12m_ago"]))
    print("loan_type_6m_ago:" + str(ps.variables["loan_type_6m_ago"]))
    print("loan_amount_6m_ago:" + str(ps.variables["principal_amount_6m_ago"]))
    print("loan_type_12m_ago:" + str(ps.variables["loan_type_12m_ago"]))
    print("loan_amount_12m_ago:" + str(ps.variables["principal_amount_12m_ago"]))


def test_06():
    ps = V41001()
    run(ps,  "350583197301156659")
    print(ps.variables["total_credit_card_limit"])
    # assert ps.variables["category"] == ['1', '--']
    # assert ps.variables["guar_type"] == ['抵质押类', '担保信用类']
    # assert ps.variables["guar_type_balance"] == [310000.0, 50000.0]
    # assert ps.variables["guar_type_cnt"] == [123, 4]
    # assert ps.variables["guar_type_balance_prop"] == ['0.86', '0.14', '0.00']
    # assert ps.variables["guar_type_balance_prop"] == ['0.86', '0.14', '0.00']
    # assert ps.variables["ensure_max_principal"] == 50000.0
    # assert ps.variables["mort_max_principal"] == 60000.0
    # assert ps.variables["ensure_principal_multi_apply"] == ''
    # assert ps.variables["mort_principal_multi_apply"] == ''
    # assert ps.variables["mort_settle_loan_date"] == []
    # assert ps.variables["credit_org"] == ['商业银行"XZ"', '商业银行"PP"', '商业银行"PP"', '商业银行"EL"', '商业银行"EL"', '商业银行"FJ"', '商业银行"NY"', '商业银行"DR"', '商业银行"CD"',
    #                                       '商业银行"ND"', '商业银行"LF"', '商业银行"LF"', '商业银行"LF"']
    # assert ps.variables["credit_loan_date"] == ['2020-02-12', '2018-04-10', '2018-04-10', '2018-01-15', '2018-01-15', '2016-10-08', '2015-10-09', '2015-10-08',
    #                                             '2015-08-03', '2014-01-14', '2013-06-09', '2012-06-19', '2012-06-19']
    # assert ps.variables["credit_loan_status"] == ['08', '01', '01', '08', '08', '01', '07', '01', '07', '08', '08', '01', '01']
    # assert ps.variables["credit_principal_amount"] == [2000.0, 14000.0, 14000.0, 3000.0, 3000.0, 5000.0, 150000.0, 65000.0, 0.0, 15000.0, 10000.0, 23000.0, 23000.0]
    # assert ps.variables["credit_quota_used"] == [0.0, 0.0, 0.0, 0.0, 0.0, 193.0, 0.0, 36341.0, 0.0, 0.0, 0.0, 0.0, 22284.0]
    # assert ps.variables["credit_avg_used_6m"] == [0.0, 0.0, 0.0, 0.0, 0.0, 84.0, 0.0, 21600.0, 0.0, 0.0, 0.0, 0.0, 33646.0]
    # assert ps.variables["credit_usage_rate"] == [0.0, 0.0, 0.0, 0.0, 0.0, 0.04, 0.0, 0.56, 0.0, 0.0, 0.0, 0.0, 1.46]
    # assert ps.variables["credit_min_repay"] == ['否', '否', '否', '否', '否', '否', '否', '否', '否', '否', '否', '否', '是']
    # assert ps.variables["credit_org_cnt"] == 13
    # assert ps.variables["total_credit_card_limit"] == 327000.0
    # assert ps.variables["total_credit_quota_used"] == 58818.0
    # assert ps.variables["total_credit_avg_used_6m"] == 55330.0
    # assert ps.variables["total_credit_usage_rate"] == 0.18
    # assert ps.variables["credit_min_repay_cnt"] == 1
    # assert ps.variables["total_credit_limit_3y_ago"] == 291000.0
    # assert ps.variables["total_credit_cnt_3y_ago"] == 8
    # assert ps.variables["total_credit_limit_2y_ago"] == 297000.0
    # assert ps.variables["total_credit_cnt_2y_ago"] == 10
    # assert ps.variables["total_credit_limit_1y_ago"] == 325000.0
    # assert ps.variables["total_credit_cnt_1y_ago"] == 12
    # assert ps.variables["account_org"] ==['商业银行"FJ"', '商业银行"NY"']
    # assert ps.variables["total_principal_3y_ago"] == [50000.0, 60000.0]
    # assert ps.variables["max_terms_3y_ago"] == [0.0, 11.0]
    # assert ps.variables["max_interest_rate_3y_ago"] == [0, 0]
    # assert ps.variables["total_principal_2y_ago"] == [50000.0, 84000.0]
    # assert ps.variables["max_terms_2y_ago"] == [0.0, 11.0]
    # assert ps.variables["max_interest_rate_2y_ago"] == [0, 0]
    # assert ps.variables["total_principal_1y_ago"] == [50000.0, 10000.0]
    # assert ps.variables["max_terms_1y_ago"] == [0.0, 29.0]
    # assert ps.variables["max_interest_rate_1y_ago"] == [0, 0]
    # assert ps.variables["new_org_3m_ago"] == ['商业银行"NY"', '商业银行"FJ"']
    # assert ps.variables["loan_type_3m_ago"] == ['01', '07']
    # assert ps.variables["principal_amount_3m_ago"] == [10000.0, 50000.0]
    # assert ps.variables["new_org_6m_ago"] == ['商业银行"NY"', '商业银行"FJ"']
    # assert ps.variables["loan_type_6m_ago"] == ['01', '07']
    # assert ps.variables["principal_amount_6m_ago"] ==[10000.0, 50000.0]
    # assert ps.variables["new_org_12m_ago"] == ['商业银行"NY"', '商业银行"FJ"']
    # assert ps.variables["loan_type_12m_ago"] == ['01', '07']
    # assert ps.variables["principal_amount_12m_ago"] == [10000.0, 50000.0]
    # assert ps.variables["each_loan_date"] == ['2019-04-23', '2019-08-25', '2019-09-22', '2019-11-19', '2019-11-20', '2019-11-24', '2019-12-29', '2020-01-03', '2020-01-05', '2020-01-21', '2020-02-04', '2020-02-05', '2020-02-23', '2020-02-25', '2018-04-02', '2018-05-24', '2018-06-29', '2018-07-02', '2018-07-02', '2018-07-08', '2018-07-08', '2018-07-15', '2018-07-22', '2018-07-22', '2018-08-10', '2018-09-02', '2018-09-28', '2018-10-15', '2018-10-24', '2018-11-09', '2018-11-23', '2018-11-24', '2018-11-26', '2018-11-28', '2018-12-31', '2019-01-07', '2019-01-09', '2019-01-11', '2019-01-17', '2019-02-09', '2019-02-10', '2019-04-26', '2019-04-28', '2019-04-29', '2019-04-30', '2019-05-31', '2019-06-01', '2019-06-02', '2019-06-03', '2019-06-06', '2019-06-06', '2019-06-20', '2019-06-29', '2019-06-29', '2019-07-06', '2019-07-08', '2019-07-10', '2019-07-15', '2019-07-22', '2019-08-05', '2019-08-15', '2019-08-17', '2019-08-19', '2019-08-19', '2019-10-18', '2019-10-19', '2019-10-24', '2019-10-25', '2019-10-27', '2019-10-29', '2019-11-06', '2019-11-06', '2019-11-11', '2019-11-12', '2019-11-17', '2019-11-20',
    #                                           '2019-11-22', '2019-12-18', '2019-12-27', '2020-01-03', '2020-01-13', '2020-01-19', '2020-01-19']
    # assert ps.variables["each_principal_amount"] == [50000.0, 40000.0, 15000.0, 5000.0, 10000.0, 69000.0, 15000.0, 35000.0, 30000.0, 60000.0, 13000.0, 18000.0, 10000.0, 10000.0, 40000.0, 50000.0, 55000.0, 55000.0, 30000.0, 15000.0, 5000.0, 40000.0, 5000.0, 5000.0, 43000.0, 13000.0, 10000.0, 60000.0, 55000.0, 70000.0, 20000.0, 3000.0, 35000.0, 12000.0, 5000.0, 30000.0, 15000.0, 10000.0, 40000.0, 10000.0, 84000.0, 15000.0, 10000.0, 6000.0, 19000.0, 35000.0, 7000.0, 5000.0, 15000.0, 20000.0, 21000.0, 31000.0, 150000.0, 150000.0, 40000.0, 20000.0, 40000.0, 15000.0, 15000.0, 20000.0, 30000.0, 30000.0, 30000.0, 10000.0, 35000.0, 20000.0, 4000.0, 50000.0, 5000.0, 15000.0, 13000.0, 33000.0,
    #                                                  20000.0, 120000.0, 9000.0, 5000.0, 20000.0, 10000.0, 81000.0, 50000.0, 86000.0, 16000.0, 10000.0]
    # assert ps.variables["each_loan_type"] == ['经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款',
    #                                           '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款', '经营性贷款']
    # assert ps.variables["each_interest_rate"] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    #                                               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    #                                               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    #                                               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # assert ps.variables["each_loan_status"] == ['01', '01', '01', '01', '01', '01', '01', '01', '01', '01', '01', '01',
    #                                             '01', '01', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04',
    #                                             '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04',
    #                                             '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04',
    #                                             '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04',
    #                                             '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04',
    #                                             '04', '04', '04', '04', '04', '04', '04', '04', '04', '04', '04']
    # assert ps.variables["max_principal_amount"] == 150000.0
    # assert ps.variables["min_principal_amount"] == 3000.0
    # assert ps.variables["rng_principal_amount"] == 147000.0
    # assert ps.variables["multiple_principal_amount"] == 50.00
    # assert ps.variables["loan_principal_0_20w_cnt"] == 83
    # assert ps.variables["loan_principal_total_cnt"] == 83
    # assert ps.variables["loan_principal_0_20w_prop"] == 1.00
    # assert ps.variables["loan_principal_20_50w_prop"] == 0.00
    # assert ps.variables["loan_principal_50_100w_prop"] == 0.00
    # assert ps.variables["loan_principal_100_200w_prop"] == 0.00
    # assert ps.variables["loan_principal_200w_prop"] == 0.00
    # assert ps.variables["loan_type"] == ['经营性贷款']
    # assert ps.variables["loan_type_balance"] == [360000.0]
    # assert ps.variables["loan_type_cnt"] == [136]
    # assert ps.variables["loan_type_balance_prop"] == ['1.00', '0.00', '0.00']

def test_06():
    ps = V41001()
    run(ps, "350583197301156659")
    assert ps.variables["settle_account_org"] == ['商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"', '商业银行"NY"',
                                                  '商业银行"NY"', '商业银行"NY"', '商业银行"NY"']
    assert ps.variables["settle_date"] == ['2019-12-23', '2019-11-10', '2019-10-13', '2019-10-21', '2019-10-13', '2019-10-30', '2019-10-13', '2019-11-03', '2019-10-21', '2019-11-10', '2019-11-06', '2019-11-10', '2019-12-25', '2019-11-03', '2019-10-31', '2019-11-10', '2019-11-10', '2019-11-21', '2019-12-25', '2019-12-25', '2019-11-21', '2019-12-23', '2019-12-25', '2020-01-04',
                                                      '2020-01-04', '2020-01-31', '2020-01-31', '2020-02-22']
    assert ps.variables["settle_loan_amount"] == [150000.0, 40000.0, 40000.0, 15000.0, 20000.0, 30000.0, 30000.0, 30000.0, 10000.0, 35000.0, 20000.0, 4000.0, 50000.0, 5000.0, 15000.0, 13000.0, 33000.0, 20000.0,
                                                  120000.0, 9000.0, 5000.0, 20000.0, 10000.0, 81000.0, 50000.0, 86000.0, 16000.0, 10000.0]




def test_null():
    ps = V41001()
    run(ps, "350583197301156659")
    #因为pcredit_acc_speculate没数据所以为空
    assert ps.variables["busi_org_cnt_2y_ago"] == ''
    assert ps.variables["busi_org_balance_2y_ago"] == ''
    assert ps.variables["busi_org_cnt_1y_ago"] == ''
    assert ps.variables["busi_org_balance_1y_ago"] == ''
    assert ps.variables["busi_org_cnt_3y_ago"] == ''
    assert ps.variables["busi_org_balance_3y_ago"] == ''
    assert ps.variables["busi_org_cnt_now"] == ''
    assert ps.variables["busi_org_balance_now"] == ''
    assert ps.variables["busi_loan_balance_max"] == ''
    assert ps.variables["busi_loan_balance_min"] == ''
    assert ps.variables["busi_org_balance_3y_ago_max"] == ''
    assert ps.variables["busi_org_balance_3y_ago_min"] == ''
    assert ps.variables["busi_org_balance_2y_ago_max"] == ''
    assert ps.variables["busi_org_balance_2y_ago_min"] == ''
    assert ps.variables["busi_org_balance_1y_ago_max"] == ''
    assert ps.variables["busi_org_balance_1y_ago_min"] == ''
    assert ps.variables["busi_org_balance_now_max"] == ''
    assert ps.variables["busi_org_balance_now_min"] == ''



def test_07():
    ps=V41001()
    # run_case_by_report_id(ps,"635534071564472320")
    # assert ps.variables['credit_now_overdue_cnt']==1
    # assert ps.variables['credit_now_overdue_money'] == 2330
    # run_case_by_report_id(ps, "658797566321889280")
    # assert ps.variables['credit_now_overdue_cnt']==6
    # assert ps.variables['credit_now_overdue_money'] == 465109
    # assert ps.variables['credit_min_repay_cnt'] == 6
    # run_case_by_report_id(ps, "703626609478995968")
    # assert ps.variables['credit_now_overdue_cnt'] == 0
    # assert ps.variables['credit_now_overdue_money'] == 0
    # run_case_by_report_id(ps,"681970431137288192")
    # assert ps.variables['loan_now_overdue_cnt']==1
    # assert ps.variables['loan_now_overdue_money'] == 1303
    run_case_by_report_id(ps, "683714959959527424")
    assert ps.variables['loan_now_overdue_cnt'] == 0
    assert ps.variables['loan_now_overdue_money'] == 0
