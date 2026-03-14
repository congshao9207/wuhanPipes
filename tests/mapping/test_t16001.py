from datetime import datetime

import pandas as pd

from mapping.t06001 import T06001
from mapping.t16001 import T16001


def test_admi_vio():
    ps = T16001()
    # a = ['金额', '额度', '额度', '金额：']
    a = ['金额：1000.00', '额度：2001.5元', '额度500.678，金额：1002.3', '金额：1001.02，额度500.0009']
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]

    mock_df = pd.DataFrame({'execution_result': a, 'specific_date': b, 'create_time': c})
    ps._admi_vio(df=mock_df)
    assert ps.variables['court_admi_vio'] == 4
    assert ps.variables['court_admi_vio_amt_3y'] == 4004.82


def test_judge():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    # d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'case_amount': a, 'closed_time': b, 'create_time': c, 'legal_status': d})
    ps._judge(df=mock_df)

    assert ps.variables['court_judge'] == 4
    assert ps.variables['court_judge_amt_3y'] == 4004.82
    assert ps.variables['court_docu_status'] == 2


def test_trial_proc():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'case_amount': a, 'closed_time': b, 'create_time': c, 'legal_status': d})
    ps._trial_proc(df=mock_df)

    assert ps.variables['court_trial_proc'] == 4
    assert ps.variables['court_proc_status'] == 3


def test_tax_pay():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'case_amount': a, 'closed_time': b, 'create_time': c, 'legal_status': d})
    ps._tax_pay(df=mock_df)

    assert ps.variables['court_tax_pay'] == 4


def test_owed_owe():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._tax_arrears(df=mock_df)

    assert ps.variables['court_tax_arrears'] == 4
    assert ps.variables['court_tax_arrears_amt_3y'] == 4004.82


def test_tax_arrears():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._tax_arrears(df=mock_df)

    assert ps.variables['court_tax_arrears'] == 4
    assert ps.variables['court_tax_arrears_amt_3y'] == 4004.82


def test_dishonesty():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._dishonesty(df=mock_df)

    assert ps.variables['court_dishonesty'] == 4


def test_dishonesty():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._dishonesty(df=mock_df)

    assert ps.variables['court_dishonesty'] == 4


def test_limit_entry():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._limit_entry(df=mock_df)

    assert ps.variables['court_limit_entry'] == 4


def test_high_cons():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._high_cons(df=mock_df)

    assert ps.variables['court_high_cons'] == 4


def test_pub_info():
    ps = T16001()
    a = ['金额：1000.00', '[罚款金额(单位：万元):0.23,[金额:1000]', '[罚款金额(单位：万元):0.15,[金额:3400]', '[罚款金额(单位：万元:0.5']
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'execute_content': a, 'filing_time': b, 'create_time': c, 'legal_status': d})
    ps._pub_info(df=mock_df)
    assert ps.variables['court_pub_info'] == 4
    assert ps.variables['court_pub_info_amt_3y'] == 5700


def test_cri_sus():
    ps = T16001()
    a = [1000.00, 2001.5, 1002.3, 1001.02]
    b = [datetime(2015, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]
    d = ['张三', '李四', '王五', '陈六']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'taxes': a, 'taxes_time': b, 'create_time': c, 'legal_status': d})
    ps._cri_sus(df=mock_df)

    assert ps.variables['court_cri_sus'] == 4


def test_court_loan():
    ps = T16001()
    a = ['金额：被告', '[罚款金额(单位：万元):0.23,[金额:1000]', '[罚款金额(单位：万元):0.15,[金额:3400]', '[罚款金额(单位：万元:0.5']
    b = ['金融借款合同纠纷', '罚款金额', '罚款金额', '罚款金额']
    c = ['罚款金额', '被告', '被告', '被告']
    d = ['张三', '民间借贷纠纷', '王五', '借款合同纠纷']
    # d = [None, None, None, None]
    # d = ['张三原告', '李四', '王五', '陈六']
    # d = ['张三被告', '李四原告', '王五', '陈六']
    mock_df = pd.DataFrame({'legal_status': a, 'case_reason': b, 'trial_status': c, 'trial_reason': d})
    ps._court_loan(df=mock_df)

    assert ps.variables['court_fin_loan_con'] == 1
    assert ps.variables['court_loan_con'] == 1
    assert ps.variables['court_pop_loan'] == 1


def test_score():
    ps = T06001()
    ps.run(user_name='马慧丽', id_card_no='310222197408170040', phone='13277154945')
    print(ps.variables['ps_score'])
