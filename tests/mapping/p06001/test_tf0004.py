from mapping.p06001.tf0004 import Tf0004


def test_tf0004():
    ps = Tf0004()
    ps.run(user_name='四川三义边坡防护工程有限公司', id_card_no='915101246696574464',
           origin_data={'preBizDate': '2020-04-01'})
    print(ps.variables['com_bus_court_open_judge_proc_laf'])
