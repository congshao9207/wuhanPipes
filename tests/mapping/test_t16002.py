from view.v07001 import V07001


def test_ps_court_info():
    ps = V07001()
    ps.run(user_name='覃杨', id_card_no='150801196106278140', phone='13277154945')
    print(ps.variables['loan_analyst_overdue_amt_interval'])
