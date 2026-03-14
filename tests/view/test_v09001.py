from view.v09001 import V09001

def test_ps_loan_other():
    ps = V09001()
    ps.run(user_name='何珊珊', id_card_no='530424197401173462', phone='')
    print(ps.variables)