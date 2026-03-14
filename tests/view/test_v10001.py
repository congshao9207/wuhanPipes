from view.v10001 import V10001

def test_v10001():
    ps = V10001()
    ps.run(user_name='余乐', id_card_no='430621199005281426', phone='15771073394')
    print(ps.variables)