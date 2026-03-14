from view.v07001 import V07001

def test_v07001():
    ps = V07001()
    ps.run(user_name='马旭', id_card_no='120225199209236759', phone='15771073394')
    print(ps.variables)