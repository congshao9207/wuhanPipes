from mapping.tf0002 import Tf0002


def test_f0002():
    t = Tf0002()
    t.run(user_name='苏玉兰', id_card_no='451226194811139899')
    # print(t.variables)
    # t.run(user_name='马利勇', id_card_no='140112197709011713')
    # print(t.variables)
