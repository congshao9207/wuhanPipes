from view.p03002.fin_unique import Fin


def test_001():
    ps = Fin()
    ps.run(user_name="梅相海", id_card_no="420281198102032438",
           origin_data={"extraParam": {"strategy": "01"}, "name": "梅相海", "idno": "420281198102032438",
                        'baseType': "U_PERSONAL"})
    print(ps.variables)

