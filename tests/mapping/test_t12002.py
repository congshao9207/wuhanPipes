from mapping.t12002 import T12002


def test_001():
    ps = T12002()
    ps.run(user_name="易胜进",id_card_no="430321198110270613",phone="")
    print(ps.variables)