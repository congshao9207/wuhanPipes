from view.p03002.bus_unique import BusUnique


def test_001():
    ps = BusUnique()
    ps.run(user_name="沃尔玛（广东）百货有限公司", id_card_no="914401016852269688",base_type= "U_COMPANY",
           origin_data={"extraParam": {"strategy": "01"}, "name": "沃尔玛（广东）百货有限公司", "idno": "914401016852269688",
                        'baseType': "U_COMPANY"})
    print(ps.variables)