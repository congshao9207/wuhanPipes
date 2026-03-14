from mapping.t00000 import T00000


def test_base_info():
    t = T00000()
    t.run(user_name='刘劭卓', id_card_no='430105199106096118', phone='11111111111', user_type='PERSONAL')
    print(t.variables)

    try:
        t.run(user_name='刘劭卓', id_card_no='12345', phone='111111', user_type='PERSONAL')
    except Exception as err:
        print(str(err))
