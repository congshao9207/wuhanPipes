from mapping.t01001 import T01001


def test_phone_on_line_days():
    t = T01001()
    t.run(user_name='刘劭卓', id_card_no='430105199106096118', phone='11111111111')
    print(t.variables)

    # t._phone_on_line_state(df=pd.DataFrame({
    #     'mobile_state': ['NORMAL']
    # }))
    # print(t.variables)
    # assert t.variables['phone_on_line_state'] == 'NORMAL'
