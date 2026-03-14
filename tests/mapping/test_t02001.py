import pandas as pd

from mapping.t02001 import T02001


def test_phone_on_line_days():
    t = T02001()
    t.run(user_name='刘劭卓', id_card_no='430105199106096118', phone='11111111111')
    print(t.variables)

    t._phone_on_line_days(df=pd.DataFrame({
        'on_line_days': [3]
    }))
    print(t.variables)
    assert t.variables['phone_on_line_days'] == 3
