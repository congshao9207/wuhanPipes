from mapping.t18001 import T18001


def test_18001():
    t = T18001()
    t.run(user_name='丘健', id_card_no='150400197201201535')
    print(t.variables)
