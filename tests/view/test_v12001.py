from src.view.v12001 import V12001


def test_v12001():
    ps = V12001()
    ps.run(user_name='莘彬', id_card_no='350700196811219950', phone='15946515656')
    print(ps.variables)

