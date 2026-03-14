from mapping.tf0003 import Tf0003


def test_case_info():
    ps = Tf0003()
    # ps.run(user_name='张小江', id_card_no='130637198205101819', phone='')
    ps.run(user_name='张虎', id_card_no='430122197512087812', phone='')

def test_tf0003():
    ps1 = Tf0003()

    ps1.run("吴琴", "231085199311092239")
    print(ps1.variables['per_com_exception_result'])
