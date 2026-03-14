import json

from view.p06001.vf0004 import Vf0004

params = {'user_name': '宿州市徽香源食品有限公司',
          'id_card_no': '91341302760810639A',
          'apply_date': '2019-10-01'}


def test_vf0004():
    ps = Vf0004()
    ps.run(user_name=params['user_name'], id_card_no=params['id_card_no'],
           origin_data={'preBizDate': params['apply_date']})
    with open('f.txt', 'w') as f:
        f.write(json.dumps(ps.variables))
    # print(ps.variables)
