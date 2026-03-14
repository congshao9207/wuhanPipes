import json

from view.p06001.v24001 import V24001

params = {'user_name': '宿州市徽香源食品有限公司',
          'id_card_no': '91341302760810639A',
          'apply_date': '2019-10-01'}


def test_v24001():
    ps = V24001()
    ps.run(user_name=params['user_name'], id_card_no=params['id_card_no'],
           origin_data={'preBizDate': params['apply_date']})
    with open('f.txt', 'w') as f:
        f.write(json.dumps(ps.variables))
    # print(ps.variables)


def test_v24001_1():
    ps = V24001()
    ps.run(user_name="扬州润江混凝土有限公司", id_card_no="913210120676387625",
           origin_data={'preBizDate': params['apply_date']})
    print(json.dumps(ps.variables))
