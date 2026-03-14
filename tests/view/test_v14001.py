import json

from mapping.tranformer import fix_cannot_to_json
from view.v14001 import V14001


def test_v14001():
    ps = V14001()
    ps.run(user_name='卫叶斌', id_card_no='310115198208146818', phone='13277154945', origin_data={"preBizDate": "2019-11-04 10:24:33"})
    fix_cannot_to_json(ps.variables)
    print(json.dumps(ps.variables))

