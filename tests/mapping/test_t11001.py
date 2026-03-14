import json
import numpy

from mapping.t11001 import T11001


def test_risk_score_loan():
    t11001 = T11001()
    t11001.run(user_name='刘劭卓', id_card_no='430105199106096118', phone='11111111111')
    # print(t11001.variables)
    for key, value in t11001.variables.items():
        print(key)
        print(type(value))
        if type(value) == numpy.int64:
            t11001.variables[key] = int(value)
        print(value)
    print(json.dumps(t11001.variables))
