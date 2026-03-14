import json
import numpy

from mapping.t24001 import T24001


def test_risk_score_loan():
    t24001 = T24001()
    t24001.run(user_name='上海点牛互联网金融信息服务有限公司', id_card_no='91310000MA1K32DCXU', phone='11111111111')
    print(t24001.variables)
