from mapping.p09003_m.limit_model_param import LimitModel
from mapping.p09003_m.pcredit_rule import PcreditRule


def test_p09003():
    lm = LimitModel()
    pr = PcreditRule()
    param = {}
    lm.process()
    pr.process()
