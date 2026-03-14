import re

from mapping.t17003 import T17003


def test_001():
    ps = T17003()
    ps.run(user_name="易胜进",id_card_no="430321198110270613",phone="")
    print(ps.variables)



def test_002():
    value = "[0,2.5]"
    print(re.findall(r"\d+\.?\d*", value)[0])