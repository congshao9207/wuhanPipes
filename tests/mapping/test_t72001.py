import re

from mapping.t72001 import T72001


def test_001():
    ps = T72001()
    # ps.run(user_name="胡晓骅",id_card_no="430102198108051016",phone="")
    ps.run(user_name="董伯文", id_card_no="142730199305130739", phone="")
    print(ps.variables)