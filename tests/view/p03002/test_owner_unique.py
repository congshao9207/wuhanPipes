import json

from file_utils.files import file_content

from view.p03002.owner_unique import Owner


def test_001():
    ps = Owner()
    ps.full_msg = json.loads(file_content(r"C:/workspace/pipes/tests/resource", "unin_level1_001.json"))
    ps.run(user_name="刘烈",id_card_no="360302197812104511",base_type="U_PERSONAL",origin_data={"extraParam":{"strategy":"01","education":"本科","marryState":"MARRIED"}})
    print(ps.variables)