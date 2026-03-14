from resource.v07001_v.test_util import read_file, run
from view.v41001 import V41001


def test01():
    ps = V41001()
    run(ps, "350583197301156659")
    # 性别
    assert ps.variables["sex"] == '1'
    # 婚姻状态
    assert ps.variables["marriage_status"] == '2'
    # 就业状态
    assert ps.variables["employment"] == '91'
    # 通讯地址
    assert ps.variables["communication_address"] == '南安市金淘镇晨光村吾坂28号'
    # 户籍地址
    assert ps.variables["residence_address"] == '金淘镇晨光村吾板28号'
    # spouse_name
    assert ps.variables["spouse_name"] == '洪淑娇'
    # 配偶证件号
    assert ps.variables["spouse_certificate_no"] == '350583197408056623'