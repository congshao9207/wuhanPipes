from resource.v07001_v.test_util import read_file, run
from view.v41001 import V41001


def test01():
    ps = V41001()
    run(ps, "350583197301156659")
    assert ps.variables["biz_first_month"]=='2009.07'
