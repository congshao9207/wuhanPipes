from resource.v07001_v.test_util import run
from view.v41001 import V41001


#todo 没有值的report_id
def test_null():
    ps = V41001()
    run(ps,  "350583197301156659")
    assert ps.variables["extension_number"]==''
