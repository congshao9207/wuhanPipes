from mapping.t10001 import T10001


def test__ovdu_sco_1y():
    t10001 = T10001()
    t10001.run('祁雷', '371300196701160962')
    print(t10001.variables)
