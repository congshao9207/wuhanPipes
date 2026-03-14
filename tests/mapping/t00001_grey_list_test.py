import pandas as pd

from mapping.t00001 import T00001


def test_00001():
    info = '''[{"id": 1, "appId": "0000000000", "riskDetail": "法院失信名单", "strategyCode": "222,333,4444", 
    "dataService": "\u5931\u4fe1\u540d\u5355", "createTime": "2019-10-16T02:50:06Z", "modifyTime": 
    "2019-10-16T02:50:11Z"}] '''
    df = pd.read_json(info)
    print(df)
    transform = T00001()
    transform.df = df
    transform.transform()
    print("variables:", transform.variables)
