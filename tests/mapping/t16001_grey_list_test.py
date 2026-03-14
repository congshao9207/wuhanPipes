from mapping.t16001 import T16001


def test_16001():
    transformer = T16001()
    transformer.user_name = "施网明"
    transformer.id_card_no = "310108196610024859"
    transformer.transform()
    print(transformer.variables)


def test_16001_1():
    transformer = T16001()
    transformer.user_name = "上海乾享机械设备有限公司"
    transformer.id_card_no = "913101207747584215"
    transformer.transform()
    print(transformer.variables)
