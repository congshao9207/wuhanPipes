class A(object):
    def __init__(self):
        self.info = "super_class"

    def opt(self):
        print("\n====super opt")
        print("super_use df_client:", self.df_client)


class B(A):
    def __init__(self):
        self.info = "child class"


def test_class():
    b = B()
    b.df_client = "AAAAA"
    b.opt()
