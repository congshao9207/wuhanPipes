from mapping.t05001 import T05001
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T05005(T05001):
    """
    手机状态
    """
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'phone_check': 1
        }
