# @Time : 2020/4/23 5:54 PM 
# @Author : lixiaobo
# @File : t41001.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from mapping.t41001_processors import T41001Processors
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class T41001(Transformer):

    """
    征信报告决策变量清洗入口及调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {}

    def __parse_product_code(self):
        try:
            strategy_param = self.full_msg.get("strategyParam")
            if strategy_param:
                return strategy_param.get('productCode')

            return self.full_msg.get("productCode")
        except Exception as e:
            logger.warn("__parse_product_code" + str(e))
        return None

    def transform(self):
        product_code = self.__parse_product_code()
        processors = T41001Processors()
        handlers, variables = processors.obtain_processors(product_code)
        self.variables.update(variables)

        for handler in handlers:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()


