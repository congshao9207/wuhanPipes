from mapping.p07003.strategy_input_process import StrategyInputProcessor
from mapping.p07003.data_prepared_processor import DataPreparedProcessor
from mapping.tranformer import Transformer


class T41003(Transformer):
    """
    企业征信报告决策变量清洗入口及调度中心
    """
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "care_industry": 0,
            "keep_year": None,
            "abnorm_status": 0,
            "on_loan_cnt": 0,
            "on_loan_prop": 0,
            "app_cnt_recent": 0,
            "asset_dispose_amt": 0,
            "advance_amt": 0,
            "overdue_prin": 0,
            "overdue_interest": 0,
            "history_prin_overdue": 0,
            "bad_loan_cnt": 0,
            "care_loan_cnt": 0,
            "loan_detail":"正常",
            "postpone_cnt": 0,
            "bad_rr_cnt": 0,
            "care_rr_cnt": 0,
            "rr_detail":"正常",
            "bad_done_cnt": 0,
            "care_done_cnt": 0,
            "settled_detail":"正常",
            "risk_case_cnt": 0,
            "case_detail":"",
            "com_credit_debt_amt":[],
        }


    def transform(self):
        handle_list = [
            DataPreparedProcessor(),
            StrategyInputProcessor()
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()