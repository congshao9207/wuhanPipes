class LoanAfter:

    def __init__(self, origin_data):
        self.loan_before_obj = origin_data.get('strategyParam').get('preQueryData')
        self.loan_after_obj = origin_data.get('strategyParam').get('queryData')
        self.last_report_time = origin_data.get('strategyParam').get('preBizDate')

    @staticmethod
    def init_grouped_transformer(transformer):
        la = LoanAfter(transformer.full_msg)
        transformer.loan_before_obj = la.loan_before_obj if la.loan_before_obj is not None and \
            isinstance(la.loan_before_obj, list) and len(la.loan_before_obj) > 0 else None
        transformer.loan_after_obj = la.loan_after_obj
        transformer.last_report_time = la.last_report_time
