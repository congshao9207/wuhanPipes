from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd

class EcPublicInform(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_public_inform"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "handle_inst": [],
            "handle_date": [],
            "handle_reason": [],
            "handle_no": [],
            "handle_type": [],
            "handle_amt": [],
            "handle_remark": [],
            "handle_status": [],
            "staff_size": 0,
            "pay_status": "",
            "arrears": 0,
            "recent_pay_date": ""
        }

    def transform(self):
        judgment = self.cached_data["ecredit_civil_judgments"][['court','filing_date',
                                                                'case_subject','case_no',
                                                                'target_amt','settle_type',
                                                                'lawsuit_standi']]
        judgment['lawsuit_standi'] = judgment.lawsuit_standi.apply(lambda x:  ("诉讼地位:" + x) if x is not None
                                                                                            else None)

        execution = self.cached_data["ecredit_force_execution"][['court','filing_date',
                                                                 'case_subject','case_no',
                                                                 'target_amt','case_status',
                                                                 'execution_target_amt']]
        execution["execution_target_amt"] = execution.execution_target_amt.apply(lambda x: ("已执行标的金额"
                                                                                            +str(x)
                                                                                            +"元") if x is not None else None)

        punishment = self.cached_data["ecredit_punishment"][['org_name','penalty_date',
                                                             'illegal_act','wird_no',
                                                             'penalty_amt','execute_status']]
        punishment["remark"] = ""


        handle_list = ['handle_inst','handle_date','handle_reason','handle_no','handle_amt','handle_status','handle_remark']

        judgment.set_axis(handle_list,
                          axis=1,
                          inplace = True)
        execution.set_axis(handle_list,
                           axis=1,
                           inplace = True)
        punishment.set_axis(handle_list,
                            axis=1,
                            inplace = True)

        judgment['handle_type']  = "民事判决记录"
        execution['handle_type'] = "强制执行记录"
        punishment['handle_type'] = "行政处罚记录"

        df = pd.concat( [judgment, execution, punishment],
                        ignore_index= True)


        df['handle_date'] = df.handle_date.apply(lambda x: str(x) if x is not None else None)
        df['handle_amt'] = df.handle_amt.apply( lambda x: round( x/10000 , 2) if pd.notnull(x) else None)

        df = df.where(df.notnull(), None)

        self.variables["handle_inst"] = df.handle_inst.tolist()
        self.variables["handle_date"] = df.handle_date.tolist()
        self.variables["handle_reason"] = df.handle_reason.tolist()
        self.variables["handle_no"] = df.handle_no.tolist()
        self.variables["handle_type"] = df.handle_type.tolist()
        self.variables["handle_amt"] = df.handle_amt.tolist()
        self.variables["handle_status"] = df.handle_status.tolist()
        self.variables["handle_remark"] = df.handle_remark.tolist()

        house_fund = self.cached_data["ecredit_house_fund"]
        if not house_fund.empty:
            self.variables["staff_size"] = house_fund.ix[0,"staff_num"]
            self.variables["pay_status"] = house_fund.ix[0,"pay_status"]
            self.variables["arrears"] = round(house_fund.ix[0,"arrearage_amt"] / 10000 , 2)
            self.variables["recent_pay_date"] = str(house_fund.ix[0,"last_date"])
