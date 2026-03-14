from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd

class RrTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "rr_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst_name": [],
            "guar_type": [],
            "r_type": [],
            "bus_type": [],
            "r_amt": [],
            "r_bal": [],
            "start_date": [],
            "due_date": [],
            "category": [],
            "left_m_cnt": [],
            "chased": [],
            "overdued": [],
            "overdue_amt": [],
            "overdue_prin": [],
            "overdue_m_cnt": [],
            "recent_repay_date": [],
            "recent_repay_amt": []
        }

    def transform(self):
        col_list = [
            'inst_name',
            'guar_type',
            'r_type',
            'bus_type',
            'r_amt',
            'r_bal',
            'start_date',
            'due_date',
            'category',
            'left_m_cnt',
            'chased',
            'overdued',
            'overdue_amt',
            'overdue_prin',
            'overdue_m_cnt',
            'recent_repay_date',
            'recent_repay_amt'
        ]

        df = pd.DataFrame(data = None,
                          columns= col_list)

        repay_duty1 = self.cached_data["ecredit_repay_duty_biz"][['account_org','duty_type','biz_type','duty_amt',
                                                                     'balance','biz_date','end_date','category',
                                                                     'surplus_repay_period','overdue_amt',
                                                                     'overdue_principal','debt_status']]
        rename_rr1 = {
            'account_org':'inst_name',
            'duty_type':'r_type',
            'biz_type':'bus_type',
            'duty_amt':'r_amt',
            'balance':'r_bal',
            'biz_date':'start_date',
            'end_date':'due_date',
            'surplus_repay_period':'left_m_cnt',
            'overdue_principal':'overdue_prin',
            'debt_status':'overdue_m_cnt'
        }

        repay_duty1.rename( columns = rename_rr1,
                               inplace = True)
        repay_duty1['guar_type'] = "借贷账户(不含贴现)"

        repay_duty2  = self.cached_data["ecredit_repay_duty_discount"][['account_org','account_type','duty_type',
                                                                        'biz_type','duty_amt','balance','category',
                                                                        'overdue_amt','overdue_principal']]
        repay_duty2 = repay_duty2[repay_duty2.account_type.str.contains("贴现")]

        rename_rr2 = {
            'account_org':'inst_name',
            'account_type':'guar_type',
            'duty_type':'r_type',
            'biz_type':'bus_type',
            'duty_amt':'r_amt',
            'balance':'r_bal',
            'overdue_principal':'overdue_prin'
        }
        repay_duty2.rename( columns = rename_rr2,
                            inplace = True)

        repay_duty2['guar_type'] = "贴现账户"

        df = pd.concat([df,repay_duty1,repay_duty2] , ignore_index= True)

        df = df.sort_values(by =['due_date','inst_name'] , ascending= [True,True])

        df['start_date'] = df.start_date.apply(lambda x : str(x) if pd.notna(x) else None)
        df['due_date'] = df.due_date.apply(lambda x: str(x) if pd.notna(x) else None)
        df['chased'] = df.bus_type.apply(lambda x : "被追偿" if pd.notna(x) and ("垫款" in x or "资产处置" in x) else None)
        df['overdued'] = df.overdue_amt.apply(lambda x : "逾期" if pd.notnull(x) and x!=0 else None)

        df = df.where(df.notnull(), None)

        self.variables["inst_name"] = df.inst_name.tolist()
        self.variables["guar_type"] = df.guar_type.tolist()
        self.variables["r_type"] = df.r_type.tolist()
        self.variables["bus_type"] = df.bus_type.tolist()
        self.variables["r_amt"] = df.r_amt.tolist()
        self.variables["r_bal"] = df.r_bal.tolist()
        self.variables["start_date"] = df.start_date.tolist()
        self.variables["due_date"] = df.due_date.tolist()
        self.variables["category"] = df.category.tolist()
        self.variables["left_m_cnt"] = df.left_m_cnt.tolist()
        self.variables["chased"] = df.chased.tolist()
        self.variables["overdued"] = df.overdued.tolist()
        self.variables["overdue_amt"] = df.overdue_amt.tolist()
        self.variables["overdue_prin"] = df.overdue_prin.tolist()
        self.variables["overdue_m_cnt"] = df.overdue_m_cnt.tolist()
        self.variables["recent_repay_date"] = df.recent_repay_date.tolist()
        self.variables["recent_repay_amt"] = df.recent_repay_amt.tolist()