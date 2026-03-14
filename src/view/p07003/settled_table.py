from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd

class SettledTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "settled_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst": [],
            "coop_cnt": [],
            "grant_total": [],
            "overdued": [],
            "last_repay_form": [],
            "category": [],
            "first_coop_date": [],
            "finish_coop_date": [],
            "grant_max": [],
            "grant_min": [],
            "coop_now" : []
        }

    def transform(self):
        loan_total = self.cached_data["ecredit_loan"][['id','settle_status','account_org','amount','balance','loan_date','last_repay_date',
                                                      'category','last_payment_type']]
        loan_data = loan_total[(loan_total.settle_status.str.contains("已结清信贷"))
                               |((loan_total.settle_status.str.contains("被追偿"))
                                 &(loan_total.balance==0))].drop(columns=['settle_status','balance'])

        if loan_data.empty:
            return

        # loan_data['loan_date'] = pd.to_datetime(loan_data['loan_date'])
        loan_data['last_repay_date'] = loan_data.apply(lambda  x:  x['loan_date'] if x['last_repay_date'] is None else x['last_repay_date'],
                                                       axis = 1)

        group1 = loan_data.drop(columns = 'id').groupby('account_org').agg({'amount':['count','sum','max','min'],
                                                                           'loan_date':['min'],
                                                                            'last_repay_date':['max']})
        group1.set_axis(['coop_cnt', 'grant_total',
                         'grant_max', 'grant_min',
                         'first_coop_date', 'finish_coop_date'],
                           axis='columns',
                           inplace=True)

        group2 = loan_data.drop(columns = 'id').sort_values( by = ['account_org','loan_date'] ,ascending = [True,False]  )\
            .drop_duplicates(subset = 'account_org',keep='first')[['account_org','last_payment_type','category']].set_index('account_org')

        group3 = pd.merge(loan_data[['id']],
                       self.cached_data["ecredit_histor_perfo"],
                       how='left',
                       left_on='id',
                       right_on='loan_id').groupby('account_org')['overdue_amt'].agg('sum').to_frame()
        group3['overdue_amt'] = group3.overdue_amt.apply( lambda x : "是" if x > 0 else "否")

        df = pd.concat( [group1 , group2 , group3] ,axis=1).reset_index()
        df.rename(columns = { 'account_org' : 'inst',
                              'overdue_amt' : 'overdued',
                              'last_payment_type':'last_repay_form'},
                  inplace = True)

        loan_list = loan_total[loan_total.settle_status.str.contains("被追偿|未结清")]['account_org'].drop_duplicates().tolist()

        df['first_coop_date'] = df.first_coop_date.apply(lambda x: str(x) if pd.notna(x) else None )
        df['finish_coop_date'] = df.finish_coop_date.apply(lambda x: str(x) if pd.notna(x) else None)

        df['coop_now'] = df.apply(lambda x : "是" if x['inst'] in loan_list else "否" , axis = 1)

        df = df.sort_values(by = 'finish_coop_date' , ascending = False)

        df['category'] = df.apply(lambda x : self.clean_category(x , loan_data) , axis = 1)

        df['grant_total'] = df.grant_total.apply(lambda x:round(x,2))
        df['grant_max']  = df.grant_max.apply(lambda x:round(x,2))
        df['grant_min']  = df.grant_min.apply(lambda x:round(x,2))

        self.variables["inst"] = df.inst.tolist()
        self.variables["coop_cnt"] = df.coop_cnt.tolist()
        self.variables["grant_total"] = df.grant_total.tolist()
        self.variables["overdued"] = df.overdued.tolist()
        self.variables["last_repay_form"] = df.last_repay_form.tolist()
        self.variables["category"] = df.category.tolist()
        self.variables["first_coop_date"] = df.first_coop_date.tolist()
        self.variables["finish_coop_date"] = df.finish_coop_date.tolist()
        self.variables["grant_max"] = df.grant_max.tolist()
        self.variables["grant_min"] = df.grant_min.tolist()
        self.variables["coop_now"] = df.coop_now.tolist()


    def clean_category(self, s , loan_data):
        temp_data = loan_data[loan_data.account_org == s['inst']]

        if temp_data[temp_data.category.str.contains("关注")].shape[0] > 0 :
            return "关注"
        elif temp_data[temp_data.category.str.contains("次级")].shape[0] > 0 :
            return "次级"
        elif temp_data[temp_data.category.str.contains("可疑")].shape[0] > 0 :
            return "可疑"
        elif temp_data[temp_data.category.str.contains("损失")].shape[0] > 0 :
            return "损失"
        else:
            return "正常"