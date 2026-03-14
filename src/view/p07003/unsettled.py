from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
from datetime import datetime,timedelta
import numpy as np

class Unsettled(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "unsettled"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "table_inst_name": [],
            "table_grant_type": [],
            "table_bus_type": [],
            "table_bus_sup":[],
            "table_guar_type": [],
            "table_grant_amt": [],
            "table_cur_bal": [],
            "table_margin_ratio": [],
            "table_start_date": [],
            "table_due_date": [],
            "table_category": [],
            "table_spec_trans": [],
            "table_spec_remark": [],
            "table_chased": [],
            "table_overdued": [],
            "table_overdue_amt": [],
            "table_overdue_prin": [],
            "table_overdue_m_cnt": [],
            "table_recent_repay_date": [],
            "table_recent_repay_amt": [],

            "add_app_1m":0,
            "add_app_3m":0,
            "add_app_6m":0,
            "add_app_1y":0,

            "bar_guar_type": [],
            "bar_guar_amt": [],
            "bar_guar_bal": [],
            "bar_guar_cnt": [],
            "bus_bar": [],
            "grant_history_bar": [],
            "discount_total_recent": 0,

            "pie_debt_bal": 0,
            "pie_debt_cnt": 0,
            "inst_pie_name": [],
            "inst_pie_loan_bal": [],
            "inst_pie_open_bal": [],
            "inst_pie_debt_prop": [],
            "bus_pie_type": [],
            "bus_pie_debt_bal": [],
            "bus_pie_debt_prop": [],
            "amt_pie": []
        }
        self.unsettled_table_df = None


    def transform(self):
        self.unsettled_table()
        self.unsettled_bar()
        self.unsettled_pie()

    def unsettled_table(self):
        col_list = [
            'inst_name',
            'grant_type',
            'bus_type',
            'bus_sup',
            'guar_type',
            'grant_amt',
            'cur_bal',
            'margin_ratio',
            'start_date',
            'due_date',
            'category',
            'spec_trans',
            'spec_remark',
            'chased',
            'overdued',
            'overdue_amt',
            'overdue_prin',
            'overdue_m_cnt',
            'recent_repay_date',
            'recent_repay_amt'
        ]
        df =  pd.DataFrame(data=None,
                           columns=col_list)
        loan_data = self.cached_data["ecredit_loan"][['settle_status',
                                                      'account_org','occur_type','account_type',
                                                      'biz_type','loan_guarantee_type',
                                                  'amount','balance','loan_date','end_date','category',
                                                  'special_briefgv','overdue_amt','overdue_principal',
                                                  'overdue_period','last_repay_date','last_repay_amt']]
        rename_loan = {
            'account_org':'inst_name',
            'occur_type':'grant_type',
            'account_type':'bus_sup',
            'biz_type':'bus_type',
            'loan_guarantee_type':'guar_type',
            'amount':'grant_amt',
            'balance':'cur_bal',
            'loan_date':'start_date',
            'end_date':'due_date',
            'special_briefgv':'spec_trans',
            'overdue_principal':'overdue_prin',
            'overdue_period':'overdue_m_cnt',
            'last_repay_date':'recent_repay_date',
            'last_repay_amt':'recent_repay_amt'
        }
        loan_data.rename(columns = rename_loan,
                         inplace = True)

        loan1 = loan_data[(loan_data.settle_status.str.contains("被追偿业务"))
                          &(loan_data.cur_bal > 0 )].drop(columns='settle_status')
        loan1['chased'] = "被追偿"
        loan2 = loan_data[loan_data.settle_status.str.contains("未结清信贷")].drop(columns='settle_status')

        open1 = pd.merge(self.cached_data.get("ecredit_credit_biz")[['id','account_type']],
                       self.cached_data.get("ecredit_draft_lc"),
                       left_on="id",right_on="biz_id"
                       )[['settle_status',
                          'account_org','biz_type','account_type',
                          'counter_guarantee_type','amount','balance','deposit_rate',
                          'loan_date','end_date','category','loan_guarantee_type']]
        open1 = open1[open1.settle_status.str.contains("未结清信贷")]
        if not open1.empty:
            open1['counter_guarantee_type'] = open1.apply(lambda x : x['counter_guarantee_type'] if pd.notnull(x['counter_guarantee_type']) else x['loan_guarantee_type'] , axis = 1)
        open1 = open1.drop(columns=['settle_status','loan_guarantee_type'])

        rename_open = {
            'account_org':'inst_name',
            'biz_type':'bus_type',
            'account_type':'bus_sup',
            'counter_guarantee_type':'guar_type',
            'amount':'grant_amt',
            'balance':'cur_bal',
            'deposit_rate':'margin_ratio',
            'loan_date':'start_date',
            'end_date':'due_date'
        }
        open1.rename(columns = rename_open,
                     inplace = True)

        # loan1['start_date'] = loan1.start_date.apply(lambda x: x.date())
        # loan1['due_date'] = loan1.due_date.apply(lambda x: x.date())
        # loan2['start_date'] = loan2.start_date.apply(lambda x: x.date())
        # loan2['due_date'] = loan2.due_date.apply(lambda x : x.date())

        df = pd.concat([df,loan2,loan1,open1] , ignore_index = True)
        df['overdued'] = df.fillna(0).overdue_amt.apply(lambda x :  "逾期" if x>0 else "")

        df = df.sort_values(by=['due_date','inst_name'] , ascending= [True,True])

        df['start_date'] = df.start_date.apply(lambda x: str(x) )
        df['due_date'] = df.due_date.apply(lambda x:  str(x) if pd.notnull(x) else None )
        df['recent_repay_date'] = df.recent_repay_date.apply(lambda x: str(x) if pd.notnull(x) else None)
        df['margin_percent'] = df.margin_ratio.apply(lambda x: str(round(x*100))+"%" if pd.notnull(x) else None)

        df = df.where(df.notnull(), None)

        self.variables['table_inst_name'] = df.inst_name.tolist()
        self.variables['table_grant_type'] = df.grant_type.tolist()
        self.variables['table_bus_type'] = df.bus_type.tolist()
        self.variables['table_bus_sup'] = df.bus_sup.tolist()
        self.variables['table_guar_type'] = df.guar_type.tolist()
        self.variables['table_grant_amt'] = df.grant_amt.tolist()
        self.variables['table_cur_bal'] = df.cur_bal.tolist()
        self.variables['table_margin_ratio'] = df.margin_percent.tolist()
        self.variables['table_start_date'] = df.start_date.tolist()
        self.variables['table_due_date'] = df.due_date.tolist()
        self.variables['table_category'] = df.category.tolist()
        self.variables['table_spec_trans'] = df.spec_trans.tolist()
        self.variables['table_spec_remark'] = df.spec_remark.tolist()
        self.variables['table_chased'] = df.chased.tolist()
        self.variables['table_overdued'] = df.overdued.tolist()
        self.variables['table_overdue_amt'] = df.overdue_amt.tolist()
        self.variables['table_overdue_prin'] = df.overdue_prin.tolist()
        self.variables['table_overdue_m_cnt'] = df.overdue_m_cnt.tolist()
        self.variables['table_recent_repay_date'] = df.recent_repay_date.tolist()
        self.variables['table_recent_repay_amt'] = df.recent_repay_amt.tolist()
        self.unsettled_table_df = df


        settled_df = self.cached_data["ecredit_loan"][
            ['id', 'settle_status', 'account_org', 'amount', 'balance', 'loan_date', 'end_date',
             'category', 'last_payment_type']]
        settled_df = settled_df[(settled_df.settle_status.str.contains("已结清信贷"))
                               | ((settled_df.settle_status.str.contains("被追偿"))
                                  & (settled_df.balance == 0))].drop(columns=['settle_status', 'balance'])

        self.variables["add_app_1m"] = df[(pd.to_datetime(df.start_date) >= datetime.now() - timedelta(days = 30))
                                          &(pd.to_datetime(df.start_date) < datetime.now() )].shape[0] \
                            + settled_df[(pd.to_datetime(settled_df.loan_date)  >= datetime.now() - timedelta(days = 30))
                                         &(pd.to_datetime(settled_df.loan_date) < datetime.now()) ].shape[0]

        self.variables["add_app_3m"] = df[(pd.to_datetime(df.start_date) >= datetime.now() - timedelta(days = 90))
                                          &(pd.to_datetime(df.start_date) < datetime.now() )].shape[0] \
                            + settled_df[(pd.to_datetime(settled_df.loan_date)  >= datetime.now() - timedelta(days = 90))
                                         &(pd.to_datetime(settled_df.loan_date) < datetime.now()) ].shape[0]

        self.variables["add_app_6m"] = df[(pd.to_datetime(df.start_date) >= datetime.now() - timedelta(days = 180))
                                          &(pd.to_datetime(df.start_date) < datetime.now() )].shape[0] \
                            + settled_df[(pd.to_datetime(settled_df.loan_date)  >= datetime.now() - timedelta(days = 180))
                                         &(pd.to_datetime(settled_df.loan_date) < datetime.now()) ].shape[0]

        self.variables["add_app_1y"] = df[(pd.to_datetime(df.start_date) >= datetime.now() - timedelta(days = 365))
                                          &(pd.to_datetime(df.start_date) < datetime.now() )].shape[0] \
                            + settled_df[(pd.to_datetime(settled_df.loan_date)  >= datetime.now() - timedelta(days = 365))
                                         &(pd.to_datetime(settled_df.loan_date) < datetime.now()) ].shape[0]

    def unsettled_bar(self):
        data = self.unsettled_table_df[self.unsettled_table_df.cur_bal > 0]
        if data is None or data.empty:
            return

        self.variables["bar_guar_type"] = ['信用/免担保', '保证', '质押', '抵押', '组合']
        guar_amt = []
        guar_bal = []
        guar_cnt = []
        for guar in self.variables["bar_guar_type"]:
            guar_amt.append(
                round(data[data.guar_type == guar].grant_amt.sum(),2)
            )
            guar_bal.append(
                round(data[data.guar_type == guar].cur_bal.sum(),2)
            )
            guar_cnt.append(
                data[data.guar_type == guar].shape[0]
            )
        self.variables["bar_guar_amt"] = guar_amt
        self.variables["bar_guar_bal"] = guar_bal
        self.variables["bar_guar_cnt"] = guar_cnt

        biz_list = set(data.bus_type.tolist())
        bus_bar = []
        for biz in biz_list:
            temp_dict = {}
            temp_df = data[data.bus_type == biz]
            temp_dict['bus_type'] = biz
            temp_dict['bus_amt'] = round(temp_df.grant_amt.sum(),2)
            temp_dict['bus_bal'] = round(temp_df.cur_bal.sum(),2)
            temp_dict['bus_cnt'] = temp_df.shape[0]
            temp_dict['bus_bal_detail'] = temp_df.groupby('bus_sup')['cur_bal'].agg(
                {'sup_amt': 'sum'}).reset_index().to_dict(orient='records')
            bus_bar.append(temp_dict)
        self.variables["bus_bar"] = bus_bar

        self.variables["discount_total_recent"] = round(data[(data.bus_sup.str.contains("贴现"))
             & (pd.to_datetime(data.start_date) > datetime.now() - timedelta(days=365))].grant_amt.sum(),2)

        data = self.cached_data["ecredit_loan"]
        rename_dict = {
            "account_type":"bus_sup",
            "biz_type":"bus_type",
            "account_org":"inst_name",
            "loan_date":"start_date",
            "end_date":"due_date",
            "amount":"grant_amt",
            "balance":"cur_bal"
        }
        data.rename(columns = rename_dict , inplace = True)

        report_year = pd.to_datetime(self.cached_data["report_time"]).year
        year_list = [report_year - 3, report_year - 2, report_year - 1, report_year]

        data['start_date_year'] = data.start_date.apply( lambda x: x.year)
        grant_history_bar = []
        for inst in set(data.inst_name.tolist()):
            temp_dict = {}
            temp_dict["inst_name"] = inst
            temp_dict["year_one"] = {
                "year": year_list[0],
                "grant_total": round(data[(data.start_date_year == year_list[0])
                                    &(data.inst_name == inst)].grant_amt.sum(),2),
                "bus_detail": data[(data.start_date_year == year_list[0])
                                   &(data.inst_name == inst)] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='records')
            }
            temp_dict["year_two"] = {
                "year": year_list[1],
                "grant_total": round(data[(data.start_date_year == year_list[1])
                                    &(data.inst_name == inst)].grant_amt.sum(),2),
                "bus_detail": data[(data.start_date_year == year_list[1])
                                   &(data.inst_name == inst)] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='records')
            }
            temp_dict["year_three"] = {
                "year": year_list[2],
                "grant_total": round(data[(data.start_date_year == year_list[2])
                                    &(data.inst_name == inst)].grant_amt.sum(),2),
                "bus_detail": data[(data.start_date_year == year_list[2])
                                   &(data.inst_name == inst)] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='records')
            }
            temp_dict["year_four"] = {
                "year": year_list[3],
                "grant_total": round(data[(data.start_date_year == year_list[3])
                                    &(data.inst_name == inst)].grant_amt.sum(),2),
                "bus_detail": data[(data.start_date_year == year_list[3])
                                   &(data.inst_name == inst)] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='records')
            }
            grant_history_bar.append(temp_dict)

        self.variables["grant_history_bar"] = grant_history_bar

    def unsettled_pie(self):

        data = self.unsettled_table_df[(~self.unsettled_table_df.bus_type.str.contains("贴现"))
                                       &(self.unsettled_table_df.cur_bal > 0)]
        if data is None or data.empty:
            return

        data['margin_ratio_c'] = 1 - data['margin_ratio'].fillna(0)
        self.variables["pie_debt_bal"] = round(np.dot( data.cur_bal.tolist(),data.margin_ratio_c.tolist() ),2)
        self.variables["pie_debt_cnt"] = data.shape[0]

        self.variables["inst_pie_name"] = list(set(data.inst_name.tolist()))
        inst_pie_loan_bal = []
        inst_pie_open_bal = []
        for inst in self.variables["inst_pie_name"]:
            inst_pie_loan_bal.append(
                round(data[(data.inst_name == inst)
                     & (pd.isnull(data.margin_ratio))].cur_bal.sum(),2)
            )

            temp_df = data[(data.inst_name == inst)
                           & (pd.notnull(data.margin_ratio))][['cur_bal', 'margin_ratio_c']]
            if temp_df.empty:
                inst_pie_open_bal.append(0)
            else:
                inst_pie_open_bal.append(
                    round(np.dot(temp_df.cur_bal.tolist(),
                           temp_df.margin_ratio_c.tolist() ),2 )
            )
        self.variables["inst_pie_loan_bal"] = inst_pie_loan_bal
        self.variables["inst_pie_open_bal"] = inst_pie_open_bal
        self.variables["inst_pie_debt_prop"] = list(
            np.round((np.array(inst_pie_loan_bal) + np.array(inst_pie_open_bal)) / self.variables["pie_debt_bal"], decimals=4)
        )

        self.variables["bus_pie_type"] = list(set(data.bus_type.tolist()))
        bus_pie_debt_bal = []
        for biz in self.variables["bus_pie_type"]:
            temp_df = data[data.bus_type == biz][['cur_bal', 'margin_ratio_c']]
            bus_pie_debt_bal.append(
                round(np.dot(temp_df.cur_bal.tolist(),
                       temp_df.margin_ratio_c.tolist()),2)
            )

        self.variables["bus_pie_debt_bal"] = bus_pie_debt_bal
        self.variables["bus_pie_debt_prop"] = list(
            np.round(np.array(bus_pie_debt_bal) / self.variables["pie_debt_bal"], decimals=4)
        )

        bins = [0, 50, 100, 200, 500, 1000, 10e8]
        bin_name = ['below50', '50to100', '100to200', '200to500', '500to1000', 'above1000']
        data['bin'] = pd.cut(data.grant_amt, bins, labels=False)
        data['bin'] = data.bin.apply(lambda x: bin_name[x])
        amt_pie = []

        for col in bin_name:
            temp_dict = {}
            temp_df = data[(data.bin == col)][['grant_amt', 'bin']]
            temp_dict['amt_pie_amt_range'] = col
            temp_dict['amt_pie_cnt'] = temp_df.shape[0]
            temp_dict['amt_pie_debt_prop'] = round(temp_df.shape[0] / data.shape[0] ,4)
            temp_dict['remark'] = temp_df.groupby('grant_amt')['bin'].agg(
                {'remark_cnt': 'count'}).reset_index().to_dict(orient='records')
            amt_pie.append(temp_dict)

        self.variables["amt_pie"] = amt_pie