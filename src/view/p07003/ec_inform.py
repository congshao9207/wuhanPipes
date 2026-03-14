from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
import numpy as np

class EcInform(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_inform"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "e_name": "",
            "credict_code": "",
            "soci_credict_code": "",
            "industry": "",
            "launch_year": 0,
            "address": "",
            "status": "",
            "capital": 0,
            "related_name": [],
            "relation": [],
            "id_code": [],
            "update_date": [],
            "remark": []

        }

    def transform(self):
        base_info = self.cached_data['ecredit_base_info']
        self.variables["e_name"] = base_info.ix[0,'ent_name']
        self.variables["credict_code"] = base_info.ix[0,'credit_code']
        self.variables["soci_credict_code"] = base_info.ix[0,'unify_credit_code']
        self.variables["capital"] = base_info.ix[0, 'registered_capital']
        generalize_info = self.cached_data['ecredit_generalize_info']
        if not generalize_info.empty:
            self.variables["industry"] = generalize_info.ix[0,'industry']
            self.variables["launch_year"] = generalize_info.ix[0, 'launch_year']
            self.variables["address"] = generalize_info.ix[0, 'office_site']
            self.variables["status"] = generalize_info.ix[0, 'life_status']

        self.e_relation()


    def e_relation(self):

        table1 = self.cached_data['ecredit_person_constitute_info'][['cust_name','cust_position','cert_no','update_date']]

        if table1[table1.cust_position.str.contains("法定代表人")].shape[0] > 1:
            table1 = table1.replace("法定代表人/非法人组织负责人", "非法人组织负责人")
        else:
            table1 = table1.replace("法定代表人/非法人组织负责人", "法定代表人")
        table1.rename(columns = {'cust_name':'related_name',
                                 'cust_position':'relation',
                                 'cert_no':'id_code'
                                 } ,
                      inplace = True)

        table2 = self.cached_data['ecredit_investor_info'][['investor','investor_type','cert_no','update_date','investor_rate']]
        table2.rename(columns = {'investor':'related_name',
                                 'investor_type':'relation',
                                 'cert_no':'id_code',
                                 'investor_rate':'remark'
                                 } ,
                      inplace = True)

        if not table2.empty:
            table2['relation'] = table2.apply( lambda x : x['relation'] + "(占股" + str(round(x['remark']*100,2)) + "%)"
                                                if x['remark'] is not None else x['relation'],
                                                axis = 1 )


        table3 = self.cached_data['ecredit_controls_person'][['cust_name','cert_no','update_date']].dropna()
        table3['relation'] = "实际控制人"
        table3.rename(columns = {'cust_name':'related_name',
                                 'cert_no':'id_code'
                                 } ,
                      inplace = True)

        table4 = self.cached_data['ecredit_superior_org'][['org_name','org_type','cert_no','update_date']]
        table4.rename(columns = {'org_name':'related_name',
                                 'org_type':'relation',
                                 'cert_no':'id_code'
                                 } ,
                      inplace = True)

        df = pd.DataFrame(data=None,
                          columns=['related_name','relation','id_code','update_date'])


        df = pd.concat([df,table1,table2.drop(columns= 'remark'),table3,table4],ignore_index=True)

        df = df.sort_values(by = 'update_date' , ascending= False)

        df['update_date'] = df.update_date.apply(lambda x:str(x))

        df = df.groupby(['id_code','related_name']).agg(lambda x: x.str.cat(sep = ";")).reset_index()

        df = df.where(df.notnull(), None)

        if not df.empty:
            self.variables['related_name'] = df['related_name'].tolist()
            self.variables['relation'] = df['relation'].tolist()
            self.variables['id_code'] = df['id_code'].tolist()
            self.variables['update_date'] = df['update_date'].tolist()
        # self.variables['remark'] = df['remark'].tolist()