import datetime

import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.common_util import get_query_data
from util.mysql_reader import sql_to_df
import time


def trans_black_froz_time(begin,end):
    if pd.isna(begin) and pd.isna(end):
        return "/"
    elif pd.isna(begin) and pd.notna(end):
        return "/" + "至" + end.strftime('%Y-%m-%d')
    elif pd.notna(begin) and pd.isna(end):
        return begin.strftime('%Y-%m-%d') + "至" + "/"
    else:
        return begin.strftime('%Y-%m-%d') + "至" + end.strftime('%Y-%m-%d')


def get_diff_days(x):
    date = datetime.datetime.now() - pd.to_datetime(x)
    diff = date.days
    if diff < 0:
        return -diff
    return diff


class Black(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "black"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'black_list_cnt': 0,
            'black_overt_cnt': 0,
            'black_judge_cnt': 0,
            'black_exec_cnt': 0,
            'black_illegal_cnt': 0,
            'black_legel_cnt': 0,
            'black_froz_cnt': 0,
            'black_list_name': [],
            'black_list_tyle': [],
            'black_list_case_no': [],
            'black_list_detail': [],
            'black_overt_name': [],
            'black_overt_reason': [],
            'black_overt_type': [],
            'black_overt_authority': [],
            'black_overt_case_no': [],
            'black_overt_status': [],
            'black_overt_date': [],
            'black_judge_name': [],
            'black_judge_reason': [],
            'black_judge_authority': [],
            'black_judge_case_no': [],
            'black_judge_time': [],
            'black_judge_url': [],
            'black_exec_name': [],
            'black_exec_authority': [],
            'black_exec_case_no': [],
            'black_exec_date': [],
            'black_exec_content': [],
            'black_exec_type': [],
            'black_illegal_name': [],
            'black_illegal_reason': [],
            'black_illegal_datetime': [],
            'black_illegal_case_no': [],
            'black_legal_name': [],
            'black_legal_cause': [],
            'black_legal_date': [],
            'black_legal_org': [],
            'black_legal_clear_cause': [],
            'black_legal_clear_date': [],
            'black_legal_clear_org': [],
            'black_froz_name': [],
            'black_froz_role': [],
            'black_froz_status': [],
            'black_froz_execute_no': [],
            'black_froz_amt': [],
            'black_froz_inv': [],
            'black_froz_auth': [],
            'black_froz_public_date': [],
            'black_froz_time': [],
            'black_froz_thaw_date': [],
            'black_froz_invalid_date': [],
            'black_froz_invalid_reason': [],
            'black_punish_cnt': 0,
            'black_punish_name': [],
            'black_punish_reason': [],
            'black_punish_datetime': [],
            'black_punish_case_no': []
        }

    def _info_court(self, user_name, id_card_no):
        sql = '''
            select id from info_court where unique_name = %(user_name)s
        '''
        if pd.notna(id_card_no):
            sql += 'and unique_id_no = %(id_card_no)s'
        sql += 'AND unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 '
        df = sql_to_df(sql=sql,
                       params={"user_name": user_name,
                               "id_card_no": id_card_no})
        return df

    def _info_court_criminal_suspect(self, id_list):
        sql = '''
            select * from info_court_criminal_suspect where court_id in %(ids)s
        '''
        df = sql_to_df(sql=sql,
                       params={"ids": id_list})
        return df

    def _info_court_deadbeat(self,ids):
        sql = '''
                   select * from info_court_deadbeat where court_id in %(ids)s
               '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_court_limit_hignspending(self,ids):
        sql = '''
                   select * from info_court_limit_hignspending where court_id in %(ids)s
               '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_court_limited_entry_exit(self,ids):
        sql = '''
                   select * from info_court_limited_entry_exit where court_id in %(ids)s
               '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_court_trial_process(self,ids):
        sql = '''
                   select * from info_court_trial_process where court_id in %(ids)s
               '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        if not df.empty:
            df['diff_day'] = df.apply(lambda x : get_diff_days(x['specific_date']),axis=1)
        return df

    def _info_court_judicative_pape(self,ids):
        sql = '''
                          select * from info_court_judicative_pape where court_id in %(ids)s
                      '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        if not df.empty:
            df['diff_day'] = df.apply(lambda x : get_diff_days(x['closed_time']),axis=1)
        return df

    def _info_court_excute_public(self,ids):
        sql = '''
                          select * from info_court_excute_public where court_id in %(ids)s
                      '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_court_administrative_violation(self, ids):
        sql = '''
                             select * from info_court_administrative_violation where court_id in %(ids)s
                         '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_com_bus_basic(self, user_name, id_card_no):
        sql = '''
            select id,ent_name from info_com_bus_basic where ent_name = %(user_name)s and channel_api_no='24001'
        '''
        if pd.notna(id_card_no):
            sql += ' and credit_code = %(id_card_no)s'
        sql += 'AND unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 '
        df = sql_to_df(sql=sql,
                       params={"user_name": user_name,
                               "id_card_no": id_card_no})
        return df

    def _info_com_bus_illegal(self, ids):
        sql = '''
                   select * from info_com_bus_illegal where basic_id in %(ids)s and illegal_date_in is not NULL
              '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_com_bus_shares_frost(self, ids):
        sql = '''
                   select * from info_com_bus_shares_frost where basic_id in %(ids)s order by froz_ent,froz_public_date DESC
              '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def _info_com_bus_case(self, ids):
        sql = '''
           SELECT distinct a.ent_name,b.pendec_no,b.illegact_type_name,b.pen_deciss_date FROM info_com_bus_basic a,info_com_bus_case b
           where a.id = b.basic_id
           and b.basic_id in %(ids)s
        '''
        df = sql_to_df(sql=sql,
                       params={"ids": ids})
        return df

    def clean_variables_court(self):
        resp = get_query_data(self.full_msg, None, '01')
        ids = []
        for i in resp:
            user_name = i.get("name")
            id_card_no = i.get("id_card_no")
            court_df = self._info_court(user_name, id_card_no)
            if not court_df.empty:
                ids.append(int(court_df.loc[0,'id']))

        if len(ids) == 0:
            return
        suspect_df = self._info_court_criminal_suspect(ids)
        deadbeat_df = self._info_court_deadbeat(ids)
        hign_df = self._info_court_limit_hignspending(ids)
        entry_df = self._info_court_limited_entry_exit(ids)
        process_df = self._info_court_trial_process(ids)
        pape_df = self._info_court_judicative_pape(ids)
        public_df = self._info_court_excute_public(ids)
        violation_df = self._info_court_administrative_violation(ids)


        #black_list_cnt
        black_list_cnt = 0
        black_list_name = []
        black_list_tyle = []
        black_list_case_no = []
        black_list_detail = []
        if not suspect_df.empty:
            black_list_cnt = black_list_cnt + suspect_df.shape[0]
            for row in suspect_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('罪犯及嫌疑人')
                black_list_case_no.append(getattr(row, 'case_no'))
                black_list_detail.append(getattr(row, 'criminal_reason'))
        if not deadbeat_df.empty:
            deadbeat_df_tmep = deadbeat_df[deadbeat_df.execute_status != '已结案']
            if not deadbeat_df_tmep.empty:
                black_list_cnt = black_list_cnt + deadbeat_df_tmep.shape[0]
                for row in deadbeat_df_tmep.itertuples():
                    black_list_name.append(getattr(row, 'name'))
                    black_list_tyle.append('失信老赖')
                    black_list_case_no.append(getattr(row, 'execute_case_no'))
                    black_list_detail.append(getattr(row, 'execute_content'))
        if not hign_df.empty:
            black_list_cnt = black_list_cnt + hign_df.shape[0]
            for row in hign_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('限制高消费')
                black_list_case_no.append(getattr(row, 'execute_case_no'))
                black_list_detail.append(getattr(row, 'execute_content'))
        if not entry_df.empty:
            black_list_cnt = black_list_cnt + entry_df.shape[0]
            for row in hign_df.itertuples():
                black_list_name.append(getattr(row, 'name'))
                black_list_tyle.append('限制出入境')
                black_list_case_no.append(getattr(row, 'execute_no'))
                black_list_detail.append(getattr(row, 'execute_content'))
        self.variables['black_list_cnt'] = black_list_cnt
        self.variables['black_list_name'] = black_list_name
        self.variables['black_list_tyle'] = black_list_tyle
        self.variables['black_list_case_no'] = black_list_case_no
        self.variables['black_list_detail'] = black_list_detail

        #black_overt_cnt
        if not process_df.empty:
            process_df1 = process_df[pd.notna(process_df.case_no)].drop_duplicates()
            if not process_df1.empty:
                self.variables['black_overt_cnt'] = process_df1.shape[0]
                process_df2 = process_df1.sort_values(by='specific_date',ascending=False)
                process_df2['specific_date'] = process_df2['specific_date'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_overt_name'] = process_df2['name'].to_list()
                self.variables['black_overt_reason'] = process_df2['case_reason'].to_list()
                self.variables['black_overt_type'] = process_df2['legal_status'].to_list()
                self.variables['black_overt_authority'] = process_df2['trial_authority'].to_list()
                self.variables['black_overt_case_no'] = process_df2['case_no'].to_list()
                self.variables['black_overt_status'] = process_df2['date_type'].to_list()
                self.variables['black_overt_date'] = process_df2['specific_date'].to_list()



        #black_judge_cnt
        if not pape_df.empty:
            pape_df1 = pape_df[~pape_df.legal_status.str.contains('原告|申请执行人|第三人')]
            if not pape_df1.empty:
                self.variables['black_judge_cnt'] = pape_df1.shape[0]
                pape_df2 = pape_df1.sort_values(by='closed_time', ascending=False)
                pape_df2['closed_time'] = pape_df2['closed_time'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_judge_name'] = pape_df2['name'].to_list()
                self.variables['black_judge_reason'] = pape_df2['case_reason'].to_list()
                self.variables['black_judge_authority'] = pape_df2['trial_authority'].to_list()
                self.variables['black_judge_case_no'] = pape_df2['case_no'].to_list()
                self.variables['black_judge_time'] = pape_df2['closed_time'].to_list()
                self.variables['black_judge_url'] = pape_df2['url'].to_list()


        #black_exec_cnt
        if not public_df.empty:
            # public_df1 = public_df[~public_df.execute_status.str.contains("已结案")].drop_duplicates(subset=['execute_case_no'])
            public_df1 = public_df.drop_duplicates(subset=['execute_case_no'])
            if not public_df1.empty:
                self.variables['black_exec_cnt'] = public_df1.shape[0]
                public_df2 = public_df1.sort_values(by='filing_time',ascending=False)
                public_df2['filing_time'] = public_df2['filing_time'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_exec_name'] = public_df2['name'].to_list()
                self.variables['black_exec_authority'] = public_df2['execute_court'].to_list()
                self.variables['black_exec_case_no'] = public_df2['execute_case_no'].to_list()
                self.variables['black_exec_date'] = public_df2['filing_time'].to_list()
                self.variables['black_exec_content'] = public_df2['execute_content'].to_list()
                self.variables['black_exec_type'] = public_df2['execute_status'].to_list()


        #black_illegal_cnt
        if not violation_df.empty:
            violation_df1 = violation_df[(violation_df.case_no != "") | (violation_df.illegalreason != "")
                                         ][['name', 'illegalreason', 'specific_date', 'case_no']].drop_duplicates()
            if not violation_df1.empty:
                violation_df1 = violation_df1.sort_values(by='specific_date', ascending=False)
                violation_df1['specific_date'] = violation_df1['specific_date'].apply(
                    lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
                self.variables['black_illegal_cnt'] = violation_df1.shape[0]
                self.variables['black_illegal_name'] = violation_df1['name'].to_list()
                self.variables['black_illegal_reason'] = violation_df1['illegalreason'].to_list()
                self.variables['black_illegal_datetime'] = violation_df1['specific_date'].to_list()
                self.variables['black_illegal_case_no'] = violation_df1['case_no'].to_list()

    def clean_variables_bus(self):
        resp = get_query_data(self.full_msg, 'COMPANY', '01')
        ids = []
        basic_dict = {"id":[],"ent_name":[]}
        for i in resp:
            user_name = i.get("name")
            id_card_no = i.get("id_card_no")
            court_df = self._info_com_bus_basic(user_name, id_card_no)
            if not court_df.empty:
                ids.append(int(court_df.loc[0, 'id']))
                basic_dict['id'].append(court_df.loc[0, 'id'])
                basic_dict['ent_name'].append(court_df.loc[0, 'ent_name'])

        if len(ids) == 0:
            return

        illegal_df = self._info_com_bus_illegal(ids)
        frost_df = self._info_com_bus_shares_frost(ids)
        case_df = self._info_com_bus_case(ids)

        basic_df = pd.DataFrame(basic_dict)
        if not illegal_df.empty:
            self.variables['black_legel_cnt'] = illegal_df.shape[0]
            merge_df = pd.merge(illegal_df,basic_df,left_on='basic_id',right_on='id',how='left')
            merge_df = merge_df.sort_values(by=['ent_name','illegal_date_in'],ascending=False)
            merge_df['illegal_date_in'] = merge_df['illegal_date_in'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            merge_df['illegal_date_out'] = merge_df['illegal_date_out'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            self.variables['black_legal_name'] = merge_df['ent_name'].to_list()
            self.variables['black_legal_cause'] = merge_df['illegal_result_in'].to_list()
            self.variables['black_legal_date'] = merge_df['illegal_date_in'].to_list()
            self.variables['black_legal_org'] = merge_df['illegal_org_name_in'].to_list()
            self.variables['black_legal_clear_cause'] = merge_df['illegal_rresult_out'].to_list()
            self.variables['black_legal_clear_date'] = merge_df['illegal_date_out'].to_list()
            self.variables['black_legal_clear_org'] = merge_df['illegal_org_name_out'].to_list()

        if not frost_df.empty:
            self.variables['black_froz_cnt'] = frost_df.shape[0]
            frost_df['black_froz_time'] = frost_df.apply(lambda x:trans_black_froz_time(x['froz_from'],x['froz_to']), axis=1)
            frost_df['froz_public_date'] = frost_df['froz_public_date'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            frost_df['thaw_date'] = frost_df['thaw_date'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            frost_df['invalid_time'] = frost_df['invalid_time'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d'))
            self.variables['black_froz_name'] = frost_df['froz_ent'].to_list()
            self.variables['black_froz_role'] = frost_df['jhi_role'].to_list()
            self.variables['black_froz_status'] = frost_df['judicial_froz_state'].to_list()
            self.variables['black_froz_execute_no'] = frost_df['froz_doc_no'].to_list()
            self.variables['black_froz_amt'] = frost_df['judicial_fro_am'].to_list()
            self.variables['black_froz_inv'] = frost_df['judicial_inv'].to_list()
            self.variables['black_froz_auth'] = frost_df['froz_auth'].to_list()
            self.variables['black_froz_public_date'] = frost_df['froz_public_date'].to_list()
            self.variables['black_froz_time'] = frost_df['black_froz_time'].to_list()
            self.variables['black_froz_thaw_date'] = frost_df['thaw_date'].to_list()
            self.variables['black_froz_invalid_date'] = frost_df['invalid_time'].to_list()
            self.variables['black_froz_invalid_reason'] = frost_df['invalid_reason'].to_list()

        if not case_df.empty:
            self.variables['black_punish_cnt'] = case_df.shape[0]
            self.variables['black_punish_name'] = case_df['ent_name'].to_list()
            self.variables['black_punish_reason'] = case_df['illegact_type_name'].to_list()
            self.variables['black_punish_datetime'] = case_df['pen_deciss_date'].apply(
                lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()
            self.variables['black_punish_case_no'] = case_df['pendec_no'].to_list()

    def transform(self):
        self.clean_variables_court()
        self.clean_variables_bus()
