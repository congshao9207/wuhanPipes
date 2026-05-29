from mapping.module_processor import ModuleProcessor
import pandas as pd
import numpy as np
from pandas.tseries import offsets
from util.mysql_reader import sql_to_df


class TransModuleProcessor(ModuleProcessor):

    def __init__(self):
        super().__init__()
        self.reqno = None
        self.trans_u_flow_portrait = None
        self.mtr_trans_flow_portrait = None
        self.trans_u_summary_portrait = None
        self.trans_u_portrait = None
        self.trans_u_loan_portrait = None
        self.trans_flow_portrait = None
        self.trans_single_summary_portrait = None
        self.credit_df = pd.DataFrame()

    def init(self, variables, user_name, id_card_no, origin_data, cached_data):
        super().init(variables, user_name, id_card_no, origin_data, cached_data)

        self.reqno = self.cached_data.get("input_param")[0]["preReportReqNo"]
        self.variables['apply_amt'] = self.cached_data.get("input_param")[0]["applyAmo"]

        input_param = self.cached_data.get("input_param")
        fileids_list = []
        for i in input_param:
            fileids_list.extend(i["extraParam"]['fileIds'])

        # P0: 复用 TransFlow 已缓存的流水数据，避免重复查询 + 重复加工
        if '_cached_trans_u_flow' in self.cached_data:
            self.trans_u_flow_portrait = self.cached_data['_cached_trans_u_flow'].copy() if self.cached_data['_cached_trans_u_flow'] is not None else None
        else:
            # ---- 原有的 trans_report_flow 查询逻辑 ----
            # acc_sql = '''
            #     select id as account_id, out_req_no, file_id, trans_flow_src_type
            #     from trans_account
            #     where file_id in %(fileids_list)s
            # '''
            # acc_df = sql_to_df(sql=acc_sql, params={"fileids_list": fileids_list})
            # out_req_no_list = acc_df['out_req_no'].tolist()
            # flow_sql = "select * from trans_report_flow where out_req_no in (%s)" % ('"' + '","'.join(out_req_no_list) + '"')
            # flow_df = sql_to_df(sql=flow_sql)
            # sql = """
            #     SELECT trf.*
            #     FROM trans_report_flow trf
            #     INNER JOIN trans_account ta ON trf.out_req_no = ta.out_req_no
            #     WHERE ta.file_id IN %(fileids_list)s
            # """
            # flow_df = sql_to_df(sql, params={"fileids_list": fileids_list})
            # df = pd.merge(flow_df, acc_df, how='left', on='out_req_no')
            # 查询包含 bank / account_no / idno 列，确保缓存与 TransFlow 兼容
            acc_sql = '''
                select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no, id_card_no as idno
                from trans_account
                where file_id in %(fileids_list)s
            '''
            acc_df = sql_to_df(sql=acc_sql, params={"fileids_list": fileids_list})
            sql = """
                SELECT trf.*
                FROM trans_report_flow trf
                INNER JOIN trans_account ta ON trf.out_req_no = ta.out_req_no
                WHERE ta.file_id IN %(fileids_list)s
            """
            flow_df = sql_to_df(sql, params={"fileids_list": fileids_list})
            df = pd.merge(flow_df, acc_df, how='left', on='out_req_no')

            # P0: label_logic 全局缓存复用
            if '_label_dict' not in self.cached_data:
                # ---- 原有的 label_logic 查询逻辑 ----
                # label_sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
                # label_df = sql_to_df(label_sql)
                # res = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in label_df.itertuples()}
                label_sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
                label_df = sql_to_df(label_sql)
                self.cached_data['_label_dict'] = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in label_df.itertuples()}
            res = self.cached_data['_label_dict']

            # ---- 原有的数据加工逻辑 ----
            # # 将码值映射成文字
            # label_sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
            # label_df = sql_to_df(label_sql)
            # res = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in label_df.itertuples()}

            # 成本支出项 水电、工资、保险、税费
            cost_lab_dict = {
                '0102010411': '水电', '0102010201': '工资', '0102010402': '保险',
                '0102010301': '税费', '0102010302': '税费', '0102010303': '税费', '0102010304': '税费'}
            df['cost_type'] = df['mutual_exclusion_label'].map(lambda x: cost_lab_dict[x] if x in cost_lab_dict.keys() else '')
            df['remark_type'] = ''
            df['trans_time'] = pd.to_datetime(df['trans_time'])
            # df['trans_date'] = df['trans_time'].apply(lambda x: x.date())
            df['trans_date'] = df['trans_time'].dt.date
            # df['trans_time'] = df['trans_time'].apply(lambda x: format(x, '%H:%M:%S'))
            df['relationship'] = ''
            df['label1'] = df['mutual_exclusion_label'].map(res)
            # df['uni_type'] = df['mutual_exclusion_label'].apply(lambda x: x[4:8])
            df['uni_type'] = df['mutual_exclusion_label'].str[4:8]
            df['usual_trans_type'] = df['compatibility_label'].apply(
                lambda x: ','.join([str(res.get(y)) for y in x.split(',')]) if pd.notna(x) else '')
            # P1: apply(axis=1) → 向量化操作
            # ---- 原有的 apply 方式 ----
            # df['unusual_trans_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0203' else None, axis=1)
            # df['loan_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0202' else None, axis=1)
            # df['is_sensitive'] = df.apply(
            #     lambda x: 1 if pd.notna(x['unusual_trans_type']) or pd.notna(x['loan_type']) else None, axis=1)
            df['unusual_trans_type'] = np.where(df['uni_type'] == '0203', df['label1'], None)
            df['loan_type'] = np.where(df['uni_type'] == '0202', df['label1'], None)
            df['is_sensitive'] = np.where(
                df['unusual_trans_type'].notna() | df['loan_type'].notna(), 1, None)

            self.trans_u_flow_portrait = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]
            # TransModuleProcessor 不写入 _cached_trans_u_flow 缓存，
            # 由 TransFlow 独占写入（含 opponent_type / _in_out_order 等全部加工列）

        # P0: 复用 TransFlow 已缓存的收单流水数据
        if '_cached_mtr_trans_flow' in self.cached_data:
            self.mtr_trans_flow_portrait = self.cached_data['_cached_mtr_trans_flow'].copy() if self.cached_data['_cached_mtr_trans_flow'] is not None else None
        else:
            # ---- 原有的收单流水查询逻辑 ----
            # mtr_acc_sql = '''
            #                 select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no
            #                 from trans_account
            #                 where file_id in %(fileids_list)s
            #             '''
            # mtr_acc_df = sql_to_df(sql=mtr_acc_sql, params={"fileids_list": fileids_list})
            # mtr_out_req_no_list = mtr_acc_df['out_req_no'].tolist()
            # mtr_flow_sql = "select * from mtr_trans_flow where out_req_no in (%s)" % ('"' + '","'.join(mtr_out_req_no_list) + '"')
            # mtr_flow_df = sql_to_df(sql=mtr_flow_sql)
            # mtr_df = pd.merge(mtr_flow_df, mtr_acc_df, how='left', on='out_req_no')
            # mtr_df['trans_time'] = pd.to_datetime(mtr_df['trans_time'])
            # mtr_df['trans_date'] = mtr_df['trans_time'].apply(lambda x: x.date())
            # self.mtr_trans_flow_portrait = mtr_df[mtr_df['trans_time'] >= mtr_df['trans_time'].max() - offsets.DateOffset(months=12)]
            mtr_acc_sql = '''
                            select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no
                            from trans_account
                            where file_id in %(fileids_list)s
                        '''
            mtr_acc_df = sql_to_df(sql=mtr_acc_sql, params={"fileids_list": fileids_list})
            mtr_out_req_no_list = mtr_acc_df['out_req_no'].tolist()
            mtr_flow_sql = "select * from mtr_trans_flow where out_req_no in (%s)" % ('"' + '","'.join(mtr_out_req_no_list) + '"')
            mtr_flow_df = sql_to_df(sql=mtr_flow_sql)
            mtr_df = pd.merge(mtr_flow_df, mtr_acc_df, how='left', on='out_req_no')
            mtr_df['trans_time'] = pd.to_datetime(mtr_df['trans_time'])
            # mtr_df['trans_date'] = mtr_df['trans_time'].apply(lambda x: x.date())
            mtr_df['trans_date'] = mtr_df['trans_time'].dt.date
            self.mtr_trans_flow_portrait = mtr_df[mtr_df['trans_time'] >= mtr_df['trans_time'].max() - offsets.DateOffset(months=12)]
            self.cached_data['_cached_mtr_trans_flow'] = self.mtr_trans_flow_portrait.copy() if self.mtr_trans_flow_portrait is not None else None

        sql = """
            select *
            from trans_u_summary_portrait
            where report_req_no = %(report_req_no)s
        """
        self.trans_u_summary_portrait = sql_to_df(sql=sql,
                                                  params={"report_req_no": self.reqno})

        sql = """
            select *
            from trans_u_portrait
            where report_req_no = %(report_req_no)s
        """
        self.trans_u_portrait = sql_to_df(sql=sql,
                                          params={"report_req_no": self.reqno})

        sql = """
            select *
            from trans_u_loan_portrait
            where report_req_no = %(report_req_no)s
        """
        self.trans_u_loan_portrait = sql_to_df(sql=sql,
                                               params={"report_req_no": self.reqno})

        sql = """
                select *
                from trans_single_summary_portrait
                where report_req_no = %(report_req_no)s
            """
        self.trans_single_summary_portrait = sql_to_df(sql=sql,
                                                       params={"report_req_no": self.reqno})

    def process(self):
        pass
