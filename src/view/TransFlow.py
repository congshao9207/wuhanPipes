from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df
from pandas.tseries import offsets
from fileparser.trans_flow.trans_config import *
import pandas as pd
import re


class TransFlow(ModuleProcessor):

    def __init__(self):
        super().__init__()
        # self.db = self._db()
        self.account_id = None
        self.cusName = None
        self.bankName = None
        self.bankAccount = None
        self.idno = None
        self.reqno = None
        self.appAmt = None
        self.previous_out_apply_no = None
        self.guarantor_list = []
        self.file_ids = []
        self.variables = {}
        self.trans_u_flow_portrait = None
        self.mtr_trans_flow_portrait = None
        self.relation_dict = {}
        self.file_df = pd.DataFrame()
        self.file_status = True
        self.main_indu = ''
        self.main_indu_name = ''
        self.main_ent = {}
        self.indu_flow = None

    def init(self, variables, user_name, id_card_no, origin_data, cached_data):
        super().init(variables, user_name, id_card_no, origin_data, cached_data)
        guarantor_temp = []
        ent_list = []
        for i in self.cached_data.get("input_param"):
            temp_name = i.get('name')
            temp_idno = i.get('idno')
            temp_file_df = pd.DataFrame(i.get('extraParam').get('fileInfo'))
            # 20250326 调整为取行业代码
            temp_indu = i.get('extraParam').get('industry')
            temp_indu_name = i.get('extraParam').get('industryName')
            is_main = i.get('extraParam').get('isMain')
            if is_main == '1':
                self.main_indu = temp_indu
                self.main_indu_name = temp_indu_name
                # 20250407调整，仅展示主营企业信息
                ent_list.append({'name': temp_name, 'idno': temp_idno, 'indu': temp_indu, 'is_main': is_main})
            # 新增判断，仅获取企业信息
            # if i.get('userType') == 'COMPANY':
            #     ent_list.append({'name': temp_name, 'idno': temp_idno, 'indu': temp_indu, 'is_main': is_main})
            if temp_file_df.shape[0] > 0:
                temp_file_df['userName'] = temp_file_df['ownerName'].apply(
                    lambda x: "未取得" if pd.isna(x) or x == "" else f"{x}(一致)" if x == temp_name else f"{x}(不一致)")
                if self.file_status and temp_file_df[temp_file_df['userName'].str.contains('不一致')].shape[0] > 0:
                    self.file_status = False
                temp_file_df = temp_file_df.groupby(by=['bankName', 'bankAccount'], as_index=False).agg({
                    'fileName': pd.Series.tolist, 'contentId': pd.Series.tolist,
                    'uploadDate': pd.Series.tolist, 'userName': pd.Series.tolist})
                temp_file_df['id_card_no'] = temp_idno
                self.file_df = pd.concat([self.file_df, temp_file_df], axis=0, ignore_index=True)
            if i["relation"] == "MAIN":
                self.cusName = i["name"]
                if len(i["extraParam"]["accounts"]) > 0:
                    self.bankName = i["extraParam"]["accounts"][0]["bankName"]
                    self.bankAccount = i["extraParam"]["accounts"][0]["bankAccount"]
                self.idno = i["idno"]
                self.reqno = i["preReportReqNo"]
                self.appAmt = i["applyAmo"]

                sql = """
                    select account_id
                    from trans_apply ap
                    left join 
                    trans_account ac
                    on ap.account_id = ac.id
                    where ap.report_req_no = %(report_req_no)s 
                    and ap.id_card_no = %(id_card_no)s 
                    and ac.account_no = %(account_no)s
                """
                account_df = sql_to_df(sql=sql,
                                       params={"report_req_no": self.reqno,
                                               "id_card_no": self.idno,
                                               "account_no": self.bankAccount})
                if not account_df.empty:
                    self.account_id = int(account_df.values[0][0])

            if i["relation"] in ["GUARANTOR", "GUAR_PER", "GUAR_ENT"]:
                guarantor_temp.append(i["name"])

            file_ids = i["extraParam"].get("fileIds", None)
            if file_ids is not None:
                self.file_ids.extend(file_ids)

        # 原列表推导式改为字典推导式
        self.main_ent = {ent['idno']: ent['is_main'] for ent in ent_list if ent['indu'] != ""}
        self.previous_out_apply_no = self.cached_data.get("previous_out_apply_no")

        sql = '''
            select related_name as name, relationship
            from trans_apply
            where report_req_no = %(report_req_no)s
        '''
        relation_df = sql_to_df(sql=sql, params={"report_req_no": self.reqno})
        relation_df.drop_duplicates(subset='name', keep='first', inplace=True)
        self.relation_dict = {getattr(row, 'name'): getattr(row, 'relationship') for row in relation_df.itertuples()}
        relation_list = relation_df[~relation_df['relationship'].isin(
            ['U_PER_GUARANTOR_PERSONAL', 'U_PER_GUARANTOR_COMPANY', 'U_COM_GUARANTOR_PERSONAL',
             'U_COM_GUARANTOR_COMPANY'])]['name'].unique().tolist()

        for name in guarantor_temp:
            if name not in relation_list:
                self.guarantor_list.append(name)
        self.get_trans_u_flow_portrait(no_filter=True)
        # 收单流水处理
        self.get_mtr_trans_flow_portrait(no_filter=True)

        # 统一计算最大时间
        max_time = self._get_global_max_time()

        # 统一应用时间筛选
        if self.trans_u_flow_portrait is not None:
            self.trans_u_flow_portrait = self.trans_u_flow_portrait[
                self.trans_u_flow_portrait['trans_time'] >= max_time - offsets.DateOffset(months=12)]
        if self.mtr_trans_flow_portrait is not None:
            self.mtr_trans_flow_portrait = self.mtr_trans_flow_portrait[
                self.mtr_trans_flow_portrait['trans_time'] >= max_time - offsets.DateOffset(months=12)]

    def get_trans_u_flow_portrait(self, no_filter=False):
        input_param = self.cached_data.get("input_param")
        fileids_list = []
        for i in input_param:
            fileids_list.extend(i["extraParam"]['fileIds'])

        acc_sql = '''
                select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no, id_card_no as idno
                from trans_account 
                where file_id in %(fileids_list)s
            '''
        acc_df = sql_to_df(sql=acc_sql, params={"fileids_list": fileids_list})
        out_req_no_list = acc_df['out_req_no'].tolist()
        flow_sql = "select * from trans_report_flow where out_req_no in (%s)" % ('"' + '","'.join(out_req_no_list) + '"')
        flow_df = sql_to_df(sql=flow_sql)
        df = pd.merge(flow_df, acc_df, how='left', on='out_req_no')

        if df.shape[0] == 0:
            return
        # 重新打relationship标签
        for i, v in self.relation_dict.items():
            df.loc[df['opponent_name'].astype(str).str.contains(i, regex=False), 'relationship'] = v
        # 将码值映射成文字
        label_sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
        label_df = sql_to_df(label_sql)
        res = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in label_df.itertuples()}

        # 成本支出项 水电、工资、保险、税费
        cost_lab_dict = {'0102010411': '水电', '0102010201': '工资', '0102010402': '保险',
                         '0102010301': '税费', '0102010302': '税费', '0102010303': '税费', '0102010304': '税费'}
        df['mutual_exclusion_label'] = df['mutual_exclusion_label'].fillna('')
        df['cost_type'] = df['mutual_exclusion_label'].map(lambda x: cost_lab_dict[x] if x in cost_lab_dict.keys() else '')
        df['remark_type'] = ''
        df['trans_time'] = pd.to_datetime(df['trans_time'])
        df['trans_date'] = df['trans_time'].apply(lambda x: x.date())
        df['label1'] = df['mutual_exclusion_label'].map(res)
        # df['uni_type'] = df['mutual_exclusion_label'].apply(lambda x: x[4:8])
        df['uni_type'] = df['mutual_exclusion_label'].str[4:8]
        df['usual_trans_type'] = df['compatibility_label'].apply(
            lambda x: ','.join([str(res.get(y)) for y in x.split(',')]) if pd.notna(x) else '')
        df['unusual_trans_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0203' else None, axis=1)
        df['loan_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0202' else None, axis=1)
        df['is_sensitive'] = df.apply(
            lambda x: 1 if pd.notna(x['unusual_trans_type']) or pd.notna(x['loan_type']) else None, axis=1)
        df['opponent_type'] = df['opponent_name'].fillna('').astype(str).apply(self._opponent_type)
        df['trans_flow_src_type'] = df['trans_flow_src_type'].apply(lambda x: 1 if x in [2, 3] else 0)
        df = self._in_out_order(df)
        df = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]
        if not no_filter and df.shape[0] > 0:
            df = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]
        self.trans_u_flow_portrait = df if df.shape[0] > 0 else None
        self.indu_flow = df[df['idno'].isin(self.main_ent)]

    def get_mtr_trans_flow_portrait(self, no_filter=False):
        """
        收单流水处理
        :return:
        """
        input_param = self.cached_data.get("input_param")
        fileids_list = []
        for i in input_param:
            fileids_list.extend(i["extraParam"]['fileIds'])

        acc_sql = '''
                select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no
                from trans_account 
                where file_id in %(fileids_list)s
            '''
        acc_df = sql_to_df(sql=acc_sql, params={"fileids_list": fileids_list})
        out_req_no_list = acc_df['out_req_no'].tolist()
        flow_sql = "select * from mtr_trans_flow where out_req_no in (%s)" % ('"' + '","'.join(out_req_no_list) + '"')
        flow_df = sql_to_df(sql=flow_sql)
        df = pd.merge(flow_df, acc_df, how='left', on='out_req_no')

        if df.shape[0] == 0:
            return

        df['trans_status_label'] = df['trans_status'].fillna('').str.contains('退货|取消|部分退|合单|作废').astype(int)
        if not no_filter and df.shape[0] > 0:
            self.mtr_trans_flow_portrait = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]

        self.mtr_trans_flow_portrait = df if df.shape[0] > 0 else None

    def _get_global_max_time(self):
        """获取全局最大时间"""
        times = []
        if self.trans_u_flow_portrait is not None and not self.trans_u_flow_portrait.empty:
            times.append(self.trans_u_flow_portrait['trans_time'].max())
        if self.mtr_trans_flow_portrait is not None and not self.mtr_trans_flow_portrait.empty:
            times.append(self.mtr_trans_flow_portrait['trans_time'].max())
        return max(times)

    @staticmethod
    def _opponent_type(op_name):
        if len(op_name) > 6 and re.search(ENT_TYPE, op_name) is not None:
            return 2
        else:
            if len(op_name) <= 15:
                cleaned_name = re.sub(TYPE_EXCEPT_1, '', op_name)
                if re.match(TYPE_START_1, cleaned_name):
                    cleaned_name = re.sub(TYPE_EXCEPT_2, '', cleaned_name)
                elif re.match(TYPE_START_2, cleaned_name):
                    cleaned_name = cleaned_name.split()[-1]
                else:
                    cleaned_name = re.sub(r' ', '', cleaned_name)
                if 2 <= len(cleaned_name) <= 3:
                    if re.search(TYPE_EXCEPT_3, cleaned_name) is None and \
                            re.match(TYPE_EXCEPT_4, cleaned_name) is None:
                        return 1

    @staticmethod
    def _in_out_order(df):
        df = df.assign(income_cnt_order=None, income_amt_order=None, expense_cnt_order=None, expense_amt_order=None)
        income_per_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt > 0) &
                           (df.opponent_type == 1) & (pd.isna(df.loan_type)) &
                           (pd.isna(df.unusual_trans_type)) &
                           (~df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
                           (~df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
        expense_per_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt < 0) &
                            (df.opponent_type == 1) & (pd.isna(df.loan_type)) &
                            (pd.isna(df.unusual_trans_type)) &
                            (~df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
                            (~df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
        income_com_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt > 0) &
                           (df.opponent_type == 2) & (pd.isna(df.loan_type)) &
                           (pd.isna(df.unusual_trans_type))]
        income_com_df = income_com_df[
            (~income_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~income_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        expense_com_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt < 0) &
                                 (df.opponent_type == 2) & (pd.isna(df.loan_type)) & (
                                     pd.isna(df.unusual_trans_type))]
        expense_com_df = expense_com_df[
            (~expense_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
            (~expense_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
        income_per_cnt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        income_per_amt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_per_cnt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_per_amt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
        income_com_cnt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        income_com_amt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_com_cnt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
        expense_com_amt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
        for i in range(len(income_per_cnt_list)):
            df.loc[df['opponent_name'] == income_per_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(income_com_cnt_list)):
            df.loc[df['opponent_name'] == income_com_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(expense_per_cnt_list)):
            df.loc[df['opponent_name'] == expense_per_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(expense_com_cnt_list)):
            df.loc[df['opponent_name'] == expense_com_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(income_per_amt_list)):
            df.loc[df['opponent_name'] == income_per_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(income_com_amt_list)):
            df.loc[df['opponent_name'] == income_com_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(expense_per_amt_list)):
            df.loc[df['opponent_name'] == expense_per_amt_list[i], 'expense_amt_order'] = i + 1
        for i in range(len(expense_com_amt_list)):
            df.loc[df['opponent_name'] == expense_com_amt_list[i], 'expense_amt_order'] = i + 1
        return df

    @staticmethod
    def flow_account_clean(account_no):
        account_no_lenth = len(account_no)
        if account_no_lenth >= 7:
            return "***" + account_no[-4:]
        if account_no_lenth >= 11:
            return account_no[:4] + "***" + account_no[-4:]
            # 7位以内的未脱敏处理， 一般不会出现此类银行卡号
        return account_no
