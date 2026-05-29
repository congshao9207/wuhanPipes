# import re
#
# import pandas as pd
# from pandas import DateOffset
# from pandas.tseries import offsets
#
# from src.fileparser.trans_flow.trans_config import *
# from src.util.mysql_reader import sql_to_df
#
#
# def get_trans_u_flow_portrait(no_filter=True):
#
#
#     acc_sql = '''
#             select id as account_id, out_req_no, file_id, trans_flow_src_type, bank, account_no, id_card_no as idno
#             from trans_account
#             where file_id in %(fileids_list)s
#         '''
#     acc_df = sql_to_df(sql=acc_sql, params={"fileids_list": [861, 860]})
#     out_req_no_list = acc_df['out_req_no'].tolist()
#     flow_sql = "select * from trans_report_flow where out_req_no in %s" % f"{tuple(out_req_no_list)}"
#     print(flow_sql)
#     flow_df = sql_to_df(sql=flow_sql)
#     # flow_df = pd.read_excel("结果集1.xlsx")
#     df = pd.merge(flow_df, acc_df, how='left', on='out_req_no')
#
#     if df.shape[0] == 0:
#         return
#     sql = '''
#                 select related_name as name, relationship
#                 from trans_apply
#                 where report_req_no = %(report_req_no)s
#             '''
#     relation_df = sql_to_df(sql=sql, params={"report_req_no": 'PR2033495656869560320'})
#     relation_df.drop_duplicates(subset='name', keep='first', inplace=True)
#     relation_dict = {getattr(row, 'name'): getattr(row, 'relationship') for row in relation_df.itertuples()}
#     # 重新打relationship标签
#     for i, v in relation_dict.items():
#         df.loc[df['opponent_name'].astype(str).str.contains(i, regex=False), 'relationship'] = v
#     # 将码值映射成文字
#     label_sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
#     label_df = sql_to_df(label_sql)
#     res = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in label_df.itertuples()}
#
#     # 成本支出项 水电、工资、保险、税费
#     cost_lab_dict = {'0102010411': '水电', '0102010201': '工资', '0102010402': '保险',
#                      '0102010301': '税费', '0102010302': '税费', '0102010303': '税费', '0102010304': '税费'}
#     df['mutual_exclusion_label'] = df['mutual_exclusion_label'].fillna('')
#     df['cost_type'] = df['mutual_exclusion_label'].map(lambda x: cost_lab_dict[x] if x in cost_lab_dict.keys() else '')
#     df['remark_type'] = ''
#     df['trans_time'] = pd.to_datetime(df['trans_time'])
#     df['trans_date'] = df['trans_time'].apply(lambda x: x.date())
#     df['label1'] = df['mutual_exclusion_label'].map(res)
#     # df['uni_type'] = df['mutual_exclusion_label'].apply(lambda x: x[4:8])
#     df['mutual_exclusion_label'] = df['mutual_exclusion_label'].fillna('').astype(str)
#     df['uni_type'] = df['mutual_exclusion_label'].str[4:8]
#     df['usual_trans_type'] = df['compatibility_label'].apply(
#         lambda x: ','.join([str(res.get(y)) for y in x.split(',')]) if pd.notna(x) else '')
#     df['unusual_trans_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0203' else None, axis=1)
#     df['loan_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0202' else None, axis=1)
#     df['is_sensitive'] = df.apply(
#         lambda x: 1 if pd.notna(x['unusual_trans_type']) or pd.notna(x['loan_type']) else None, axis=1)
#     df['opponent_type'] = df['opponent_name'].fillna('').astype(str).apply(_opponent_type)
#     df['trans_flow_src_type'] = df['trans_flow_src_type'].apply(lambda x: 1 if x in [2, 3] else 0)
#     df = _in_out_order(df)
#     df = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]
#     if not no_filter and df.shape[0] > 0:
#         df = df[df['trans_time'] >= df['trans_time'].max() - offsets.DateOffset(months=12)]
#     trans_u_flow_portrait = df if df.shape[0] > 0 else None
#     return trans_u_flow_portrait
#
# def _opponent_type(op_name):
#     if len(op_name) > 6 and re.search(ENT_TYPE, op_name) is not None:
#         return 2
#     else:
#         if len(op_name) <= 15:
#             cleaned_name = re.sub(TYPE_EXCEPT_1, '', op_name)
#             if re.match(TYPE_START_1, cleaned_name):
#                 cleaned_name = re.sub(TYPE_EXCEPT_2, '', cleaned_name)
#             elif re.match(TYPE_START_2, cleaned_name):
#                 cleaned_name = cleaned_name.split()[-1]
#             else:
#                 cleaned_name = re.sub(r' ', '', cleaned_name)
#             if 2 <= len(cleaned_name) <= 3:
#                 if re.search(TYPE_EXCEPT_3, cleaned_name) is None and \
#                         re.match(TYPE_EXCEPT_4, cleaned_name) is None:
#                     return 1
#
# def _in_out_order(df):
#     df = df.assign(income_cnt_order=None, income_amt_order=None, expense_cnt_order=None, expense_amt_order=None)
#     income_per_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt > 0) &
#                        (df.opponent_type == 1) & (pd.isna(df.loan_type)) &
#                        (pd.isna(df.unusual_trans_type)) &
#                        (~df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
#                        (~df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
#     expense_per_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt < 0) &
#                         (df.opponent_type == 1) & (pd.isna(df.loan_type)) &
#                         (pd.isna(df.unusual_trans_type)) &
#                         (~df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP))) &
#                         (~df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
#     income_com_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt > 0) &
#                        (df.opponent_type == 2) & (pd.isna(df.loan_type)) &
#                        (pd.isna(df.unusual_trans_type))]
#     income_com_df = income_com_df[
#         (~income_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
#         (~income_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
#     expense_com_df = df[(pd.notnull(df.opponent_name)) & (df.trans_amt < 0) &
#                              (df.opponent_type == 2) & (pd.isna(df.loan_type)) & (
#                                  pd.isna(df.unusual_trans_type))]
#     expense_com_df = expense_com_df[
#         (~expense_com_df.opponent_name.astype(str).str.contains('|'.join(UNUSUAL_OPPO_NAME))) &
#         (~expense_com_df.relationship.astype(str).str.contains('|'.join(STRONGER_RELATIONSHIP)))]
#     income_per_cnt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     income_per_amt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     expense_per_cnt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     expense_per_amt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
#         sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
#     income_com_cnt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     income_com_amt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     expense_com_cnt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
#         sort_values(by='trans_amt', ascending=False).index.tolist()[:20]
#     expense_com_amt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
#         sort_values(by='trans_amt', ascending=True).index.tolist()[:20]
#     for i in range(len(income_per_cnt_list)):
#         df.loc[df['opponent_name'] == income_per_cnt_list[i], 'income_cnt_order'] = i + 1
#     for i in range(len(income_com_cnt_list)):
#         df.loc[df['opponent_name'] == income_com_cnt_list[i], 'income_cnt_order'] = i + 1
#     for i in range(len(expense_per_cnt_list)):
#         df.loc[df['opponent_name'] == expense_per_cnt_list[i], 'expense_cnt_order'] = i + 1
#     for i in range(len(expense_com_cnt_list)):
#         df.loc[df['opponent_name'] == expense_com_cnt_list[i], 'expense_cnt_order'] = i + 1
#     for i in range(len(income_per_amt_list)):
#         df.loc[df['opponent_name'] == income_per_amt_list[i], 'income_amt_order'] = i + 1
#     for i in range(len(income_com_amt_list)):
#         df.loc[df['opponent_name'] == income_com_amt_list[i], 'income_amt_order'] = i + 1
#     for i in range(len(expense_per_amt_list)):
#         df.loc[df['opponent_name'] == expense_per_amt_list[i], 'expense_amt_order'] = i + 1
#     for i in range(len(expense_com_amt_list)):
#         df.loc[df['opponent_name'] == expense_com_amt_list[i], 'expense_amt_order'] = i + 1
#     return df
#
# def bank_account_summary():
#     """
#     分账户汇总经营性收入和支出，以及分账户按月汇总经营性收入和支出
#     :return:
#     """
#     variables = {'trans_report_fullview': {'bank_account_summary': {}}}
#     account_summary = []
#     account_monthly_summary = []
#     all_df = get_trans_u_flow_portrait()
#     print(all_df["trans_date"])
#     all_df = get_u_flow_portrait_detail(all_df)
#     if not all_df.empty:
#         # 筛选经营性数据：剔除多头、特殊、关联关系
#         normal_df = all_df.loc[
#             pd.isna(all_df.loan_type)
#             & pd.isna(all_df.unusual_trans_type)
#             & pd.isna(all_df.relationship)
#         ]
#         if not normal_df.empty:
#             # 分别计算进账和出账
#             income_df = normal_df[normal_df.trans_amt >= 0]
#             expense_df = normal_df[normal_df.trans_amt < 0]
#
#             # ========== 1. 分账户汇总经营性收入和支出 ==========
#             income_by_account = income_df.groupby('account_id').agg(
#                 {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
#             expense_by_account = expense_df.groupby('account_id').agg(
#                 {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})
#             account_agg_df = income_by_account.join(expense_by_account, how='outer').fillna(0).reset_index()
#             account_agg_df['net_income_amt'] = account_agg_df['normal_income_amt'] - account_agg_df[
#                 'normal_expense_amt']
#
#             # 计算各账户收入和支出占比
#             total_income = account_agg_df['normal_income_amt'].sum()
#             total_expense = account_agg_df['normal_expense_amt'].sum()
#             account_agg_df['income_proportion'] = round(
#                 account_agg_df['normal_income_amt'] / total_income, 4) if total_income > 0 else 0
#             account_agg_df['expense_proportion'] = round(
#                 account_agg_df['normal_expense_amt'] / total_expense, 4) if total_expense > 0 else 0
#
#             # 匹配账户详情
#             account_id_list = account_agg_df['account_id'].tolist()
#             account_detail_df = get_trans_account_detail(account_id_list)
#             account_agg_df = account_agg_df.merge(
#                 account_detail_df[['id', 'account_name', 'bank', 'account_no']],
#                 how='left', left_on='account_id', right_on='id')
#             # 同一账户可能存在多条记录（不同file_id），需合并
#             account_agg_df = account_agg_df.groupby(
#                 ['account_name', 'bank', 'account_no'], as_index=False).agg(
#                 {'normal_income_amt': 'sum', 'normal_expense_amt': 'sum',
#                  'net_income_amt': 'sum', 'income_proportion': 'sum',
#                  'expense_proportion': 'sum'})
#             # 银行账号保留后4位
#             account_agg_df['account_no'] = account_agg_df['account_no'].astype(str).str[-4:]
#             # 金额保留2位小数
#             for col in ['normal_income_amt', 'normal_expense_amt', 'net_income_amt']:
#                 account_agg_df[col] = account_agg_df[col].apply(lambda x: round(x, 2))
#             account_agg_df.rename(columns={
#                 'account_name': 'user_name', 'bank': 'bank_name', 'account_no': 'bank_no'
#             }, inplace=True)
#             account_summary = account_agg_df[
#                 ['user_name', 'bank_name', 'bank_no', 'normal_income_amt', 'normal_expense_amt',
#                  'net_income_amt', 'income_proportion', 'expense_proportion']].to_dict('records')
#
#             # ========== 2. 分账户按月汇总经营性收入和支出 ==========
#             income_by_acct_month = income_df.groupby(['account_id', 'trans_month']).agg(
#                 {'trans_amt': 'sum'}).rename(columns={'trans_amt': 'normal_income_amt'})
#             expense_by_acct_month = expense_df.groupby(['account_id', 'trans_month']).agg(
#                 {'trans_amt': lambda x: x.abs().sum()}).rename(columns={'trans_amt': 'normal_expense_amt'})
#             monthly_df = income_by_acct_month.join(
#                 expense_by_acct_month, how='outer').fillna(0).reset_index()
#             monthly_df['net_income_amt'] = monthly_df['normal_income_amt'] - monthly_df['normal_expense_amt']
#
#             # 匹配账户详情
#             monthly_df = monthly_df.merge(
#                 account_detail_df[['id', 'account_name', 'bank', 'account_no']],
#                 how='left', left_on='account_id', right_on='id')
#             # 同一账户同月合并
#             monthly_df = monthly_df.groupby(
#                 ['account_name', 'bank', 'account_no', 'trans_month'], as_index=False).agg(
#                 {'normal_income_amt': 'sum', 'normal_expense_amt': 'sum',
#                  'net_income_amt': 'sum'})
#             monthly_df['account_no'] = monthly_df['account_no'].astype(str).str[-4:]
#             for col in ['normal_income_amt', 'normal_expense_amt', 'net_income_amt']:
#                 monthly_df[col] = monthly_df[col].apply(lambda x: round(x, 2))
#             monthly_df.sort_values(['account_name', 'bank', 'account_no', 'trans_month'], inplace=True)
#             monthly_df.rename(columns={
#                 'account_name': 'user_name', 'bank': 'bank_name', 'account_no': 'bank_no'
#             }, inplace=True)
#             account_monthly_summary = monthly_df[
#                 ['user_name', 'bank_name', 'bank_no', 'trans_month',
#                  'normal_income_amt', 'normal_expense_amt', 'net_income_amt']].to_dict('records')
#
#     variables['trans_report_fullview']['bank_account_summary'] = {
#         'account_summary': account_summary,
#         'account_monthly_summary': account_monthly_summary
#     }
#     print(variables)
#
#
# def get_trans_account_detail(id_list):
#     sql = """select * from trans_account where id in %(id_list)s"""
#     df = sql_to_df(sql=sql, params={"id_list": id_list})
#     return df
#
# def get_u_flow_portrait_detail(df):
#     # 取联合版块的画像数据
#     df = df.copy()
#     if df.shape[0] == 0:
#         return pd.DataFrame()
#     # 获取一年前的日期
#     year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
#     # 新增交易年-月列
#     df['trans_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
#     # 筛选近一年数据
#     df = df.loc[pd.to_datetime(df.trans_date) >= year_ago]
#     if df.shape[0] == 0:
#         return pd.DataFrame()
#     return df
#
# bank_account_summary()