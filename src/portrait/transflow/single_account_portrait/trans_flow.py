import datetime

import pandas as pd
# TODO eval 动态导入，不能删除下面的导入。
from app import logger, sql_db, sql_session
from portrait.transflow.single_account_portrait.models import TransAccount, TransFlow, TransFlowPortrait, \
    TransSinglePortrait, TransSingleSummaryPortrait, TransSingleRemarkPortrait, TransSingleCounterpartyPortrait, \
    TransSingleRelatedPortrait, TransSingleLoanPortrait, TransApply, TransUFlowPortrait, TransULoanPortrait, \
    TransUModelling, TransUPortrait, TransUCounterpartyPortrait, TransURelatedPortrait, TransURemarkPortrait, \
    TransUSummaryPortrait, TransFlowException
from portrait.transflow.single_account_portrait.trans_config import *
from util.mysql_reader import sql_to_df
from pandas.tseries import offsets
import re
from sqlalchemy import text


def months_ago(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date.date()
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day).date()


def months_ago_datetime(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day)


# def transform_class_str(params, class_name):
#     func_str = class_name + '('
#     for k, v in params.items():
#         if v is not None and v != '':
#             func_str += k + "='" + str(v) + "',"
#     func_str = func_str[:-1]
#     func_str += ')'
#     value = eval(func_str)
#     return value


def transform_class_str(params, class_name):
    f = eval(class_name + "()")
    col_list = [x for x in dir(f) if not x.startswith("_") and x not in ['id', 'metadata', 'registry']]
    start = f"insert into {f.__tablename__}({','.join(col_list)}) values "

    def sql_values(col_val):
        vals = []
        for col in col_list:
            if col in col_val and pd.notna(col_val[col]):
                vals.append(re.sub(r"(?<![\da-zA-Z]):", '-', f"'{col_val[col]}'"))
            else:
                vals.append('null')
        return f"({','.join(vals)})"
    insert_list = [start + ','.join([sql_values(params[j]) for j in range(i, min(i + 1000, len(params)))])
                   for i in range(0, len(params), 1000)]
    db = sql_session
    try:
        for ins in insert_list:
            db.session.execute(text(ins))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.info(f"库表{f.__tablename__}写入数据失败，失败原因{e}")
    return insert_list


class TransFlowBasic:

    def __init__(self, portrait):
        super().__init__()
        self.trans_flow_df = None
        self.account_id = None
        # # 限制上传时间在3个月内的流水会生成画像表,后续可配置
        # self.month_interval = 3
        self.object_k = 0
        self.object_nums = len(portrait.query_data_array)
        self.object_k_k = 0
        self.user_name = portrait.user_name
        self.query_data_array = portrait.query_data_array
        self.report_req_no = portrait.public_param.get('reportReqNo')
        self.app_no = portrait.public_param.get('outApplyNo')
        self.trans_flow_portrait_df = None
        self.trans_flow_portrait_df_2_years = None
        self.trans_u_flow_df = None
        self.trans_u_flow_portrait_df = None
        self.trans_u_flow_portrait_df_2_years = None
        self.db = portrait.sql_db
        self.user_type = None
        self.product_code = portrait.public_param.get('productCode')
        self.portrait_status = True
        self.label_tree = portrait.cached_data.get('label_tree')

    def process(self):
        data = self.query_data_array[self.object_k]
        bank_account = None
        bank_name = None
        fileids = []
        self.user_name = data.get('name')
        id_card_no = data.get('idno')
        self.user_type = data.get('userType')
        if data.__contains__('extraParam') and data['extraParam'].__contains__('accounts') and \
                data['extraParam']['accounts'][self.object_k_k].__contains__('bankAccount'):
            bank_account = data['extraParam']['accounts'][self.object_k_k]['bankAccount']
            bank_name = data['extraParam']['accounts'][self.object_k_k]['bankName']
        if data.__contains__('extraParam') and data['extraParam'].__contains__('fileIds'):
            fileids = data['extraParam']['fileIds']

        # 若为担保人，跳过
        # 流水报告2.0 担保人不跳过，20220903 修改为担保人，跳过
        if data.get('relation') == 'GUARANTOR':
            return

        # 若关联人不存在银行卡号,则必然没有上传过流水,跳过此关联人
        # 此处修改 根据姓名，身份证号查询所有的账户
        if bank_account is None or bank_name is None:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        acc_sql = "select id as account_id, out_req_no, trans_flow_src_type, file_id from trans_account where " \
            "account_name = '%s' and id_card_no = '%s' and bank = '%s' and account_no = '%s'" % \
            (self.user_name, id_card_no, bank_name, bank_account)
        acc_df = sql_to_df(acc_sql)
        if fileids is not None and len(fileids) > 0:
            acc_df = acc_df[acc_df['file_id'].isin(fileids)]
        out_req_no_list = acc_df['out_req_no'].astype(str).tolist()
        flow_sql = "select * from trans_report_flow where out_req_no in (%s)" % ('"'+'","'.join(out_req_no_list)+'"')
        df = sql_to_df(flow_sql)
        df['trans_time'] = pd.to_datetime(df['trans_time'])
        df['trans_date'] = df['trans_time'].apply(lambda x: x.date())
        # 成本支出项 水电、工资、保险、税费
        cost_lab_dict = {'0102010411': '水电', '0102010201': '工资', '0102010402': '保险',
                         '0102010301': '税费', '0102010302': '税费', '0102010303': '税费', '0102010304': '税费'}
        df['cost_type'] = df['mutual_exclusion_label'].map(lambda x: cost_lab_dict[x] if x in cost_lab_dict.keys() else '')
        df['remark_type'] = ''
        # 若数据库里面不存在该银行卡的流水信息,则跳过此关联人
        if len(df) == 0:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        # 获取trans_flow_src_type
        df = pd.merge(df, acc_df, how='left', on='out_req_no')
        df.trans_flow_src_type.fillna(1, inplace=True)
        df['trans_flow_src_type'] = df['trans_flow_src_type'].apply(lambda x: 1 if x in [2, 3] else 0)
        # 重新打relationship标签
        self._relationship_dict()
        for i, v in self.relation_dict.items():
            df.loc[df['opponent_name'].astype(str).str.contains(i, regex=False), 'relationship'] = v
        # 将码值映射成文字
        df['label1'] = df['mutual_exclusion_label'].map(self.label_tree)
        # df['uni_type'] = df['mutual_exclusion_label'].apply(lambda x: x[4:8])
        df['uni_type'] = df['mutual_exclusion_label'].str[4:8]
        df['usual_trans_type'] = df['compatibility_label'].apply(
            lambda x: ','.join([str(self.label_tree.get(y)) for y in x.split(',')]) if pd.notna(x) else '')
        df['unusual_trans_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0203' else None, axis=1)
        df['loan_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0202' else None, axis=1)
        df['is_sensitive'] = df.apply(
            lambda x: 1 if pd.notna(x['unusual_trans_type']) or pd.notna(x['loan_type']) else None, axis=1)
        self.trans_flow_df = self._time_interval(df, 2)
        self.account_id = self.trans_flow_df.sort_values(by='trans_date').tail(1)['account_id'].tolist()[0]
        self.trans_flow_portrait_df = self._time_interval(df, 1)
        self.trans_flow_portrait_df_2_years = self._time_interval(df, 2)
        return out_req_no_list

    def trans_single_portrait(self):
        sql = """select * from trans_flow_portrait where account_id = '%s' and report_req_no = '%s'
        """ % (self.account_id, self.report_req_no)
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_flow_portrait_df = self._time_interval(df, 1)
        self.trans_flow_portrait_df_2_years = self._time_interval(df, 2)

    def u_process(self, out_req_no_list):
        # sql = """select a.*,b.bank,b.account_no from trans_flow_portrait a left join trans_account b on
        #     a.account_id=b.id where a.report_req_no = '%s'""" % self.report_req_no
        # df = sql_to_df(sql)
        # if len(df) == 0:
        #     return
        # self.trans_u_flow_df = df

        acc_sql = "select id as account_id, out_req_no, trans_flow_src_type, file_id from trans_account " \
                  "where out_req_no in (%s)" % ('"'+'","'.join(out_req_no_list)+'"')
        acc_df = sql_to_df(acc_sql)

        flow_sql = "select * from trans_report_flow where out_req_no in (%s)" % ('"'+'","'.join(out_req_no_list)+'"')
        df = sql_to_df(flow_sql)
        df['trans_time'] = pd.to_datetime(df['trans_time'])
        df['trans_date'] = df['trans_time'].apply(lambda x: x.date())
        # 成本支出项 水电、工资、保险、税费
        cost_lab_dict = {'0102010411': '水电', '0102010201': '工资', '0102010402': '保险',
                         '0102010301': '税费', '0102010302': '税费', '0102010303': '税费', '0102010304': '税费'}
        df['cost_type'] = df['mutual_exclusion_label'].map(lambda x: cost_lab_dict[x] if x in cost_lab_dict.keys() else '')
        df['remark_type'] = ''
        # 若数据库里面不存在该笔业务的流水信息
        if len(df) == 0:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        df = pd.merge(df, acc_df, how='left', on='out_req_no')
        df.trans_flow_src_type.fillna(1, inplace=True)
        # 重新打relationship标签
        self._relationship_dict()
        for i, v in self.relation_dict.items():
            df.loc[df['opponent_name'].astype(str).str.contains(i, regex=False), 'relationship'] = v
        # 将码值映射成文字
        df['label1'] = df['mutual_exclusion_label'].map(self.label_tree)
        # df['uni_type'] = df['mutual_exclusion_label'].apply(lambda x: x[4:8])
        df['uni_type'] = df['mutual_exclusion_label'].str[4:8]
        df['usual_trans_type'] = df['compatibility_label'].apply(
            lambda x: ','.join([str(self.label_tree.get(y)) for y in x.split(',')]) if pd.notna(x) else '')
        df['unusual_trans_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0203' else None, axis=1)
        df['loan_type'] = df.apply(lambda x: x['label1'] if x['uni_type'] == '0202' else None, axis=1)
        df['is_sensitive'] = df.apply(
            lambda x: 1 if pd.notna(x['unusual_trans_type']) or pd.notna(x['loan_type']) else None, axis=1)
        self.trans_u_flow_df = self._time_interval(df, 2)
        self.trans_u_flow_portrait_df = self._time_interval(df, 1)
        self.trans_u_flow_portrait_df_2_years = self._time_interval(df, 2)

    def trans_union_portrait(self):
        sql = """select * from trans_u_flow_portrait where report_req_no = '%s'""" % self.report_req_no
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_u_flow_portrait_df = self._time_interval(df, 1)
        self.trans_u_flow_portrait_df_2_years = self._time_interval(df, 2)

    @staticmethod
    def _time_interval(df, year=1):
        flow_df = df.copy()
        if 'trans_date' in flow_df.columns:
            filter_col = 'trans_date'
        else:
            filter_col = 'trans_time'
        flow_df[filter_col] = pd.to_datetime(flow_df[filter_col])
        max_date = flow_df[filter_col].max()
        # min_date = flow_df[filter_col].min()
        #
        # if year != 1:
        #     if max_date.month == 12:
        #         years_before_first = datetime.datetime(max_date.year - year + 1, 1, 1)
        #     else:
        #         years_before_first = datetime.datetime(max_date.year - year, max_date.month + 1, 1)
        # else:
        #     years_before_first = datetime.datetime(max_date.year - year, max_date.month, 1)
        # min_date = max(min_date, years_before_first)
        flow_df = flow_df[flow_df[filter_col] >= max_date - offsets.DateOffset(months=12 * year)]
        return flow_df

    def _relationship_dict(self):
        length = len(self.query_data_array)
        self.relation_dict = dict()
        for i in range(length):
            temp = self.query_data_array[i]
            base_type_detail = temp['baseTypeDetail']
            # if base_type_detail not in ['U_PER_GUARANTOR_PERSONAL', 'U_PER_GUARANTOR_COMPANY',
            #                             'U_COM_GUARANTOR_PERSONAL', 'U_COM_GUARANTOR_COMPANY']:
            name = temp['name']
            if name in ['*', '', '.', '?']:
                name = f'\\{name}'
            self.relation_dict[name] = base_type_detail
            if base_type_detail in ['U_PER_SP_PERSONAL', 'U_COM_CT_SP_PERSONAL']:
                self.spouse_name = str(temp['name'])
