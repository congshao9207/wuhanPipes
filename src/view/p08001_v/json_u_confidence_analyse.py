from view.TransFlow import TransFlow
import numpy as np
import pandas as pd
from util.mysql_reader import sql_to_df
from pandas.tseries.offsets import *
from fileparser.trans_flow.trans_config import *
import re


def multi_mapping_score(val, cnt, cut_list1, cut_list2, score_list):
    if pd.isna(val):
        return score_list[0]
    for i in range(1, len(cut_list1)):
        if cut_list1[i - 1] < val <= cut_list1[i]:
            if cnt < cut_list2[i - 1]:
                return score_list[2 * i - 2]
            else:
                return score_list[2 * i - 1]
    return score_list[0]


def union_date(start, end):
    res = []
    for i, v in enumerate(start):
        if i == 0:
            res.append([v, end[i]])
        else:
            if v <= res[-1][-1]:
                if end[i] > res[-1][-1]:
                    res[-1][-1] = end[i]
            else:
                res.append([v, end[i]])
    return res


class JsonUnionConfidenceAnalyse(TransFlow):
    def process(self):
        self.variables['confidence_analyse'] = []
        basic_sql = """
                SELECT ap.related_name AS relatedName, acc.id as account_id,
                ap.relationship AS relation, ap.account_id as unique_id,
                ac.bank AS bankName,ac.account_no AS bankAccount, acc.out_req_no,
                acc.start_time, acc.end_time, acc.trans_flow_src_type, ap.id_card_no, acc.file_id
                FROM trans_apply ap
                left join trans_account ac
                on ap.account_id = ac.id
                left join trans_account acc
                on ac.account_no = acc.account_no and ac.bank = acc.bank and ac.id_card_no = acc.id_card_no
                where ap.report_req_no = %(report_req_no)s
            """
        basic_df = sql_to_df(sql=basic_sql, params={"report_req_no": self.reqno})
        basic_df.rename(columns={'relatedname': 'relatedName', 'bankname': 'bankName', 'bankaccount': 'bankAccount'},
                        inplace=True)
        # 20230308非实名版仅保留选择的文件
        # 20240620 流水3.0 不再区分product_code，统一筛选file_id
        # product_code = self.origin_data['strategyInputVariables']['product_code']
        if self.file_ids is not None and len(self.file_ids) > 0:
            basic_df = basic_df[basic_df['file_id'].isin(self.file_ids)]
        # relation_list = basic_df['relatedName'].unique().tolist()
        year_ago = pd.to_datetime((basic_df['end_time'].max() - DateOffset(months=12)).date())
        basic_df = basic_df[(basic_df['start_time'] >= year_ago) | (basic_df['end_time'] >= year_ago) |
                            pd.isna((basic_df['account_id']))]
        basic_df.loc[basic_df['start_time'] < year_ago, 'start_time'] = year_ago
        account_list = list(map(str, basic_df[pd.notna(basic_df['out_req_no'])]['out_req_no'].unique().tolist()))

        flow_sql = f"""select * from trans_report_flow where out_req_no in ({'"'+'","'.join(account_list)+'"'})"""
        total_flow = sql_to_df(sql=flow_sql)
        total_flow = total_flow[total_flow['trans_time'] >= year_ago]

        if not total_flow.empty:
            account_df = basic_df[pd.notna(basic_df.account_id)]
            acc_info = \
                account_df.drop_duplicates(subset=['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id'],
                                           )[['relatedName', 'id_card_no', 'bankName', 'bankAccount', 'unique_id']]
            unique_id_list = acc_info.unique_id.unique().tolist()
            overview_confidence_tips = ""
            suggest_confidence_tips = ""
            for ind in acc_info.index:
                # a. 遍历所有!银行账户流水!进行可信度分析
                bank, bank_acc = acc_info.loc[ind, 'bankName'], acc_info.loc[ind, 'bankAccount']
                name, idno = acc_info.loc[ind, 'relatedName'], acc_info.loc[ind, 'id_card_no']
                unique_id = acc_info.loc[ind, 'unique_id']
                temp_df = account_df[(account_df['id_card_no'] == idno) & (account_df['bankName'] == bank) &
                                     (account_df['bankAccount'] == bank_acc) &
                                     (~account_df['trans_flow_src_type'].isin([2, 3]))]
                # 若id_str中包含微信支付宝流水（或当前账户为微信支付宝流水），跳过
                if temp_df.empty:
                    continue
                temp_df.sort_values(by='start_time', inplace=True, ascending=True)
                temp_df['start_time'] = temp_df['start_time'].apply(lambda x: x.date())
                temp_df['end_time'] = temp_df['end_time'].apply(lambda x: x.date())
                temp_start = pd.to_datetime(temp_df['start_time']).tolist()
                temp_end = pd.to_datetime(temp_df['end_time']).tolist()
                temp_date_list = union_date(temp_start, temp_end)
                # 当前账户流水数据
                df = total_flow[total_flow['out_req_no'].isin(temp_df['out_req_no'].unique().tolist())]
                if df.empty:
                    continue
                # 初始化参数
                tmp_dict = {'account_name': f"{name}—{bank}", 'total_score': None}
                # 银行账号处理
                handled_acc_no = ''.join([_ for _ in bank_acc if _.isnumeric()])
                if len(handled_acc_no) < 4:
                    tmp_dict['account_name'] += f'（{bank_acc}）'
                    # 若银行卡不规范，则任意赋值
                    acc_no = '!!!!!!!!!!'
                else:
                    acc_no = handled_acc_no[-4:]
                    tmp_dict['account_name'] += f'（{bank_acc}）'
                # 流水规模
                tmp_dict['income_scale'] = df[df['trans_amt'] > 0]['trans_amt'].sum()
                tmp_dict['expense_scale'] = abs(df[df['trans_amt'] < 0]['trans_amt'].sum())
                # 取近一年流水进行可信度分析
                tmp_dict['balance_constance'] = self.balance_constance(df, temp_date_list)
                tmp_dict['trans_logical'] = self.trans_logical_check(df)

                id_str = ','.join(list(map(str, basic_df[
                    basic_df['unique_id'] == unique_id]['account_id'].tolist())))
                tmp_dict['data_consistency'] = self.data_consistency(df, id_str, temp_start[0])
                tmp_dict['intact_check'] = self.intact_check(df)

                # 银行流水交叉验证
                if basic_df.unique_id.nunique() == 1 or len(unique_id_list) == 1:
                    tmp_dict['flow_cross_verify'] = {'score': 7, 'level': 'B',
                                                     'risk_tips': '主体用户未上传其他账户流水，无法进行交叉验证'}
                else:
                    risk_list = []
                    flow_cross_score = 10
                    for _ in unique_id_list:
                        # 不与本身进行校验
                        if _ != unique_id:
                            refer_df = total_flow[total_flow['out_req_no'].isin(
                                basic_df[basic_df['unique_id'] == _]['out_req_no'].tolist())]
                            if refer_df.empty:
                                continue
                            refer_acc_no = acc_info[acc_info['unique_id'] == _]['bankAccount'].tolist()[0]
                            refer_bank = acc_info[acc_info['unique_id'] == _]['bankName'].tolist()[0]
                            refer_name = acc_info[acc_info['unique_id'] == _]['relatedName'].tolist()[0]
                            score = self.flow_cross_verify(df, refer_df, acc_no, bank, name)
                            if score < 7:
                                risk_list.append({'risk_tip': f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                              f"（{refer_acc_no}）交叉验证失败",
                                                  'tip_level': 'RED'})
                                flow_cross_score -= 5
                            elif score == 7:
                                risk_list.append({'risk_tip': f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                              f"（{refer_acc_no}）无交易记录",
                                                  'tip_level': 'NORMAL'})
                            else:
                                risk_list.append({'risk_tip': f"{name}—{bank}（{bank_acc}）与{refer_name}—{refer_bank}"
                                                              f"（{refer_acc_no}）交叉验证成功",
                                                  'tip_level': 'NORMAL'})
                    tmp_dict['flow_cross_verify'] = {
                        'score': -1 if flow_cross_score < 0 else flow_cross_score,
                        'level': 'A' if flow_cross_score > 7 else 'B' if flow_cross_score > 5 else 'C'
                        if flow_cross_score > 0 else 'D',
                        'risk_detail': risk_list}

                # 所有模块报文封装
                if tmp_dict['balance_constance']['score'] == -1 or tmp_dict['trans_logical']['score'] == -1 or \
                        tmp_dict['data_consistency']['score'] == -1:
                    total_score = 70
                else:
                    total_score = tmp_dict['balance_constance']['score'] + tmp_dict['trans_logical']['score'] + \
                        tmp_dict['data_consistency']['score'] + tmp_dict['flow_cross_verify']['score'] + \
                        tmp_dict['intact_check']['score']
                tmp_dict['total_score'] = total_score
                if total_score < 70:
                    tmp_dict['total_status'] = '可信度为低'
                    suggest_confidence_tips = "存在可信度为低的流水，建议谨慎授信"
                elif total_score < 90:
                    tmp_dict['total_status'] = '可信度为中'
                else:
                    tmp_dict['total_status'] = '可信度为高'
                tmp_dict['file_attribute'] = {'score': 0, 'level': 'A', 'risk_detail': []}
                overview_confidence_tips += f"{name}—{bank}（{bank_acc}）{tmp_dict['total_status']};"
                self.variables['confidence_analyse'].append(tmp_dict)
            self.variables['trans_report_overview']['trans_general_info']['confidence_analyse'][
                'risk_tips'] = overview_confidence_tips
            self.variables['suggestion_and_guide']['trans_general_info']['confidence_analyse'][
                'risk_tips'] = suggest_confidence_tips

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
    def balance_constance(flow, date_list):
        resp = {
            'score': 0,
            'level': 'A',
            'status': '',
            'trans_detail': [],
            'unusual_detail': []
        }
        df = flow.copy()
        # df['trans_date'] = df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d'))
        df['trans_date'] = df['trans_time'].dt.strftime('%Y-%m-%d')
        df.reset_index(drop=True, inplace=True)
        month_list = pd.date_range(df['trans_time'].min().date(), df['trans_time'].max().date() + MonthEnd(0), freq='M')
        max_date = df['trans_time'].max()
        d_ind = 0
        for i, m in enumerate(month_list):
            tmp_dict = dict()
            tmp_dict['order'] = i + 1
            m_end = m + MonthEnd(0)
            m_start = m_end - MonthBegin(1)
            tmp_dict['start_date'], tmp_dict['end_date'] = [], []
            while True:
                tmp_min = max(m_start, pd.to_datetime(date_list[d_ind][0]))
                tmp_max = min(m_end, pd.to_datetime(date_list[d_ind][-1]), max_date)
                if tmp_min <= tmp_max:
                    tmp_dict['start_date'].append(format(tmp_min, '%Y-%m-%d'))
                    tmp_dict['end_date'].append(format(tmp_max, '%Y-%m-%d'))
                if m_end < pd.to_datetime(date_list[d_ind][-1]) or m_end >= pd.to_datetime(date_list[-1][-1]):
                    break
                d_ind += 1
            tmp_dict['unusual_date'] = df[(df.verif_label.astype(str).str.contains('T01'))
                                          & (m_start <= df.trans_time)
                                          & (df.trans_time < m_end + DateOffset(1))
                                          ].trans_date.astype(str).unique().tolist()
            resp['trans_detail'].append(tmp_dict)
        df.loc[df.verif_label.astype(str).str.contains('T01'), 'flow_level'] = 'RED'
        unusual_list = sorted(df[df.verif_label.astype(str).str.contains('T01')].index.tolist() + (
                df[df.verif_label.astype(str).str.contains('T01')].index - 1).tolist())
        # 剔除-1的数据
        unusual_list = [i for i in unusual_list if i != -1]
        # df['trans_time'] = df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d %H:%M:%S'))
        df['trans_time'] = df['trans_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        resp['unusual_detail'] = df.loc[unusual_list, ['trans_time', 'trans_amt', 'account_balance', 'opponent_name',
                                                       'remark', 'flow_level']].to_dict('records')

        # 余额不连续占比
        unusual_cnt = df[df.verif_label.astype(str).str.contains('T01')].shape[0]
        unusual_proportion = unusual_cnt / df.shape[0]
        if df[df.verif_label.astype(str).str.contains('T11|T12')].shape[0] > 0:
            score = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
                                        [0, 5, 10, 20, 20], [25, 25, 21, 18, 15, 12, 8, -1, -1, -1])
        else:
            score = multi_mapping_score(unusual_proportion, unusual_cnt, [0, 0.01, 0.03, 0.05, 0.1, 1],
                                        [0, 10, 15, 20, 30], [25, 25, 21, 18, 15, 12, 8, -1, -1, -1])
        level = 'A' if score >= 20 else 'B' if score >= 15 else 'C' if score >= 10 else 'D'
        resp['score'] = score
        resp['level'] = level
        return resp

    def trans_logical_check(self, flow):
        df = flow.copy()
        resp = {
            'score': 37,
            'level': 'A',
            'opponent_check': {},
            'opponent_detail': [],
            'unusual_check': {
                'unusual_hint': [],
                'unusual_detail': []
            }
        }
        df['opponent_type'] = df['opponent_name'].fillna('').apply(self._opponent_type)
        df['week_day'] = df['trans_time'].dt.weekday
        # df['trans_time'] = df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d %H:%M:%S'))
        df['trans_time'] = df['trans_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['status'] = df['trans_amt'].apply(lambda x: 0 if x % 100 == 0 else 1)
        df.rename(columns={'opponent_account_no': 'opponent_account'}, inplace=True)
        detail_cols = ['trans_time', 'trans_amt', 'account_balance', 'opponent_name', 'opponent_account', 'remark']

        score = 37
        # 剔除特殊交易对手和交易账号后， 汇总所有账号数量
        unusual_df = df[pd.notna(df.opponent_account)
                        & pd.notna(df.opponent_name)
                        & (~df.opponent_account.isin(IGNORE_ACC_NO))
                        & (~df.opponent_name.astype(str).str.contains(IGNORE_OPPO_NAME_PATTERN))
                        & (~df.opponent_account.astype(str).str.contains(IGNORE_ACC_NO_PATTERN))
                        & (df.verif_label.astype(str).str.contains('T07'))
                        & (~df.verif_label.astype(str).str.contains('T10'))]
        unusual_cnt = unusual_df.opponent_account.nunique()
        if unusual_cnt > 0:
            score = -1
            unusual_acc = unusual_df.opponent_account.unique().tolist()
            for acc in unusual_acc:
                acc_df = unusual_df[unusual_df.opponent_account == acc]. \
                    groupby('opponent_name', as_index=False).agg({'out_req_no': 'count'}). \
                    rename(columns={'out_req_no': 'trans_count'})
                acc_df['trans_cnt_prop'] = round(acc_df['trans_count'] / acc_df['trans_count'].sum(), 4) \
                    if acc_df['trans_count'].sum() > 0 else 0
                resp['opponent_check'][acc] = acc_df.to_dict('records')

        resp['opponent_detail'] = unusual_df.sort_values(by=['opponent_account', 'trans_time'])[
            detail_cols].to_dict('records')

        unusual_hint = []

        ent_df = df[(df['opponent_type'] == 2) & (df['trans_amt'] > 0) & (df['week_day'].isin([5, 6])) &
                    (~df['opponent_name'].astype(str).str.isnumeric()) &
                    (~df['opponent_name'].astype(str).str.contains(''.join(UNUSUAL_OPPO_NAME)))]
        if ent_df.shape[0] > 0:
            unusual_hint.append('对公户存在周末交易')
            score -= 6 if score > 0 else 0

        if not self.file_status:
            unusual_hint.append('预解析账户名称账号存在不一致')
            score -= 2 if score > 0 else 0

        atm_df = df[(df.remark.str.contains('atm|ATM')) & (df.trans_amt < 0) & (df['status'] == 1)]
        if atm_df.shape[0] > 0:
            unusual_hint.append('存在ATM取款金额不为整百')
            score -= 6 if score > 0 else 0

        salary_df = df[(df['mutual_exclusion_label'] == '0102010201') & (df['week_day'].isin([5, 6]))]
        if salary_df.shape[0] > 0:
            unusual_hint.append('工资发放出现在周末')
            score -= 6 if score > 0 else 0

        hund_prop = df['status'].mean()
        hund_cnt = df[df['status'] == 0].shape[0]
        if hund_cnt > 10 and hund_prop < 0.5:
            unusual_hint.append('整百交易比例超过50%')
            score -= 2 if score > 0 else 0

        unusual_detail = pd.concat([ent_df, atm_df, salary_df], axis=0, ignore_index=True)[
            detail_cols].to_dict('records')
        resp['score'] = score
        resp['level'] = 'A' if score >= 27 else 'B' if score >= 21 else 'C' if score >= 15 else 'D'
        resp['unusual_check']['unusual_hint'] = unusual_hint
        resp['unusual_check']['unusual_detail'] = unusual_detail
        return resp

    def data_consistency(self, flow, id_str, year_ago):
        resp = {
            'score': 20,
            'level': 'A',
            'interest_analyse': {
                'interest_cnt': '',
                'interest_detail': []
            },
            'trans_freq_check': {
                "coefficient": 1.1,
                "level": "正常"
            },
            'fund_procurement_check': {
                "coefficient": 0.9,
                "level": "正常"
            },
            'benford_coefficient': {
                "coefficient": 0.8,
                "level": "优"
            }
        }
        df = flow.sort_values(by=['trans_time', 'id'], ascending=True)
        # df['trans_date'] = df['trans_time'].apply(lambda x: format(x, '%Y-%m-%d'))
        df['trans_date'] = df['trans_time'].dt.strftime('%Y-%m-%d')
        score, interest_analyse = self.interest_analyse(df, id_str, year_ago)
        benford_coefficient = self.benford_ratio(df)
        benford_level = '优' if benford_coefficient >= 0.8 else '良' if benford_coefficient >= 0.5 else '中'

        df['hour_cnt'] = df.rolling('1h', on='trans_time').count()['out_req_no']
        hour_df = df.groupby('trans_date').agg(
            freq_diff=pd.NamedAgg('hour_cnt', lambda x: max(x) / np.percentile(x, 75)))
        max_trans_freq_diff = hour_df['freq_diff'].max()

        if score > 0:
            score += 0 if benford_coefficient < 0.5 else 1 if benford_coefficient < 0.8 else 2
            score += 0 if max_trans_freq_diff >= 2.5 else 2
            score += 2
        level = 'A' if score >= 16 else 'B' if score >= 12 else 'C' if score >= 8 else 'D'
        resp['score'] = score
        resp['level'] = level
        resp['interest_analyse'] = interest_analyse
        resp['benford_coefficient'] = {'coefficient': benford_coefficient,
                                       'level': benford_level}
        resp['trans_freq_check'] = {'coefficient': max_trans_freq_diff,
                                    'level': '正常' if max_trans_freq_diff < 2.5 else '异常'}
        return resp

    def interest_analyse(self, flow, id_str, year_ago):
        flow['trans_month'] = flow['trans_time'].dt.strftime('%Y-%m')
        single_summary_sql = """select distinct account_id, month, interest_amt, balance_amt,
            interest_balance_proportion from trans_single_summary_portrait
            where account_id in (%s) and report_req_no = '%s'""" % (id_str, self.reqno)
        single_df = sql_to_df(single_summary_sql)
        single_df = single_df[(~single_df['month'].str.isnumeric()) &
                              (single_df['month'] >= str(year_ago)[:7])].sort_values('month')
        resp = {'interest_cnt': '0/0', 'interest_detail': []}
        score = 0
        if single_df.shape[0] > 0:
            # 页面展示，结息次数
            real_cnt = single_df[pd.notna(single_df.interest_amt)].shape[0]
            should_cnt = single_df[(~single_df['month'].str.contains(r'\*')) |
                                   ((single_df['month'].str.contains(r'\*')) &
                                    (pd.notna(single_df.interest_amt)))].shape[0]
            should_cnt = real_cnt if real_cnt > should_cnt else should_cnt
            resp['interest_cnt'] = f'{real_cnt}/{should_cnt}'
            # 页面展示，结息金额具体状态
            single_df.where(single_df.notnull(), None)
            if should_cnt - real_cnt >= 2:
                score = 4 if single_df.shape[0] >= 4 else 0
            elif should_cnt - real_cnt == 1:
                score = 12 if single_df.shape[0] >= 4 else 8
            else:
                score = 16
            quar_list = single_df['month'].tolist()
            for quar in quar_list:
                quar_end = pd.to_datetime(f"{quar[:7]}-21")
                quar_start = quar_end - DateOffset(months=3)
                single_df.loc[single_df['month'] == quar, 'trans_freq'] = round(
                    flow[(flow['trans_time'] >= quar_start) &
                         (flow['trans_time'] < quar_end)].groupby('trans_month').agg({'id': 'count'})['id'].mean(), 2)
            single_df['balance_interest_prop'] = single_df['interest_balance_proportion'].fillna(0)
            single_df['tip_level'] = single_df['balance_interest_prop'].apply(
                lambda x: 'NORMAL' if pd.isna(x) or 0.75 <= x <= 1.25 else 'RED')
            resp['interest_detail'] = single_df[['month', 'interest_amt', 'balance_amt', 'balance_interest_prop',
                                                 'trans_freq', 'tip_level']].to_dict('records')
            # interest_cnt = single_df[pd.notna(single_df.interest_balance_proportion)
            #                          & ((single_df.interest_balance_proportion < 0.75) |
            #                             (single_df.interest_balance_proportion > 1.25))].shape[0]
            # if interest_cnt > 0:
            #     score = -1
        return score, resp

    @staticmethod
    def intact_check(df):
        """
        检查交易对手、交易对手账号、备注字段是否完整
        :param df:
        :return:
        """
        resp = dict()
        score = 6
        intact_list = []
        col_mapping = {'opponent_name': '交易对手', 'opponent_account_no': '交易对手账号', 'remark': '备注'}
        for col in ['opponent_name', 'opponent_account_no', 'remark']:
            col_cnt = df[pd.isna(df[col]) | (df[col] == '')].shape[0]
            col_rate = col_cnt / df.shape[0]
            score -= 2 if col_rate > 0.5 else 0
            if col_rate > 0.5:
                intact_list.append({'risk_tip': f'{col_mapping[col]}字段缺失率为{col_rate: .0%}',
                                    'miss_rate': col_rate, 'tip_level': 'RED'})
        resp['score'] = score
        resp['level'] = 'A' if score > 4 else 'B' if score > 2 else 'C' if score > 0 else 'D'
        resp['intact_detail'] = intact_list
        return resp

    @staticmethod
    def flow_cross_verify(flow, other_flow, acc_no, bank, user_name):
        """
        对比银行流水和其他流水交叉验证
        :param flow:
        :param other_flow:
        :param acc_no:
        :param bank:
        :param user_name:
        :return:
        """
        df, refer_df = flow.copy(), other_flow.copy()
        # 取银行流水和微信流水交集
        start_time, check_end_time = max((min(df.trans_time)), min(refer_df.trans_time)), \
            min(max(df.trans_time), max(refer_df.trans_time))
        score = 7
        if start_time < check_end_time:
            df = df[(df.trans_time >= start_time) & (df.trans_time <= check_end_time)]
            refer_df = refer_df[(refer_df.trans_time >= start_time) & (refer_df.trans_time <= check_end_time)]
            trans_record = refer_df[
                (refer_df.trans_type.astype(str).str.contains(acc_no)
                 & ~refer_df.trans_type.astype(str).str.contains('余额')
                 & refer_df.trans_type.astype(str).str.contains(bank))
                | (refer_df.opponent_account_no.astype(str).str.contains(acc_no)
                   | (refer_df.opponent_name.astype(str).str.contains(acc_no)))
                & (refer_df.opponent_name.astype(str).str.contains('|'.join([user_name, bank]))
                   | refer_df.opponent_account_bank.astype(str).str.contains(bank))]
            # 1.交易类型包含银行名称以及银行账户，但不包含“余额” （针对微信支付宝流水）
            # 2.交易对手账号包含银行账户 且 交易对手包含用户名|银行名称 或 交易银行名包含银行名称
            if not trans_record.empty:
                unusual_cnt = 0
                trans_record.index = [_ for _ in range(trans_record.shape[0])]
                for ind in trans_record.index:
                    check_start_time = trans_record.loc[ind, 'trans_time'] - DateOffset(1)
                    check_end_time = check_start_time + DateOffset(2)
                    # 参照银行账户金额
                    record_amt = abs(trans_record.loc[ind, 'trans_amt'])
                    check_df = df[(df.trans_time <= check_end_time) & (df.trans_time >= check_start_time)
                                  & (abs(df.trans_amt) >= record_amt * 0.998)
                                  & (abs(df.trans_amt) <= record_amt * 1.002)]
                    if check_df.empty:
                        unusual_cnt += 1
                unsual_proportion = unusual_cnt / trans_record.shape[0]
                score = multi_mapping_score(unsual_proportion, unusual_cnt, [-1, 0.2499, 0.4999, 0.7499, 1],
                                            [0, 9, 6, 3], [8, 8, 6, 4, 4, 2, 2, 0])
        return score

    @staticmethod
    def benford_ratio(df):
        """
        计算Benford分布检验系数
        :param df:
        :return:
        """
        expect_frequency = [0.301, 0.176, 0.125, 0.097, 0.079, 0.067, 0.058, 0.051, 0.046]
        first_num_list = [str(abs(_))[:1] for _ in df[abs(df.trans_amt) >= 1].trans_amt.tolist()]
        rate = 0
        for _ in range(1, 10):
            actual_frequency = first_num_list.count(str(_)) / len(first_num_list) if len(first_num_list) > 0 else 0
            if actual_frequency == 0:
                actual_frequency = 1e-6
            rate += (actual_frequency - expect_frequency[_ - 1]) * np.log(actual_frequency / expect_frequency[_ - 1])
        return 1 - rate
