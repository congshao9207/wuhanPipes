from view.TransFlow import TransFlow
import pandas as pd
from fileparser.trans_flow.trans_config import UP_DOWNSTREAM_THRESHOLD, UNUSUAL_OPPO_NAME
import re
from pandas.tseries.offsets import *


class JsonUnionCounterpartyPortrait(TransFlow):
    """
        主要交易对手模块信息
        author:汪腾飞
        created_time:20200708
        updated_time_v1:
    """

    def process(self):
        self.read_u_counterparty_pt()

    def read_u_counterparty_pt(self):
        flow_df = self.trans_u_flow_portrait[['trans_date', 'trans_amt', 'opponent_name', 'relationship',
                                              'is_sensitive', 'trans_flow_src_type']]
        # 获取一年前的日期
        year_ago = pd.to_datetime(flow_df['trans_date']).max() - DateOffset(months=12)

        # 筛选近一年经营性流水
        flow_df = flow_df[(pd.isnull(flow_df['relationship'])) &
                          (flow_df['is_sensitive'] != 1) &
                          (pd.notnull(flow_df['opponent_name'])) &
                          (pd.to_datetime(flow_df.trans_date) >= year_ago) &
                          (~flow_df['opponent_name'].astype(str).str.isnumeric()) &
                          (~flow_df['opponent_name'].astype(str).str.contains(''.join(UNUSUAL_OPPO_NAME)))]
        # 若筛选后df为空，直接返回节点信息
        if flow_df.shape[0] == 0:
            self.variables["trans_u_counterparty_portrait"] = {
                'bank_flow': {"income_amt_order": {},
                              "expense_amt_order": {},
                              "superposition": {}},
                'wxzfb_flow': {"income_amt_order": {},
                               "expense_amt_order": {},
                               "superposition": {}}
            }
            self.variables['lite_counterparty'] = {
                'lite_income': [],
                'lite_expense': []
            }

            # 解析结果专家经验
            self.variables['trans_report_overview']['business_info']['upstream_customers']['risk_tips'] = ""
            self.variables['trans_report_overview']['business_info']['downstream_customers']['risk_tips'] = ""
            return

        min_date = min(flow_df['trans_date'])
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                str((x.year - min_year) * 12 + x.month - min_month))
        bank_income_amt_order = {}
        bank_expense_amt_order = {}
        bank_superposition_amt_order = {}
        wxzfb_income_amt_order = {}
        wxzfb_expense_amt_order = {}
        wxzfb_superposition_amt_order = {}
        bank_income_amt_risk_tips = ''
        bank_expense_amt_risk_tips = ''
        wxzfb_income_amt_risk_tips = ''
        wxzfb_expense_amt_risk_tips = ''

        # 对交易对手名进行处理，防止有账户名加账号情况
        def op_name_trans(op_name):
            all_ascii_str = re.findall(r'&#\d{2,5};', str(op_name))
            for s in all_ascii_str:
                ss = re.sub(r'\D', '', s)
                op_name.replace(s, chr(int(ss)))
            op_name = re.sub(r'[^\u4e00-\u9fa5]', '', str(op_name))
            return op_name

        flow_df['opponent_name'] = flow_df['opponent_name'].apply(op_name_trans)
        flow_df = flow_df[flow_df['opponent_name'] != '']
        bank_df = flow_df[flow_df['trans_flow_src_type'] == 0]
        wxzfb_df = flow_df[flow_df['trans_flow_src_type'] == 1]
        if bank_df.shape[0] > 0:
            bank_income_amt_order, bank_income_amt_risk_tips, \
                bank_expense_amt_order, bank_expense_amt_risk_tips, bank_superposition_amt_order = self.get_amt_order(bank_df)

        if wxzfb_df.shape[0] > 0:
            wxzfb_income_amt_order, wxzfb_income_amt_risk_tips, \
                wxzfb_expense_amt_order, wxzfb_expense_amt_risk_tips, wxzfb_superposition_amt_order = self.get_amt_order(wxzfb_df)

        income_amt_risk_tips = bank_income_amt_risk_tips + wxzfb_income_amt_risk_tips
        expense_amt_risk_tips = bank_expense_amt_risk_tips + wxzfb_expense_amt_risk_tips

        # # 20240710 新增需传递给lite-pipes的上下游客户名单，仅考虑银行流水的上下游名单
        lite_income_list = self._get_lite_counterparty_list(bank_income_amt_order)
        lite_expense_list = self._get_lite_counterparty_list(bank_expense_amt_order)

        self.variables['lite_counterparty'] = {
            'lite_income': lite_income_list,
            'lite_expense': lite_expense_list
        }
        self.variables["trans_u_counterparty_portrait"] = {
            'bank_flow': {"income_amt_order": bank_income_amt_order,
                          "expense_amt_order": bank_expense_amt_order,
                          "superposition": bank_superposition_amt_order},
            'wxzfb_flow': {"income_amt_order": wxzfb_income_amt_order,
                           "expense_amt_order": wxzfb_expense_amt_order,
                           "superposition": wxzfb_superposition_amt_order}
        }
        # 解析结果专家经验
        self.variables['trans_report_overview']['business_info']['upstream_customers'][
            'risk_tips'] = expense_amt_risk_tips
        self.variables['trans_report_overview']['business_info']['downstream_customers'][
            'risk_tips'] = income_amt_risk_tips

    @staticmethod
    def _get_lite_counterparty_list(amt_order):
        if len(amt_order) == 0:
            return []
        # 仅需要名单，不用考虑排名情况
        counterparty_list = []
        for k, v in amt_order.items():
            if k != 'risk_tips' and len(v) > 0:
                for i in v:
                    if i['month'] == '汇总':
                        counterparty_list.append(i['opponent_name'])
                        break
        return counterparty_list

    def get_amt_order(self, flow_df):
        # 上下游交叉部分处理
        risk_type = '银行流水：' if flow_df['trans_flow_src_type'].iloc[0] == 0 else '微信支付宝流水：'
        income_df = flow_df[flow_df.trans_amt > 0]
        expense_df = flow_df[flow_df.trans_amt < 0]

        #  剔除交易对手既是上游客户，也是下游客户
        income_name_list = income_df['opponent_name'].unique().tolist()
        expense_name_list = expense_df['opponent_name'].unique().tolist()
        superposition_list = [i for i in income_name_list if i in expense_name_list]
        superposition_df = flow_df.loc[flow_df['opponent_name'].isin(superposition_list)]
        for name in income_name_list:
            if (name in income_df.opponent_name.unique()) and (name in expense_df.opponent_name.unique()):
                income_amt = income_df[income_df.opponent_name == name].trans_amt.sum()
                expense_amt = expense_df[expense_df.opponent_name == name].trans_amt.sum()
                if income_amt >= abs(expense_amt):
                    expense_df.drop(expense_df[expense_df['opponent_name'] == name].index, inplace=True)
                else:
                    income_df.drop(income_df[income_df['opponent_name'] == name].index, inplace=True)

        income_amt_order = self.in_out_detail(income_df)
        expense_amt_order = self.in_out_detail(expense_df)
        superposition_amt_order = self.superposition_detail(superposition_df)

        # 专家经验部分
        op_mapping = {1: '最大', 2: '前两大', 3: '前三大', 4: '前四大', 5: '前五大'}
        income_amt_risk_tips = ''
        if len(income_amt_order) > 0:
            max_income_order = max([int(_) for _ in income_amt_order.keys()])
            # 下游客户专家经验
            # 获取下游客户前5大交易对手交易占比
            income_amt_top_trans_amt_proportion_list = [
                self.get_topn_trans_amt_proportion(income_amt_order, str(_))
                for _ in range(1, 1 + min(max_income_order, 5))]
            income_amt_top_trans_amt_proportion_list = [_ for _ in income_amt_top_trans_amt_proportion_list if
                                                        _ is not None]
            total_income_amt_proportion = sum(income_amt_top_trans_amt_proportion_list)
            if total_income_amt_proportion > 1:
                total_income_amt_proportion = 1
            if total_income_amt_proportion >= 0.5:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户比较集中，建议收集相关业务合同;"
            elif total_income_amt_proportion <= 0.2:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户比较分散;"
            else:
                income_amt_risk_tips = \
                    f"{op_mapping[len(income_amt_top_trans_amt_proportion_list)]}下游客户交易总金额" \
                    f"占比{round(total_income_amt_proportion * 100, 2)}%，下游客户构成无异常;"
            income_amt_order['risk_tips'] = income_amt_risk_tips
            income_amt_risk_tips = risk_type + income_amt_risk_tips if income_amt_risk_tips != '' else ''

        expense_amt_risk_tips = ''
        if len(expense_amt_order) > 0:
            max_expense_order = max(map(int, expense_amt_order.keys()))
            # 上游客户专家经验
            # 获取上游客户前5大交易对手交易占比
            expense_amt_top_trans_amt_proportion_list = [
                self.get_topn_trans_amt_proportion(expense_amt_order, str(_))
                for _ in range(1, 1 + min(5, max_expense_order))]
            expense_amt_top_trans_amt_proportion_list = [_ for _ in expense_amt_top_trans_amt_proportion_list if
                                                         _ is not None]
            total_expense_amt_proportion = sum(expense_amt_top_trans_amt_proportion_list)
            if total_expense_amt_proportion > 1:
                total_expense_amt_proportion = 1
            if total_expense_amt_proportion >= 0.5:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户比较集中，建议收集相关业务合同;"
            elif total_expense_amt_proportion <= 0.2:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户比较分散;"
            else:
                expense_amt_risk_tips = \
                    f"{op_mapping[len(expense_amt_top_trans_amt_proportion_list)]}上游客户交易总金额" \
                    f"占比{round(total_expense_amt_proportion * 100, 2)}%，上游客户构成无异常;"

            expense_amt_order['risk_tips'] = expense_amt_risk_tips
            expense_amt_risk_tips = risk_type + expense_amt_risk_tips if expense_amt_risk_tips != '' else ''
        return income_amt_order, income_amt_risk_tips, expense_amt_order, expense_amt_risk_tips, superposition_amt_order

    @staticmethod
    def in_out_detail(df):
        # 进出帐分别处理
        all_detail = {}
        in_out_type = 'income_amt_order'
        out_in_type = 'expense_amt_order'
        if df['trans_amt'].sum() < 0:
            in_out_type = 'expense_amt_order'
            out_in_type = 'income_amt_order'

        # 前十客户表单
        # 平均账期函数
        def gap_avg(date):
            all_unique_trans_date = sorted(list(set(date.to_list())))
            diff_days = [(all_unique_trans_date[i + 1] - all_unique_trans_date[i]).days - 1
                         for i in range(len(all_unique_trans_date) - 1)]
            diff_days = [x for x in diff_days if x != 0]
            return sum(diff_days) / len(diff_days) if len(diff_days) != 0 else 0

        df['trans_amt'] = df['trans_amt'].apply(abs)
        order_df = df.groupby('opponent_name').aggregate(
            {'trans_amt': ['sum', 'count', 'mean'],
             'calendar_month': ['nunique'],
             'trans_date': [gap_avg]}).reset_index()
        order_df.columns = ['opponent_name', 'trans_amt', 'trans_cnt', 'trans_mean', 'trans_month_cnt', 'trans_gap_avg']

        # 剔除交易总额1000以下的客户
        order_df = order_df[order_df['trans_amt'] > UP_DOWNSTREAM_THRESHOLD]
        order_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        order_df.reset_index(inplace=True, drop=True)
        # 总金额
        income_total_amt = order_df['trans_amt'].sum()
        # 金额占比
        order_df['trans_amt_proportion'] = \
            order_df['trans_amt'] / income_total_amt if income_total_amt != 0 else 0
        # 取前十
        order_df = order_df.iloc[:10, ]
        # 添加序号
        order_df[in_out_type] = order_df.index + 1
        order_df[in_out_type] = order_df[in_out_type].astype(str)
        # 月份
        order_df['month'] = '汇总'
        # 同步老接口
        order_df[out_in_type] = None
        order_df['income_amt_proportion'] = None

        order_df_list = order_df.to_dict('records')
        for index, detail in enumerate(order_df_list):
            all_detail[str(index + 1)] = [detail]
        # 下游客户明细表单
        order_df_detail = df.groupby(['opponent_name', 'calendar_month']).agg(
            trans_amt=('trans_amt', 'sum'),
            trans_cnt=('trans_amt', 'count')
        ).reset_index()
        order_df_detail = order_df_detail.rename(columns={"calendar_month": "month"})
        # 获取13个月份
        full_df = pd.DataFrame({'month': [str(_ + 1) for _ in range(13)]})
        for order in range(order_df.shape[0]):
            amt_order = str(order + 1)
            opponent_name = order_df[order_df[in_out_type] == amt_order]['opponent_name'].iloc[0]
            detail = order_df_detail[order_df_detail['opponent_name'] == opponent_name]
            detail = pd.merge(full_df, detail, how='left', on='month')
            detail['opponent_name'].fillna(opponent_name, inplace=True)
            detail['trans_amt'].fillna(0, inplace=True)
            detail['trans_cnt'].fillna(0, inplace=True)
            detail[in_out_type] = amt_order
            # 适配老接口
            detail[out_in_type] = None
            detail['income_amt_proportion'] = None
            detail['trans_gap_avg'] = None
            detail['trans_amt_proportion'] = None
            detail['trans_mean'] = None
            detail['trans_month_cnt'] = None
            detail['trans_gap_avg'] = None
            all_detail[amt_order] += detail.to_dict('records')
        return all_detail

    # 获取排名第n名交易对手的交易占比
    @staticmethod
    def get_topn_trans_amt_proportion(json_data, key):
        topn_amt_order_list = json_data[key]
        for data in topn_amt_order_list:
            month = data['month']
            trans_amt_proportion = data['trans_amt_proportion']
            if month == "汇总":
                return trans_amt_proportion

    @staticmethod
    def superposition_detail(df):
        # 进出帐分别处理
        all_detail = {}
        in_out_type = 'income_amt_order'
        out_in_type = 'expense_amt_order'
        if df['trans_amt'].sum() < 0:
            in_out_type = 'expense_amt_order'
            out_in_type = 'income_amt_order'

        # 前十客户表单
        # 平均账期函数
        def gap_avg(date):
            all_unique_trans_date = sorted(list(set(date.to_list())))
            diff_days = [(all_unique_trans_date[i + 1] - all_unique_trans_date[i]).days - 1
                         for i in range(len(all_unique_trans_date) - 1)]
            diff_days = [x for x in diff_days if x != 0]
            return sum(diff_days) / len(diff_days) if len(diff_days) != 0 else 0

        df['trans_amt'] = df['trans_amt'].apply(abs)
        order_df = df.groupby('opponent_name').aggregate(
            {'trans_amt': ['sum', 'count', 'mean'],
             'calendar_month': ['nunique'],
             'trans_date': [gap_avg]}).reset_index()
        order_df.columns = ['opponent_name', 'trans_amt', 'trans_cnt', 'trans_mean', 'trans_month_cnt', 'trans_gap_avg']

        # 剔除交易总额1000以下的客户
        order_df = order_df[order_df['trans_amt'] > UP_DOWNSTREAM_THRESHOLD]
        order_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        order_df.reset_index(inplace=True, drop=True)
        # 总金额
        income_total_amt = order_df['trans_amt'].sum()
        # 金额占比
        order_df['trans_amt_proportion'] = \
            order_df['trans_amt'] / income_total_amt if income_total_amt != 0 else 0
        # 取前十
        order_df = order_df.iloc[:10, ]
        # 添加序号
        order_df['amt_order'] = order_df.index + 1
        order_df['amt_order'] = order_df['amt_order'].astype(str)
        order_df[in_out_type] = order_df.index + 1
        order_df[in_out_type] = order_df[in_out_type].astype(str)
        # 月份
        order_df['month'] = '汇总'
        # 同步老接口
        order_df[out_in_type] = None
        order_df['income_amt_proportion'] = None

        order_df_list = order_df.to_dict('records')
        for index, detail in enumerate(order_df_list):
            all_detail[str(index + 1)] = [detail]
        # 下游客户明细表单
        order_df_detail = df.groupby(['opponent_name', 'calendar_month']).agg(
            trans_amt=('trans_amt', 'sum'),
            trans_cnt=('trans_amt', 'count')
        ).reset_index()
        order_df_detail = order_df_detail.rename(columns={"calendar_month": "month"})
        # 获取13个月份
        full_df = pd.DataFrame({'month': [str(_ + 1) for _ in range(13)]})
        for order in range(order_df.shape[0]):
            amt_order = str(order + 1)
            opponent_name = order_df[order_df['amt_order'] == amt_order]['opponent_name'].iloc[0]
            detail = order_df_detail[order_df_detail['opponent_name'] == opponent_name]
            detail = pd.merge(full_df, detail, how='left', on='month')
            detail['opponent_name'].fillna(opponent_name, inplace=True)
            detail['trans_amt'].fillna(0, inplace=True)
            detail['trans_cnt'].fillna(0, inplace=True)
            detail['amt_order'] = amt_order
            # 适配老接口
            detail[out_in_type] = None
            detail['income_amt_proportion'] = None
            detail['trans_gap_avg'] = None
            detail['trans_amt_proportion'] = None
            detail['trans_mean'] = None
            detail['trans_month_cnt'] = None
            detail['trans_gap_avg'] = None
            all_detail[amt_order] += detail.to_dict('records')
        return all_detail
