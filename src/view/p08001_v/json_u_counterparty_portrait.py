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
        # 缓存日期转换结果，避免重复转换
        trans_date_dt = pd.to_datetime(flow_df['trans_date'])
        year_ago = trans_date_dt.max() - DateOffset(months=12)
        opponent_name_str = flow_df['opponent_name'].astype(str)

        # 筛选近一年经营性流水
        flow_df = flow_df[(pd.isnull(flow_df['relationship'])) &
                          (flow_df['is_sensitive'] != 1) &
                          (pd.notnull(flow_df['opponent_name'])) &
                          (trans_date_dt >= year_ago) &
                          (~opponent_name_str.str.isnumeric()) &
                          (~opponent_name_str.str.contains('|'.join(UNUSUAL_OPPO_NAME)))]
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
        # 先统一转为字符串
        flow_df['opponent_name'] = flow_df['opponent_name'].astype(str)
        # 仅对有HTML实体的行进行解码（&#xxx; → chr），罕见情况不做全表扫描
        has_entity = flow_df['opponent_name'].str.contains(r'&#\d{2,5};')
        if has_entity.any():
            def _decode_entity(s):
                return re.sub(r'&#(\d{2,5});', lambda m: chr(int(m.group(1))), s)
            flow_df.loc[has_entity, 'opponent_name'] = flow_df.loc[has_entity, 'opponent_name'].apply(_decode_entity)
        # 只保留中文字符（向量化操作，比逐行apply快数倍）
        flow_df['opponent_name'] = flow_df['opponent_name'].str.replace(r'[^\u4e00-\u9fa5]', '', regex=True)
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
        income_df = flow_df[flow_df.trans_amt > 0].copy()
        expense_df = flow_df[flow_df.trans_amt < 0].copy()

        #  剔除交易对手既是上游客户，也是下游客户
        #  向量化处理：用 groupby 一次算出每个交易对手的总收支，避免 O(n²) 循环
        income_sum = income_df.groupby('opponent_name')['trans_amt'].sum()
        expense_sum = expense_df.groupby('opponent_name')['trans_amt'].sum().abs()
        common_names = income_sum.index.intersection(expense_sum.index)

        superposition_df = flow_df.loc[flow_df['opponent_name'].isin(common_names)]

        if len(common_names) > 0:
            # 一次判断每个交叉名称归入收入还是支出方
            income_side = common_names[income_sum[common_names] >= expense_sum[common_names]]
            expense_side = common_names[income_sum[common_names] < expense_sum[common_names]]
            # 用布尔索引一次性过滤，避免逐行 drop
            income_df = income_df[~income_df['opponent_name'].isin(common_names) |
                                  income_df['opponent_name'].isin(income_side)]
            expense_df = expense_df[~expense_df['opponent_name'].isin(common_names) |
                                    expense_df['opponent_name'].isin(expense_side)]

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

    def in_out_detail(self, df):
        return self._build_detail(df, is_superposition=False)

    def superposition_detail(self, df):
        return self._build_detail(df, is_superposition=True)

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
    def _build_detail(df, is_superposition=False):
        """进出帐明细构建（合并 in_out_detail 和 superposition_detail 的公共逻辑）"""
        all_detail = {}
        in_out_type = 'income_amt_order'
        out_in_type = 'expense_amt_order'
        if df['trans_amt'].sum() < 0:
            in_out_type = 'expense_amt_order'
            out_in_type = 'income_amt_order'

        # 平均账期函数
        def gap_avg(date):
            all_unique_trans_date = sorted(set(date.to_list()))
            diff_days = [(all_unique_trans_date[i + 1] - all_unique_trans_date[i]).days - 1
                         for i in range(len(all_unique_trans_date) - 1)]
            diff_days = [x for x in diff_days if x != 0]
            return sum(diff_days) / len(diff_days) if diff_days else 0

        df['trans_amt'] = df['trans_amt'].abs()
        order_df = df.groupby('opponent_name').aggregate(
            {'trans_amt': ['sum', 'count', 'mean'],
             'calendar_month': ['nunique'],
             'trans_date': [gap_avg]}).reset_index()
        order_df.columns = ['opponent_name', 'trans_amt', 'trans_cnt', 'trans_mean', 'trans_month_cnt', 'trans_gap_avg']

        # 剔除交易总额1000以下的客户
        order_df = order_df[order_df['trans_amt'] > UP_DOWNSTREAM_THRESHOLD]
        order_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        order_df.reset_index(inplace=True, drop=True)
        income_total_amt = order_df['trans_amt'].sum()
        order_df['trans_amt_proportion'] = \
            order_df['trans_amt'] / income_total_amt if income_total_amt != 0 else 0
        order_df = order_df.iloc[:10, ]
        order_index_col = 'amt_order' if is_superposition else in_out_type
        order_df[order_index_col] = (order_df.index + 1).astype(str)
        order_df[in_out_type] = order_df[order_index_col]
        order_df['month'] = '汇总'
        order_df[out_in_type] = None
        order_df['income_amt_proportion'] = None

        # 汇总行
        for idx, row in enumerate(order_df.to_dict('records')):
            all_detail[str(idx + 1)] = [row]

        # 按月明细：预构建 dict 避免循环内重复过滤 DataFrame
        order_df_detail = df.groupby(['opponent_name', 'calendar_month']).agg(
            trans_amt=('trans_amt', 'sum'),
            trans_cnt=('trans_amt', 'count')
        ).reset_index().rename(columns={"calendar_month": "month"})
        detail_by_name = {name: grp for name, grp in order_df_detail.groupby('opponent_name')}

        full_df = pd.DataFrame({'month': [str(_ + 1) for _ in range(13)]})
        for order in range(order_df.shape[0]):
            amt_order = str(order + 1)
            opponent_name = order_df.iloc[order]['opponent_name']
            detail = detail_by_name.get(opponent_name, pd.DataFrame())
            detail = full_df.merge(detail, how='left', on='month')
            detail['opponent_name'] = opponent_name
            detail['trans_amt'] = detail['trans_amt'].fillna(0)
            detail['trans_cnt'] = detail['trans_cnt'].fillna(0)
            detail[order_index_col] = amt_order
            detail[in_out_type] = amt_order
            detail[out_in_type] = None
            detail['income_amt_proportion'] = None
            detail['trans_gap_avg'] = None
            detail['trans_amt_proportion'] = None
            detail['trans_mean'] = None
            detail['trans_month_cnt'] = None
            all_detail[amt_order] += detail.to_dict('records')
        return all_detail
