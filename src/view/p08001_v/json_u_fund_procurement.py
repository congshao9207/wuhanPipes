import numpy as np
from view.TransFlow import TransFlow
import pandas as pd


class JsonUFundProcurement(TransFlow):

    def process(self):
        self.get_fund_section()

    def get_fund_section(self):
        df = self.trans_u_flow_portrait[self.trans_u_flow_portrait['trans_flow_src_type'] != 1]
        self.variables['trans_risk_money'] = {
            "risk_tips": "",
            "income_section": [],
            "expense_section": []
        }
        if df.empty:
            return
        segment_amt = []
        for i, col in enumerate(['income', 'expense']):
            temp_df = df[df.trans_amt > 0] if i == 0 else df[df.trans_amt < 0]
            if temp_df.shape[0] == 0:
                segment_amt.extend([0, 1])
                continue
            temp_df['trans_amt'] = temp_df['trans_amt'].apply(lambda x: abs(x) if pd.notna(x) else 0)
            max_amt = int(max(temp_df.trans_amt) / 10000) + 1
            segment = 1 if temp_df.shape[0] == 0 else max(round(np.percentile(temp_df.trans_amt,
                                                                              90, interpolation='linear') / 10000), 1)
            if segment == 1:
                segment_amt.extend([0, 1])
                section_list = [0, 1, 3, 5, max_amt]
            elif segment <= 5:
                segment_amt.extend([0, 5])
                section_list = [0, 1, 3, 5, max_amt]
            else:
                diff = 2 if segment <= 10 else 5 if segment <= 40 else 10
                section_list = [0, int(segment - 2 * diff), int(segment - diff), int(segment), max_amt]
                segment_amt.extend([section_list[-3], section_list[-2]])
            temp_list = []
            if len(section_list) > 0:
                for _ in range(4):
                    var = f'{section_list[_]}-{section_list[_ + 1]}万(含)'
                    if _ == 3 and section_list[_ + 1] != 5:
                        var = f'{section_list[_]}万以上'
                    temp_cnt = temp_df[(temp_df.trans_amt > section_list[_] * 10000) &
                                       (temp_df.trans_amt <= section_list[_ + 1] * 10000)].shape[0]
                    temp_dict = {'order': _ + 1,
                                 f'{col}_section_variable': var,
                                 f'{col}_section_cnt': temp_cnt,
                                 f'{col}_cnt_proportion': temp_cnt / temp_df.shape[0]}
                    temp_list.append(temp_dict)
            self.variables['trans_risk_money'][f'{col}_section'] = temp_list
        if max(segment_amt[1], segment_amt[3]) == 1:
            risk = '资金调动能力不足1万'
        else:
            segment_amt[1], segment_amt[3] = max(segment_amt[1], 5), max(segment_amt[3], 5)
            risk = f'进账资金调动能力是{segment_amt[0]}-{segment_amt[1]}万，' \
                   f'出账资金调动能力是{segment_amt[2]}-{segment_amt[3]}万，' \
                   f'最大资金调动能力是{max(segment_amt[0], segment_amt[2])}-{max(segment_amt[1], segment_amt[3])}万'
        self.variables['trans_report_overview']['business_info']['money_mobilize_ability']['risk_tips'] = risk
        self.variables['suggestion_and_guide']['business_info']['money_mobilize_ability']['risk_tips'] = \
            risk.split('，')[-1] + "，可根据最大资金调动能力调整客户授信额度"
        self.variables['trans_risk_money']['risk_tips'] = risk + ";注：资金调动能力为去除部分大额交易后的最大交易金额区间"
