import pandas as pd


class SunstreakerFeature:
    """
    将报告中的指标输出到strategyInputVariables下，供智能微贷调用
    """

    def __init__(self, response_data):
        self.response = response_data

    def process(self):
        self.marketing_feedback_feature()
        self.counterparty_info_feature()
        return self.response

    def marketing_feedback_feature(self):
        """
        营销反哺指标
        20240607 产品确认，仅考虑对公和对私的进账客户输出
        对私按笔数和金额倒叙取前5，对公按笔数和金额倒叙取前10
        :return:
        """
        com_info, per_info = pd.DataFrame(), pd.DataFrame()
        if self.response.__contains__('subject'):
            for subject in self.response['subject']:
                if subject['queryData']['relation'] != "MAIN":
                    continue
                marketing_feedback_info = subject['reportDetail'][0]['variables']['营销反哺']
                for key, value in marketing_feedback_info.items():
                    if '对公进账' in str(key) and len(value) > 0:
                        temp_df = pd.DataFrame(value)
                        com_info = pd.concat([com_info, temp_df])
                        com_info['recommended_reason'] = '营销反哺'
                        com_info['recommended_products'] = ''
                    elif '对私进账' in str(key) and len(value) > 0:
                        temp_df = pd.DataFrame(value)
                        per_info = pd.concat([per_info, temp_df])
                        per_info['recommended_reason'] = '营销反哺'
                        per_info['recommended_products'] = ''
                use_col = ['opponent_name', 'recommended_reason', 'recommended_products']
                if com_info.shape[0] > 0:
                    com_info = com_info.sort_values(by=['trans_cnt', 'trans_amt'], ascending=False).head(10)
                    com_info = com_info[use_col]
                if per_info.shape[0] > 0:
                    per_info = per_info.sort_values(by=['trans_cnt', 'trans_amt'], ascending=False).head(5)
                    per_info = per_info[use_col]
                subject['queryData']['strategyInputVariables']['marketing_feedback'] = \
                    {"com_info": com_info.to_dict(orient='records'),
                     "per_info": per_info.to_dict(orient='records')}
                break

        # 判断，若最后不存在marketing_feedback节点，则新增
        if self.response.__contains__('subject'):
            for subject in self.response['subject']:
                if subject['queryData']['relation'] != "MAIN":
                    continue
                if subject['queryData']['strategyInputVariables'].__contains__('marketing_feedback'):
                    break
                else:
                    subject['queryData']['strategyInputVariables']['lite_counterparty'] = {"com_info": [], "per_info": []}

    def counterparty_info_feature(self):
        """
        上下游客户信息
        :return:
        """
        # 处理节点，将上下游内容，新增到strategyInputVariables下
        if self.response.__contains__('subject'):
            subject_arr = self.response['subject']
            if len(subject_arr) > 0:
                for subject in subject_arr:
                    if subject['queryData']['relation'] == "MAIN":
                        subject['queryData']['strategyInputVariables']['lite_counterparty'] = \
                            subject['reportDetail'][0]['variables']['lite_counterparty']
        # 判断，若最后不存在lite_counterparty节点，则新增
        if self.response.__contains__('subject'):
            for subject in self.response['subject']:
                if subject['queryData']['relation'] != "MAIN":
                    continue
                if subject['queryData']['strategyInputVariables'].__contains__('lite_counterparty'):
                    break
                else:
                    subject['queryData']['strategyInputVariables']['lite_counterparty'] = {'lite_expense': [], 'lite_income': []}
                    