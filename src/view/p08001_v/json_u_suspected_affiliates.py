import pandas as pd
from view.TransFlow import TransFlow
from pandas import DateOffset
import re


class JsonUSuspectedAffiliates(TransFlow):
    """
    This class is used to generate the JSON file of suspected affiliates for a given date range.
    """

    def process(self):
        self.variables['opponent_info'] = {
            "manual_management": [],
            "suspected_affiliates": [],
            "related_opponent": []
        }
        self.process_manual_management_and_suspected_affiliates()

    # 定义函数，处理“手动管理和疑似关联”的名单
    def process_manual_management_and_suspected_affiliates(self):
        manual_management_detail = self._get_opponent_detail('manual_management')
        suspected_affiliates_detail = self._get_opponent_detail('suspected_affiliates')
        related_opponent_detail = self._get_opponent_detail('related_opponent')
        self.variables['opponent_info']['related_opponent'] = related_opponent_detail
        self.variables['opponent_info']['manual_management'] = manual_management_detail
        self.variables['opponent_info']['suspected_affiliates'] = suspected_affiliates_detail

    def get_u_flow_portrait_detail(self):
        # 取联合版块的画像数据
        df = self.trans_u_flow_portrait.copy()
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 获取一年前的日期
        year_ago = pd.to_datetime(df['trans_date']).max() - DateOffset(months=12)
        # 新增交易年-月列
        df['trans_month'] = df.trans_date.apply(lambda x: x.strftime('%Y-%m'))
        # 筛选近一年数据
        df = df.loc[pd.to_datetime(df.trans_date) >= year_ago]
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 添加交易对手类型
        df['oppo_name'] = df['opponent_name'].fillna('').apply(self._oppo_name_optimize)
        df['oppo_type'] = df.apply(lambda x: self._oppo_type(
            x['oppo_name'], x['trans_flow_src_type']), axis=1)
        # 临时处理，填充oppo_type为空和oppo_type为unknown的为1
        df['oppo_type'] = df['oppo_type'].apply(lambda x: 1 if x is None or x == 'unknown' or pd.isna(x) else x)
        return df

    @staticmethod
    def _oppo_name_optimize(oppo_name):
        """
        若存在 000/xxx类交易对手，将数字替换为''
        :param oppo_name:
        :return:
        """
        if re.search(r'/', oppo_name) is not None:
            oppo_name = ''.join([re.sub(r'[0-9]', '', i) for i in oppo_name.split('/')])
        return oppo_name

    @staticmethod
    def _oppo_type(oppo_name, trans_flow_src_type):
        """
        判定交易对手类别
        :param oppo_name:
        :return:
        """
        # 企业
        ENT_TYPE = r"(厂|店|公司|经营部|门市|中心|局|厅|院|部)$"
        # 开头关键字
        TYPE_START_1 = r"(支付宝|消费支付宝|淘宝)"
        TYPE_START_2 = r"(转账|跨行转出|对私提)"
        # 剔除关键字
        TYPE_EXCEPT_1 = r"[^\u4e00-\u9fa5 *]|支付宝转账|支付宝代发"
        TYPE_EXCEPT_2 = r"(支付宝外部商户|支付宝划账|支付宝| |消费支付宝|淘宝)"
        TYPE_EXCEPT_3 = r"(转|贷|消费|自取|资金|自定义|友宝|分期|肯德基|代付|麦当劳|携程|红包|活期|房租" \
                        r"|过渡|必胜客|理财|缴费|工资|特约|还款|充值|京东|星巴克|银联|拼多多|爱奇艺|采购" \
                        r"|天猫|租金|提现|淘宝|\*\*|备用|撤销|花呗|借呗|余额宝|全家)|[费款税账]$"
        TYPE_EXCEPT_4 = r"[财存天停大柜订百本宝网保北电放还好汇结借跨理利内其上深浙税现中微短发卡随有月油退收快取]"

        oppo_name = JsonUSuspectedAffiliates._oppo_name_optimize(oppo_name)
        # 分银行流水和微信支付宝流水来判断
        if trans_flow_src_type is None or trans_flow_src_type == 1 or trans_flow_src_type == '1':
            if len(oppo_name) > 6 and re.search(ENT_TYPE, oppo_name) is not None:
                return 2
            elif len(oppo_name) <= 15:
                cleaned_name = re.sub(TYPE_EXCEPT_1, '', oppo_name)
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
            else:
                return 'unknown'
        else:
            if len(oppo_name) > 6 and re.search(ENT_TYPE, oppo_name) is not None:
                return 2
            elif len(oppo_name) <= 15:
                cleaned_name = re.sub(TYPE_EXCEPT_1, '', oppo_name)
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
            else:
                return 'unknown'

    def _get_opponent_detail(self, opponent_type: str):
        df = self.get_u_flow_portrait_detail()
        if df.shape[0] == 0:
            return []
        opponent_info_list = []
        total_income_amt = df.loc[df.trans_amt > 0]['trans_amt'].sum()
        total_expense_amt = df.loc[df.trans_amt < 0]['trans_amt'].abs().sum()
        related_opponent_dict = {}
        # opponent_init_type = 'manual_management'
        # if opponent_init_type == opponent_type:
        #     opponent_init_type = opponent_type
        # else:
        #     opponent_init_type = 'suspected_affiliates'
        # 根据给定的opponent_type，筛选出对应的opponent_list
        if opponent_type == 'manual_management':
            temp_df = df.copy()
            # 交易对手按金额和出账金额求和
            temp_df['trans_amt'] = temp_df['trans_amt'].abs()
            # 剔除空值
            temp_df = temp_df.loc[pd.notna(temp_df['oppo_name']) & (temp_df['oppo_name'] != '')]
            df_grouped = temp_df.groupby('oppo_name').agg({'trans_amt': 'sum'})
            df_grouped.sort_values(by='trans_amt', ascending=False, inplace=True)
            # 取前十
            opponent_list = df_grouped.index.to_list()[:10]
        elif opponent_type == 'suspected_affiliates':
            temp_df = df.loc[df['compatibility_label'].apply(self.has_valid_code)]
            temp_df = temp_df.loc[pd.notna(temp_df['oppo_name']) & (temp_df['oppo_name'] != '')]
            opponent_list = temp_df['oppo_name'].unique().tolist()
        else:
            opponent_list = []
            for item in self.cached_data['input_param']:
                opponent_list.append(item['name'])
                related_opponent_dict[item['name']] = 1 if item['userType'] == 'PERSONAL' else 2
        for opponent in opponent_list:
            opponent_detail_df = df.loc[df['oppo_name'] == opponent]
            if opponent_detail_df.shape[0] == 0:
                continue
            income_amt = df.loc[(df['oppo_name'] == opponent) & (df['trans_amt'] > 0)]['trans_amt'].sum()
            expense_amt = df.loc[(df['oppo_name'] == opponent) & (df['trans_amt'] < 0)]['trans_amt'].abs().sum()
            diff_amt = abs(income_amt - expense_amt)
            income_amt_prop = income_amt / total_income_amt if total_income_amt > 0 else 0
            expense_amt_prop = expense_amt / total_expense_amt if total_expense_amt > 0 else 0
            if opponent_type != 'related_opponent':
                oppo_type = int(df.loc[df['oppo_name'] == opponent]['oppo_type'].values[0])
            else:
                oppo_type = related_opponent_dict[opponent]

            def func1(x):
                return round(x[x > 0].sum() / 10000, 2)

            def func2(x):
                return round(abs(x[x < 0].sum()) / 10000, 2)

            info_detail_df = df.loc[(df['oppo_name'] == opponent)].groupby(
                'trans_month').agg({'trans_amt': [func1, func2]}).reset_index()
            info_detail_df.columns = ['trans_month', 'income_amt', 'expense_amt']
            temp_dict = {
                'opponent_name': opponent,
                'income_amt': round(income_amt / 10000, 2),
                'expense_amt': round(expense_amt / 10000, 2),
                'diff_amt': round(diff_amt / 10000, 2),
                'income_amt_prop': round(income_amt_prop, 4),
                'expense_amt_prop': round(expense_amt_prop, 4),
                "opponent_type": oppo_type,
                'opponent_trans_detail': info_detail_df.to_dict(orient='records')
            }
            opponent_info_list.append(temp_dict)
        return opponent_info_list

    @staticmethod
    def has_valid_code(label):
        if pd.isna(label):
            return False
        labels = label.split(',')
        for l in labels:
            if len(l) == 10 and l[-6:] == '030103':
                return True
        return False
