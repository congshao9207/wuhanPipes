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
        # 只调用一次 get_u_flow_portrait_detail，避免重复执行 3 次相同的数据预处理
        df = self.get_u_flow_portrait_detail()
        if df.shape[0] == 0:
            return
        self.process_manual_management_and_suspected_affiliates(df)

    # 定义函数，处理“手动管理和疑似关联”的名单
    def process_manual_management_and_suspected_affiliates(self, df):
        manual_management_detail = self._get_opponent_detail(df, 'manual_management')
        suspected_affiliates_detail = self._get_opponent_detail(df, 'suspected_affiliates')
        related_opponent_detail = self._get_opponent_detail(df, 'related_opponent')
        self.variables['opponent_info']['related_opponent'] = related_opponent_detail
        self.variables['opponent_info']['manual_management'] = manual_management_detail
        self.variables['opponent_info']['suspected_affiliates'] = suspected_affiliates_detail

    def get_u_flow_portrait_detail(self):
        # 取联合版块的画像数据
        df = self.trans_u_flow_portrait.copy()
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 缓存日期转换结果，避免重复转换
        trans_date_dt = pd.to_datetime(df['trans_date'])
        year_ago = trans_date_dt.max() - DateOffset(months=12)
        # 筛选近一年数据
        df = df.loc[trans_date_dt >= year_ago].copy()
        if df.shape[0] == 0:
            return pd.DataFrame()
        # 新增交易年-月列
        df['trans_month'] = df['trans_date'].apply(lambda x: x.strftime('%Y-%m'))
        # 添加交易对手类型（向量化 apply 仅作用于单列，避免 axis=1 全表逐行调用）
        df['oppo_name'] = df['opponent_name'].fillna('').apply(self._oppo_name_optimize)
        df['oppo_type'] = df['oppo_name'].apply(self._oppo_type)
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
    def _oppo_type(oppo_name):
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

    def _get_opponent_detail(self, df, opponent_type: str):
        if df.shape[0] == 0:
            return []

        total_income_amt = df.loc[df.trans_amt > 0, 'trans_amt'].sum()
        total_expense_amt = df.loc[df.trans_amt < 0, 'trans_amt'].abs().sum()
        related_opponent_dict = {}

        # 根据给定的opponent_type，筛选出对应的数据子集
        if opponent_type == 'manual_management':
            mask = pd.notna(df['oppo_name']) & (df['oppo_name'] != '')
            temp_df = df.loc[mask].copy()
            temp_df['trans_amt'] = temp_df['trans_amt'].abs()
            df_grouped = temp_df.groupby('oppo_name')['trans_amt'].sum()
            df_grouped = df_grouped.sort_values(ascending=False)
            top10 = df_grouped.index[:10].tolist()
            df = df[df['oppo_name'].isin(top10)]
        elif opponent_type == 'suspected_affiliates':
            valid_mask = df['compatibility_label'].apply(self.has_valid_code)
            mask = valid_mask & pd.notna(df['oppo_name']) & (df['oppo_name'] != '')
            df = df.loc[mask]
        else:
            # related_opponent
            for item in self.cached_data['input_param']:
                related_opponent_dict[item['name']] = 1 if item['userType'] == 'PERSONAL' else 2
            df = df[df['oppo_name'].isin(related_opponent_dict.keys())]

        if df.shape[0] == 0:
            return []

        # ----- 向量化计算：一次性用 groupby 算出所有对手的汇总指标 -----
        # 计算每个对手的收支总额  (替代循环中 5 次 df.loc[df['oppo_name'] == x])
        def income_sum(s):
            return s[s > 0].sum()
        def expense_sum(s):
            return s[s < 0].abs().sum()

        oppo_stats = df.groupby('oppo_name')['trans_amt'].agg([income_sum, expense_sum]).reset_index()
        oppo_stats.columns = ['oppo_name', 'income_amt', 'expense_amt']
        oppo_stats['diff_amt'] = (oppo_stats['income_amt'] - oppo_stats['expense_amt']).abs()

        # 对手类型（每个对手取第一个值）
        oppo_types = df.groupby('oppo_name')['oppo_type'].first().to_dict()

        # 按月收支明细：一次性 groupby (oppo_name, trans_month)
        monthly = df.groupby(['oppo_name', 'trans_month'])['trans_amt'].agg(
            income=income_sum, expense=expense_sum
        ).reset_index()

        # ----- 构建输出（此时循环内无 DataFrame 过滤操作）-----
        opponent_info_list = []
        for _, row in oppo_stats.iterrows():
            opponent = row['oppo_name']
            income_amt = row['income_amt']
            expense_amt = row['expense_amt']

            income_amt_prop = income_amt / total_income_amt if total_income_amt > 0 else 0
            expense_amt_prop = expense_amt / total_expense_amt if total_expense_amt > 0 else 0

            if opponent_type != 'related_opponent':
                oppo_type = int(oppo_types.get(opponent, 1))
            else:
                oppo_type = related_opponent_dict.get(opponent, 1)

            # 取出该对手的月度明细
            m = monthly[monthly['oppo_name'] == opponent]
            opponent_trans_detail = [
                {
                    'trans_month': r['trans_month'],
                    'income_amt': round(r['income'] / 10000, 2),
                    'expense_amt': round(r['expense'] / 10000, 2),
                }
                for _, r in m.iterrows()
            ]

            opponent_info_list.append({
                'opponent_name': opponent,
                'income_amt': round(income_amt / 10000, 2),
                'expense_amt': round(expense_amt / 10000, 2),
                'diff_amt': round(row['diff_amt'] / 10000, 2),
                'income_amt_prop': round(income_amt_prop, 4),
                'expense_amt_prop': round(expense_amt_prop, 4),
                "opponent_type": oppo_type,
                'opponent_trans_detail': opponent_trans_detail,
            })
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
