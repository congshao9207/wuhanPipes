
from fileparser.trans_flow.trans_config import INCOME_PATTERN, OUTCOME_PATTERN, OUTCOME_FULL_PATTERN
import pandas as pd
import re


class TransactionAmt:
    """
    将流水文件中交易金额标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:20200911,搜索标签列时,要同时包含进账关键字和出账关键字,避免只有一类关键字;去除金额列不符合要求的字符的时候
        先删除空格,再删除负号结尾的字符
    updated_time_v2:20201223,将所有正则匹配格式都纳入配置文件
    """

    def __init__(self, trans_data, col_mapping):
        self.df = trans_data
        self.amt_col = col_mapping['amt_col']
        self.basic_status = True
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def _find_tag_col(self):
        """
        在金额列中寻找是否有标签列,如果存在标签列则新增一列'tag'将标签列对应的出账表示为-1,进账表示为1
        :return:
        """
        length = len(self.amt_col)
        for index in range(-length, 0):
            col = self.amt_col[index]
            temp = self.df[col].value_counts()
            index = ''.join([str(_) for _ in temp.index])
            if len(temp) == 2 and re.search(INCOME_PATTERN, index) and re.search(OUTCOME_PATTERN, index):
                tag = col
                self.df['tag'] = self.df[tag].astype(str).apply(lambda x: re.sub(OUTCOME_FULL_PATTERN, '1', x)).\
                    apply(lambda x: 1 if x != '1' else -1)
                self.amt_col.remove(col)
                return 1
            if col == '交易类型':
                self.amt_col.remove(col)
                self.amt_col.append(col)
        return 0

    def _remove_amt_col(self):
        """
        去除金额列中不符合规范的列
        :return:
        """
        length = len(self.amt_col)
        for index in range(-length, 0):
            col = self.amt_col[index]
            # 将每个金额列数据类型都替换为字符串,先将字符串中的空格都替换为空,再将字符串中的非数字小数点负号替换为空,或者以负号结尾的数据替换为空
            self.df[col] = self.df[col].fillna('').astype(str).apply(lambda x: re.sub(r'[^\d.-]|.*-$',
                                                                                      '', re.sub(r'\s', '', x)))
            # 若转化过后整列全都是空字符串则删除该列
            temp = self.df[col].value_counts()
            if len(temp) == 1 and temp.index[0] == '':
                self.amt_col.remove(col)
        # 若金额列依然包含两列以上,则筛选其中的对立列
        if len(self.amt_col) > 2:
            string = ''.join([str(x) for x in self.amt_col])
            if re.search(INCOME_PATTERN, string) and re.search(OUTCOME_PATTERN, string):
                for index in range(-len(self.amt_col), 0):
                    col = self.amt_col[index]
                    if not (re.search(INCOME_PATTERN, col) or re.search(OUTCOME_PATTERN, col)):
                        self.amt_col.remove(col)
                    elif len(self.df[col].value_counts().index) == 1:
                        self.amt_col.remove(col)
        return

    def _one_col_match(self, col: str, col_name='trans_amt'):
        """
        将对应的金额列转化为标准浮点型数据
        :param col: 需要转化的列名
        :param col_name: 转化后的列名
        :return:
        """
        self.df[col_name] = self.df[col].apply(lambda x: float(re.sub(r'[^\d.-]', '', str(x)))
                                               if str(x).strip() != '' else 0)
        return

    def _multi_col_match(self):
        """
        交易金额列存在多列时的处理
        :return:
        """
        tag = self._find_tag_col()
        self._remove_amt_col()
        for col in self.amt_col:
            self.df[col] = self.df[col].apply(lambda x: float(re.sub(r'[^\d.-]', '', str(x)))
                                              if str(x).strip() != '' else 0)
        length = len(self.amt_col)
        if length == 1:
            if tag:
                if self.df.loc[self.df['tag'] == -1][self.amt_col[0]].sum() < 0:
                    self.df['trans_amt'] = self.df[self.amt_col[0]]
                else:
                    self.df['trans_amt'] = self.df['tag']*self.df[self.amt_col[0]]
            else:
                if len(self.df.loc[self.df[self.amt_col[0]] < 0]) > 0:
                    self.df['trans_amt'] = self.df[self.amt_col[0]]
                else:
                    raise ValueError("未找到交易金额列")
        elif length == 2:
            if tag:
                self.df['trans_amt'] = self.df[self.amt_col[0]] + self.df[self.amt_col[1]]
                if self.df.loc[self.df['tag'] == -1]['trans_amt'].sum() >= 0:
                    self.df['trans_amt'] = self.df['tag'] * self.df['trans_amt']
            else:
                if re.search(OUTCOME_PATTERN, self.amt_col[0]):
                    # neg = len(self.df.loc[self.df[self.amt_col[0]] < 0])
                    neg = self.df[self.amt_col[0]].sum()
                    multi = 1 if neg < 0 else -1
                    self.df['trans_amt'] = multi * self.df[self.amt_col[0]] + self.df[self.amt_col[1]]
                elif re.search(OUTCOME_PATTERN, self.amt_col[1]):
                    # neg = len(self.df.loc[self.df[self.amt_col[1]] < 0])
                    neg = self.df[self.amt_col[1]].sum()
                    multi = 1 if neg < 0 else -1
                    self.df['trans_amt'] = self.df[self.amt_col[0]] + multi * self.df[self.amt_col[1]]
                else:
                    raise ValueError("存在多列无法区分的交易金额列")
        else:
            raise ValueError("未找到交易金额列")
        return

    def amt_match(self):
        length = len(self.amt_col)
        try:
            if length == 1:
                self._one_col_match(self.amt_col[0])
            else:
                self._multi_col_match()
        except ValueError as e:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'].append(e)
