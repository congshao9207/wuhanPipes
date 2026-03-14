import pandas as pd
import decimal
import re

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)

class TransactionBalance:
    """
    将流水文件中交易余额标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:20200709,流水验真顺序为时间顺序
    updated_time_v2:20200818,进行流水验真时先按照原流水顺序验真,
        若不通过再根据时间顺序(排序方式分别按照日期顺序+时间顺序,日期顺序+时间逆序)
    updated_time_v3:20200918,允许上传余额为负值的流水,依然需要识别形如“-      ”的数据
    updated_time_v4:20201224,流水验真逻辑调整，现在不再是进行四次不同顺序的验证，而是将每一天的流水单独排序，然后进行合并
                    且现在可以识别因为时间缺失或者银行数据存储问题造成的时间顺序错乱的问题，只要每一天的流水都能对上，就视为真流水
    """

    def __init__(self, trans_data, col_mapping, sort_list):
        self.df = trans_data
        self.bal_col = col_mapping['bal_col']
        self.basic_status = True
        self.sort_list = sort_list
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def _notnull_max(self):
        length = len(self.df)
        cnt = self.df[self.bal_col].count()
        max_v = -1
        max_col = ''
        for k, v in cnt.items():
            if v == length:
                return k
            elif v > max_v:
                max_v = v
                max_col = k
        return max_col

    def _remove_bal_col(self):
        """
        去除余额列中不符合规范的列
        :return:
        """
        length = len(self.bal_col)
        for index in range(-length, 0):
            col = self.bal_col[index]
            if re.search(r"(上|Last|last|前)", col):
                self.bal_col.remove(col)
        return

    def _one_col_match(self, col):
        self.df['account_balance'] = self.df[col].fillna('').astype(str). \
                apply(lambda x: re.sub(r'[^\d.-]|.*-$|.+-.+', '', re.sub(r'\s', '', x))).replace('', '0').astype(float)
        return

    def balance_match(self):
        self._remove_bal_col()
        col = self._notnull_max()
        if col != '':
            self._one_col_match(col)
        else:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'] = ['该流水中未找到交易余额列,请检查后再上传']
        return

    def balance_sequence_check(self):
        # 优先进行顺序校验，若已校验通过则不进行智能排序，校验是否通过用self.basic_status判断
        self.balance_sequence_check1()
        if self.basic_status:
            return
        # 重新赋值为True，然后进行后续校验
        self.basic_status = True
        self.df = self.df[self.df['trans_amt'] != 0]
        self.df['last_trans_bal'] = self.df.apply(
            lambda x: float(decimal.Decimal(str(x['account_balance'])) - decimal.Decimal(str(x['trans_amt']))), axis=1)
        self.df['concat_str'] = self.df.apply(
            lambda x: x['trans_channel'] + x['trans_type'] + x['trans_use'] + x['remark'], axis=1)
        sort_df = self.df.sort_values(by='trans_time', ascending=True)
        # sort_df.drop_duplicates(subset=['trans_time', 'trans_amt', 'account_balance', 'remark'], inplace=True)
        sort_df.reset_index(drop=True, inplace=True)
        sort_df['trans_date'] = sort_df['trans_time'].apply(lambda x: x.date())
        date_list = sort_df['trans_date'].unique()
        final_df = pd.DataFrame(columns=sort_df.columns)
        cnt = 0
        last_bal = 0
        index = 0
        for d in date_list:
            index = index + 1
            # logger.info("%d/%d-----------------------%s" % (index, len(date_list), 'balance_sequence_check-1'))
            temp_date_df = sort_df[sort_df['trans_date'] == d]
            temp_date_df['last_trans_bal'] = temp_date_df.apply(
                lambda x: float(decimal.Decimal(str(x['account_balance'])) - decimal.Decimal(str(x['trans_amt']))),
                axis=1)
            if cnt == 0:
                sort_date_df = self.trans_sort(temp_date_df)
                if sort_date_df is False:
                    drop_duplicate_df = temp_date_df.drop_duplicates(subset=['trans_time', 'trans_amt',
                                                                             'account_balance', 'remark'])
                    sort_date_df = self.trans_sort(drop_duplicate_df)
            else:
                sort_date_df = self.trans_sort(temp_date_df, start=last_bal)
                if sort_date_df is False:
                    drop_duplicate_df = temp_date_df.drop_duplicates(subset=['trans_time', 'trans_amt',
                                                                             'account_balance', 'remark'])
                    sort_date_df = self.trans_sort(drop_duplicate_df, start=last_bal)
            if sort_date_df is False:
                if temp_date_df[
                        temp_date_df['concat_str'].str.contains('冲正|抹账|退账|抹帐|退帐|冲帐|冲账')].shape[0] == 0:
                    self.basic_status = False
                    self.resp['resCode'] = '22'
                    self.resp['resMsg'] = '验真失败'
                    self.resp['data']['warningMsg'] = ['该流水存在余额与交易金额不匹配的行']
                    return
                reversed_date_df = temp_date_df.copy()
                reversed_date_df['trans_amt'] = temp_date_df.apply(
                    lambda x: -x['trans_amt'] if re.search('冲正|抹账|退账|抹帐|退帐|冲帐|冲账', x['concat_str'])
                    else x['trans_amt'], axis=1)
                reversed_date_df['last_trans_bal'] = reversed_date_df.apply(
                    lambda x: float(decimal.Decimal(str(x['account_balance'])) - decimal.Decimal(str(x['trans_amt']))),
                    axis=1)
                if cnt == 0:
                    sort_date_df1 = self.trans_sort(reversed_date_df)
                    if sort_date_df1 is False:
                        drop_duplicate_df1 = reversed_date_df.drop_duplicates(subset=['trans_time', 'trans_amt',
                                                                                      'account_balance', 'remark'])
                        sort_date_df1 = self.trans_sort(drop_duplicate_df1)
                else:
                    sort_date_df1 = self.trans_sort(reversed_date_df, start=last_bal)
                    if sort_date_df1 is False:
                        drop_duplicate_df1 = reversed_date_df.drop_duplicates(subset=['trans_time', 'trans_amt',
                                                                                      'account_balance', 'remark'])
                        sort_date_df1 = self.trans_sort(drop_duplicate_df1, start=last_bal)
                if sort_date_df1 is False:
                    self.basic_status = False
                    self.resp['resCode'] = '22'
                    self.resp['resMsg'] = '验真失败'
                    self.resp['data']['warningMsg'] = ['该流水存在余额与交易金额不匹配的行']
                    return
                else:
                    final_df = pd.concat([final_df, sort_date_df1], sort=False)
                    last_bal = sort_date_df1['account_balance'].tolist()[-1]
            else:
                final_df = pd.concat([final_df, sort_date_df], sort=False)
                last_bal = sort_date_df['account_balance'].tolist()[-1]
            cnt += 1
        self.df = final_df

    def trans_sort(self, df, start=None, basic_df=pd.DataFrame(columns=['trans_amt', 'account_balance'])):
        """
        智能排序某一天交易的所有流水，即若因某个交易时间的缺失或者银行数据问题导致交易顺序被打乱，本方法可以将流水顺序智能调整成正确顺序
        :param df:涉及到某一天的流水
        :param start:上一天的最后一条交易流水余额
        :param basic_df:基础数据表
        :return:如果可以，返回排序好的流水记录
                如果不可以，返回False
        """
        if df.shape[0] == 0:
            return True
        if start is None:
            for index in df.index:
                basic_df = df[df.index == index]
                res_df = df[df.index != index]
                if res_df.shape[0] == 0:
                    return basic_df
                index_df = self.trans_sort(res_df, basic_df['account_balance'].tolist()[0])
                if index_df is not False:
                    return pd.concat([basic_df, index_df], sort=False)
                else:
                    continue
            return False
        else:
            temp_df = df[df['last_trans_bal'] == start]
            if temp_df.shape[0] == 0:
                return False
            elif temp_df.shape[0] == 1:
                basic_df = pd.concat([basic_df, temp_df], sort=False)
                res_df = df[df['last_trans_bal'] != start]
                if res_df.shape[0] > 0:
                    another_df = self.trans_sort(res_df, temp_df['account_balance'].tolist()[0])
                    if another_df is not False:
                        return pd.concat([basic_df, another_df], sort=False)
                    else:
                        return False
                else:
                    return basic_df
            else:
                for index in temp_df.index:
                    res_df = df[df.index != index]
                    basic_df = pd.concat([basic_df, temp_df[temp_df.index == index]], sort=False)
                    index_df = self.trans_sort(res_df, temp_df.loc[index, 'account_balance'])
                    if index_df is not False:
                        return pd.concat([basic_df, index_df], sort=False)
                    else:
                        basic_df.drop(index, inplace=True)
                        continue
                return False

    def interest_check(self):
        temp_df = self.df
        for row in temp_df.itertuples():
            trans_amt = getattr(row, "trans_amt")
            op_name = getattr(row, "opponent_name")
            try:
                trans_dt = getattr(row, "trans_date")
            except:
                trans_dt = getattr(row, "trans_time")
            trans_channel = getattr(row, 'trans_channel')
            trans_type = getattr(row, 'trans_type')
            trans_use = getattr(row, 'trans_use')
            remark = getattr(row, 'remark')
            concat_str = trans_channel + trans_type + trans_use + remark

            if str(trans_dt.day) in ['20','21','22'] and str(trans_dt.month) in ['3','6','9','12'] and \
                (re.search('活期|批量业务|付利息',op_name) or re.search('付息|利息存入|结息|入息|利息起息|增值息',concat_str)) and \
                trans_amt < 0 :
                    self.basic_status = False
                    self.resp['resCode'] = '22'
                    self.resp['resMsg'] = '验真失败'
                    self.resp['data']['warningMsg'] = ['该流水存在季度结息为负']
                    return

    # 流水验真逻辑
    @staticmethod
    def _balance_check(df):
        last = -1
        for row in df.itertuples():
            if last == -1:
                last = getattr(row, 'account_balance')
            else:
                index = getattr(row, 'Index')
                trans_amt = getattr(row, 'trans_amt')
                trans_bal = getattr(row, 'account_balance')
                trans_channel = getattr(row, 'trans_channel')
                trans_type = getattr(row, 'trans_type')
                trans_use = getattr(row, 'trans_use')
                remark = getattr(row, 'remark')
                concat_str = trans_channel + trans_type + trans_use + remark

                if float(decimal.Decimal(str(trans_amt)) + decimal.Decimal(str(last))) != trans_bal:
                    # 若某行不满足余额校验,但是该行属于冲正,抹账,退账,则将交易金额乘以-1重新验证一次,若通过则继续往下校验
                    if re.search('冲正|抹账|退账|抹帐|退帐|冲帐|冲账', concat_str) is not None:
                        if float(decimal.Decimal(str(-trans_amt)) + decimal.Decimal(str(last))) == trans_bal:
                            df.loc[index, 'trans_amt'] = -trans_amt
                            last = trans_bal
                            continue
                    return
                last = trans_bal
        return df

    # 进行数次不同顺序的验真
    def balance_sequence_check1(self):
        self.df = self.df[self.df['trans_amt'] != 0]
        sort_df1 = self.df.copy()
        sort_df1['index'] = list(range(len(self.df)))
        self.sort_list.append('index')
        # 1.流水文件中原有顺序顺序验真
        result1 = self._balance_check(sort_df1)
        if result1 is not None:
            self.df = result1
            return
        # 2.流水文件中原有顺序逆序验真
        sort_df2 = sort_df1.sort_values(by='index', ascending=False)
        result2 = self._balance_check(sort_df2)
        if result2 is not None:
            self.df = result2
            self.df.reset_index(drop=True, inplace=True)
            return
        # 3.将流水文件按照交易日期,交易时间,index,顺序,顺序,顺序排序后进行验真(可能存在日期,时间均相同的两条流水)
        ascending_list3 = [True] * len(self.sort_list)
        sort_df3 = sort_df1.sort_values(by=self.sort_list, ascending=ascending_list3)
        result3 = self._balance_check(sort_df3)
        if result3 is not None:
            self.df = result3
            self.df.reset_index(drop=True, inplace=True)
            return
        # 4.将流水文件按照交易日期,交易时间,index,顺序,顺序,逆序排序后进行验真(可能存在日期,时间均相同的两条流水)
        ascending_list3[-1] = False
        sort_df4 = sort_df1.sort_values(by=self.sort_list, ascending=ascending_list3)
        result4 = self._balance_check(sort_df4)
        if result4 is not None:
            self.df = result4
            self.df.reset_index(drop=True, inplace=True)
            return
        # 5.上述逻辑都没有通过,则验真失败
        self.basic_status = False
