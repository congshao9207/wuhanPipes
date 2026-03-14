import pandas as pd
from util.mysql_reader import sql_to_df
from view.TransFlow import TransFlow


class MeanInterestBalance(TransFlow):

    def process(self):
        sql = """
                    select trans_amt, trans_time, account_balance, remark
                    from trans_flow
                    where account_id = %(account_id)s
                """
        data = sql_to_df(sql=sql, params={"account_id": self.account_id})
        mean_interest = interest_executor(data)
        mean_balance = balance_executor(data)
        rat = get_rat(mean_interest, mean_balance)
        if (mean_interest is None) or (mean_balance is None) or (rat is None):
            self.variables['mean_interest_balance'] = '[]'
            return
        result = pd.DataFrame({'mean_interest': mean_interest.values, 'mean_balance': mean_balance.values,
                               'interest_balance_proportion': rat})
        if result[pd.isna(result['half'])].shape[0] == result.shape[0]:
            result.drop(['half', 'year'], axis=1, inplace=True)
        elif result[pd.isna(result['year'])].shape[0] == result.shape[0]:
            result.drop('year', axis=1, inplace=True)
        self.variables['mean_interest_balance'] = result.to_json(orient='index')


def interest_executor(data):
    df = data.copy()
    if 1 - df.empty:
        df.trans_time = pd.to_datetime([str(t)[:10] for t in df.trans_time], format='%Y-%m-%d')
        st_df = df[
            (df.trans_amt > 0)
            & (df.remark.str.contains('利息|结息|个人活期结息|批量结息|存息|付息|存款利息|批量业务'))
            & 1 - (df.remark.str.contains('保证金|透支|转账|智能|其他账户|税后|转存'))
            ]
        if 1 - st_df.empty:
            for t in st_df.trans_time:
                if t.month % 3 != 0 or t.day not in [20, 21, 22, 23]:
                    return None
            if len(st_df.trans_time) == len(set(st_df.trans_time)):
                mean_interest = st_df['trans_amt'] * 2000
                mean_interest.index = [str(t)[:7] for t in st_df.trans_time]
                if len(mean_interest) >= 4:
                    mean_interest['half'] = st_df.trans_amt[-2:].mean() * 2000
                    mean_interest['year'] = st_df.trans_amt[-4:].mean() * 2000
                elif len(mean_interest) >= 2:
                    mean_interest['half'] = st_df.trans_amt[-2:].mean() * 2000
                    mean_interest['year'] = None
                return mean_interest
    return None


def balance_executor(trans_df):
    df = trans_df.copy()
    if 1 - df.empty:
        df.trans_time = pd.to_datetime([str(t)[:10] for t in df.trans_time], format='%Y-%m-%d')
        st_df = df[
            (df.trans_amt > 0)
            & (df.remark.str.contains('利息|结息|个人活期结息|批量结息|存息|付息|存款利息|批量业务'))
            & 1 - (df.remark.str.contains('保证金|透支|转账|智能|其他账户|税后|转存'))
            ]
        if 1 - st_df.empty:
            for t in st_df.trans_time:
                if t.month % 3 != 0 or t.day not in [20, 21, 22, 23]:
                    return None
            if len(st_df.trans_time) == len(set(st_df.trans_time)):
                ori_interest = st_df.copy()
                ori_interest.index = range(st_df.shape[0])
                temp_df = df.copy()
                temp_df.drop_duplicates(subset=['trans_time'], keep='last', inplace=True)
                temp_df.index = temp_df.trans_time
                time_index = pd.date_range(start=ori_interest.trans_time.min(),
                                           end=ori_interest.trans_time.max())
                temp_flow = pd.DataFrame(index=time_index, columns=['account_balance'])
                for t in temp_df.index:
                    temp_flow.loc[t, 'account_balance'] = temp_df.loc[t, 'account_balance']
                temp_flow.fillna(method='ffill', inplace=True)
                mean_balance = pd.Series()
                mean_balance.loc[str(ori_interest.trans_time[0])[:7]] = None
                for i in range(ori_interest.shape[0] - 1):
                    start = ori_interest.trans_time[i]
                    end = ori_interest.trans_time[i + 1]
                    mean_balance.loc[str(end)[:7]] = temp_flow[(temp_flow.index >= start)
                                                               & (temp_flow.index < end)].values.mean()
                if len(ori_interest) >= 5:
                    mean_balance['half'] = temp_flow[(temp_flow.index >= list(ori_interest.trans_time)[-3])
                                                     & (temp_flow.index < list(ori_interest.trans_time)[
                        -1])].values.mean()
                    mean_balance['year'] = temp_flow[(temp_flow.index >= list(ori_interest.trans_time)[-5])
                                                     & (temp_flow.index < list(ori_interest.trans_time)[
                        -1])].values.mean()
                elif len(ori_interest) >= 3:
                    mean_balance['half'] = temp_flow[(temp_flow.index >= list(ori_interest.trans_time)[-3])
                                                     & (temp_flow.index < list(ori_interest.trans_time)[
                        -1])].values.mean()
                    mean_balance['year'] = None
                return mean_balance
    return None


def get_rat(mean_interest, mean_balance):
    if (mean_interest is None) and (mean_balance is None):
        return None
    rat = pd.Series()
    for k in mean_interest.to_dict():
        if (mean_interest.get(k) is None) or (mean_balance.get(k) is None):
            rat[k] = None
        else:
            rat[k] = mean_interest.get(k) / mean_balance.get(k)
    return rat
