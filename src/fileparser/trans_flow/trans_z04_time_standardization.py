
from fileparser.trans_flow.trans_config import MIN_IMPORT_INTERVAL, MIN_QUERY_INTERVAL, MIN_TRANS_INTERVAL, \
    DTTIME_PATTERN, TIME_PATTERN, DATE_PATTERN, TIME_S_PATTERN
import pandas as pd
import datetime
import re


def dttime_apply(time):
    # 首位是'2',形如'2020-01-01 05:02:04',末尾加上'000000'是为了防止出现秒钟缺失情况
    try:
        temp = format(pd.to_datetime(str(time).replace('\\', '/')), '%Y%m%d%H%M%S')
    except:
        temp = ''.join([_ for _ in time if _.isdigit()])
    if len(temp) == 0:
        raise ValueError("交易日期列存在不符合格式的值t001")
    if temp[0] == '2':
        temp += '000000'
        temp = temp[:14]
        result = datetime.datetime.strptime(temp, '%Y%m%d%H%M%S')
    # 首位是'4',形如'43562.125',表示'2019-04-07 03:00:00'
    elif temp[0] == '4':
        # temp = ''.join([_ for _ in time if _.isdigit()])
        date = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=int(temp[:5]) - 2)
        date_str = datetime.datetime.strftime(date, '%Y%m%d')
        time_str = '000000'
        if len(temp) > 5:
            time_value = int(temp[5:]) / 10 ** (len(temp) - 5)
            if time != 0:
                time_str = str(int(time_value * 24)).rjust(2, '0') + \
                           str(int(time_value * 1440) % 60).rjust(2, '0') + \
                           str(int(time_value * 86400) % 60).rjust(2, '0')
        result = datetime.datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S')
    else:
        raise ValueError("交易日期列有不符合格式的值t001")
    return result


class TransactionTime:
    """
    将流水文件中交易时间标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:20200819找到时间列后不再进行排序,排序放到余额验真里面进行,且若交易时间列存在空值则用上面的值填充,
            不再删除交易时间列含有部门空值的情况
    updated_time_v2:20200911,导入间隔校验时间扩充为45天,相应的导入失败提示也更改为45天
    updated_time_v3:20201125,导入间隔,查询间隔,交易间隔现在都是可配置的,修改起始截止时间校验逻辑
    updated_time_v4:20201223,时间日期格式和日期格式匹配加入了字符串为空的判断，之后不会再出现下标越界的提示
    """

    def __init__(self, trans_data, col_mapping, title_param):
        self.df = trans_data
        self.title_param = title_param
        self.time_col = col_mapping['time_col']
        self.acc_time_col = col_mapping['acc_time_col']
        self.query_start = None
        self.query_end = None
        self.basic_status = True
        self.sort_list = []  # 需要排序的列
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def _query_date_transform(self):
        try:
            self.query_start = dttime_apply(self.title_param.get('start_date'))
            self.query_end = dttime_apply(self.title_param.get('end_date'))
        except (ValueError, TypeError):
            self.query_start = None
            self.query_end = None

    def _notnull_cnt(self, columns):
        length = len(self.df)
        cnt = self.df[columns].count()
        res = list()
        for i, v in cnt.items():
            if v == length:
                res.append(i)
        return res

    def _match_time_head(self, column, pattern, number):
        """
        匹配时间列的前10行(最多)
        :param column: 列名
        :param pattern: 正则表达式
        :param number: 匹配的数字位数
        :return:
        """
        sample = list(self.df[column][:10])
        cnt = 0
        for x in sample:
            try:
                if "/" in x or "\\" in x:
                    x = format(pd.to_datetime(str(x).replace('\\', '/')), '%Y%m%d%H%M%S')
                else:
                    raise ValueError
            except:
                if type(x) == float:
                    x = str(int(x))
                else:
                    x = str(x)
            y = [_ for _ in x if _.isdigit()]
            z = ''.join(y)[:number]
            if number == 6 or number == 4:
                z = z.rjust(number, '0')
            # 一旦出现不匹配的时间格式则返回False
            if re.match(pattern, z) is None:
                return False
            # 日期时间列的时间不能全都是0,否则可能忽略真实的时间列
            if number == 14 and z[-6:] == '000000':
                cnt += 1
        if cnt == len(sample):
            return False
        return True

    @staticmethod
    def _date_apply(time):
        try:
            temp = format(pd.to_datetime(str(time).replace('\\', '/')), '%Y%m%d')
        except:
            temp = ''.join([_ for _ in time if _.isdigit()])
        if len(temp) == 0:
            raise ValueError("交易日期列有不符合格式的值t002")
        if temp[0] == '2':
            # temp = ''.join([_ for _ in time if _.isdigit()])
            result = temp[:8]
        elif temp[0] == '4':
            # temp = ''.join([_ for _ in time if _.isdigit()])
            date = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=int(temp[:5]) - 2)
            result = datetime.datetime.strftime(date, '%Y%m%d')
        else:
            raise ValueError("交易日期列有不符合格式的值t002")
        return result

    @staticmethod
    def _time_apply(time):
        if ':' in time or '：' in time:
            time = time.split('.')[0]
            temp = '000000' + ''.join([_ for _ in time if _.isdigit()])
            result = temp[-6:]
        elif '.' in time:
            try:
                temp = float(time)
            except ValueError:
                raise ValueError("交易时间列有不符合格式的值t003")
            if temp < 1:
                result = str(int(temp * 24)).rjust(2, '0') + str(int(temp * 1440) % 60).rjust(2, '0') + str(
                    int(temp * 86400) % 60).rjust(2, '0')
            else:
                result = str(int(temp)).rjust(6, '0')
        elif len(time) >= 1:
            temp = '000000' + time.split('.')[0]
            result = temp[-6:]
        else:
            result = '000000'
        return result

    # 交易时间排序方法,暂时不用20200819
    def _sort_trans_time(self):
        first_date = self.df.trans_time.to_list()[0]
        last_date = self.df.trans_time.to_list()[-1]
        if first_date < last_date:
            self.df['index'] = list(range(len(self.df)))
        else:
            self.df['index'] = list(range(len(self.df), 0, -1))
        self.sort_list.append('index')
        self.df.sort_values(by=self.sort_list, ascending=True, inplace=True)
        self.df.drop(['index'], axis=1, inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    def _one_col_match(self, col):
        dttime_pat = re.compile(DTTIME_PATTERN)
        date_pat = re.compile(DATE_PATTERN)
        x1 = self._match_time_head(col, dttime_pat, 14)
        x2 = self._match_time_head(col, date_pat, 8)
        if x1:
            self.df['trans_time'] = self.df[col].astype(str).apply(dttime_apply)
            self.sort_list = [col]
            # self.df.sort_values(by=col, ascending=True, inplace=True)
            # self.df.reset_index(drop=True, inplace=True)
        elif x2:
            self.df['trans_time'] = self.df[col].astype(str).apply(dttime_apply)
        else:
            raise ValueError("交易日期列有不符合格式的值t004")

    def _multi_col_match(self, res):
        dttime_pat = re.compile(DTTIME_PATTERN)
        date_pat = re.compile(DATE_PATTERN)
        time_pat = re.compile(TIME_PATTERN)
        time_s_pat = re.compile(TIME_S_PATTERN)
        date_col = ''
        time_col = ''
        for col in res:
            if self._match_time_head(col, dttime_pat, 14):
                self.df['trans_time'] = self.df[col].astype(str).apply(dttime_apply)
                self.sort_list = [col]
                # self.df.reset_index(drop=False, inplace=True)
                # self.df.sort_values(by=[col, 'index'], ascending=True, inplace=True)
                return
        for col in res:
            if self._match_time_head(col, date_pat, 8):
                date_col = col
                self.df[date_col] = self.df[date_col].astype(str).apply(self._date_apply)
                self.sort_list.append(date_col)
                res.remove(col)
                break
        for col in res:
            # 如果已经找到日期列，且在寻找时间列时，时间列与日期列完全一致，则不考虑该列
            if date_col != '' and list(self.df[date_col].astype(str)[:10]) == list(self.df[col].astype(str)[:10]):
                continue
            if self._match_time_head(col, time_pat, 6):
                time_col = col
                self.df[time_col] = self.df[time_col].fillna(0).astype(str).apply(self._time_apply)
                self.sort_list.append(time_col)
                break
            elif self._match_time_head(col, time_s_pat, 4):
                time_col = col
                self.df[time_col] = self.df[time_col].fillna(0).astype(str).apply(self._time_apply)
                self.sort_list.append(time_col)
                break
        if date_col != '' and time_col != '':
            self.df['trans_time'] = self.df[date_col] + self.df[time_col]
            self.df['trans_time'] = self.df['trans_time'].apply(dttime_apply)
            # self.df.reset_index(drop=False, inplace=True)
            # self.df.sort_values(by=[date_col, time_col, 'index'], ascending=True, inplace=True)
        elif date_col != '' and time_col == '':
            self.df['trans_time'] = self.df[date_col].apply(dttime_apply)
        else:
            raise ValueError("没有找到交易日期列t005")
        self.df.reset_index(drop=True, inplace=True)
        return

    def time_match(self):
        self.df.loc[:, self.time_col] = self.df.loc[:, self.time_col].replace('', None).fillna('000000')
        self.df.loc[:, self.acc_time_col] = self.df.loc[:, self.acc_time_col].replace('', None).fillna('000000')
        res = self.time_col
        acc_res = self.acc_time_col
        # res = self._notnull_cnt(self.time_col)
        length = len(res)
        acc_length = len(acc_res)
        if length == 0 and acc_length == 0:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'] = ['找不到时间列']
            return
        try:
            if length == 1:
                self._one_col_match(res[0])
            else:
                self._multi_col_match(res)
            # 排序放到余额验真里面做
            # self._sort_trans_time()
        except Exception as e1:
            try:
                if acc_length == 1:
                    self._one_col_match(acc_res[0])
                else:
                    self._multi_col_match(acc_res)
            except Exception as e2:
                self.basic_status = False
                self.resp['resCode'] = '20'
                self.resp['resMsg'] = '解析失败'
                self.resp['data']['warningMsg'].append(str(e1) + str(e2))

    def time_interval_check(self):
        self._query_date_transform()
        trans_max = max(self.df['trans_time'])
        trans_min = min(self.df['trans_time'])
        if self.query_end is not None and self.query_start is not None:
            x1 = self.query_end < trans_max - datetime.timedelta(days=1)
            x2 = self.query_start > trans_min
            if x1 or x2:
                self.basic_status = False
                self.resp['resCode'] = '22'
                self.resp['resMsg'] = '验真失败'
                if x1:
                    self.resp['data']['warningMsg'].append('查询结束时间早于最大交易时间')
                if x2:
                    self.resp['data']['warningMsg'].append('查询开始时间晚于最小交易时间')
                return
            query_interval = (self.query_end - self.query_end).days
            max_date = self.query_end
        else:
            query_interval = -1
            max_date = trans_max
        trans_interval = (trans_max - trans_min).days
        import_interval = (datetime.datetime.now() - max_date).days
        y2 = trans_interval < MIN_TRANS_INTERVAL
        y3 = import_interval > MIN_IMPORT_INTERVAL
        if query_interval != -1:
            y1 = query_interval < MIN_QUERY_INTERVAL
            if (y1 or y3) and (y2 or y3):
                self.basic_status = False
                self.resp['resCode'] = '21'
                self.resp['resMsg'] = '校验失败'
                if y1:
                    self.resp['data']['warningMsg'].append('查询间隔小于%d天' % MIN_QUERY_INTERVAL)
                if y2:
                    self.resp['data']['warningMsg'].append('交易间隔小于%d天' % MIN_TRANS_INTERVAL)
                if y3:
                    self.resp['data']['warningMsg'].append('导入间隔大于%d天' % MIN_IMPORT_INTERVAL)
        else:
            if y2 or y3:
                self.basic_status = False
                self.resp['resCode'] = '21'
                self.resp['resMsg'] = '校验失败'
                if y2:
                    self.resp['data']['warningMsg'].append('交易间隔小于%d天' % MIN_TRANS_INTERVAL)
                if y3:
                    self.resp['data']['warningMsg'].append('导入间隔大于%d天' % MIN_IMPORT_INTERVAL)
        return

    # 可能不需要
    def time_sequence_check(self):
        trans_first = list(self.df['trans_time'])[0]
        trans_last = list(self.df['trans_time'])[-1]
        if trans_first == trans_last:
            self.basic_status = False
            self.resp['resCode'] = '22'
            self.resp['resMsg'] = '验真失败'
            self.resp['data']['warningMsg'] = ['该流水存在时间顺序错乱的行,该流水为假流水']
            return
        elif trans_first > trans_last:
            self.df.sort_index(ascending=False, inplace=True)
            trans_first = trans_last
        self.df.reset_index(drop=True, inplace=True)
        last_date = trans_first
        for row in self.df.itertuples():
            if getattr(row, 'Index') == self.df.index[0]:
                continue
            this_date = getattr(row, 'trans_time')
            if this_date < last_date:
                self.basic_status = False
                self.resp['resCode'] = '22'
                self.resp['resMsg'] = '验真失败'
                self.resp['data']['warningMsg'] = ['该流水存在时间顺序错乱的行,该流水为假流水']
                return
            last_date = this_date
