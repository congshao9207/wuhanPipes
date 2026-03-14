import re

import pandas as pd

from fileparser.trans_flow.trans_config import MAX_TITLE_NUMBER, CSV_DELIMITER, TRANS_TIME_PATTERN, \
    TRANS_AMT_PATTERN, TRANS_BAL_PATTERN, CSV_ENCODING, ACCOUNTING_DATE_PATTERN
from fileparser.trans_flow.trans_z04_time_standardization import dttime_apply
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


class TransCsv:
    """
    若上传流水文件类型是csv文件，将调用此类进行数据读取
    author:汪腾飞
    create_time:20201222
    """

    def __init__(self, file):
        self.file = file
        # self.param = param
        self.title = None
        self.title_params = {}
        self.trans_data = None
        self.basic_status = True
        self.delimiter = b','
        self.csv_encoding = 'utf-8'
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def process(self):
        self.title = self._find_title()
        if self.title == -1 or self.title is None:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            if self.title == -1:
                self.resp['data']['warningMsg'] = ['上传失败,无法找到标题行,流水文件内容有误']
            else:
                self.resp['data']['warningMsg'] = ['文件读取失败,请检查文件是否损坏']
            return
        self.trans_data = self._convert_to_dataframe()

    def _find_title(self):
        try:
            with open(self.file, 'rb') as f:
                # 读取前n行数据，查找标题行所在
                lines = f.readlines()[:MAX_TITLE_NUMBER]
        except Exception as e:
            logger.info("_find_title exception" + str(e))
            return
        max_len = 0  # 最大列
        title = -1  # 标题行号
        cnt = 0  # 非空行计数
        for line in lines:
            line = re.sub(rb'\s', b'', line)
            if len(line) == 0:
                continue
            if cnt == 0:  # 第一个非空行里面寻找本csv文件的分隔符
                for enco in CSV_ENCODING:
                    try:
                        line.decode(encoding=enco)
                        self.csv_encoding = enco
                        break
                    except:
                        continue
                deli_max_len = 0
                for deli in CSV_DELIMITER:
                    deli_len = line.count(deli.encode(encoding=self.csv_encoding))
                    if deli_len > deli_max_len:
                        deli_max_len = deli_len
                        self.delimiter = deli.encode(encoding=self.csv_encoding)

            temp_len = line.count(self.delimiter)
            if (re.search(TRANS_TIME_PATTERN.encode(encoding=self.csv_encoding), line) or
                re.search(ACCOUNTING_DATE_PATTERN.encode(encoding=self.csv_encoding), line)) and \
                    re.search(TRANS_AMT_PATTERN.encode(encoding=self.csv_encoding), line) and \
                    re.search(TRANS_BAL_PATTERN.encode(encoding=self.csv_encoding), line):
                if temp_len > max_len:
                    title = cnt
                    max_len = temp_len
            cnt += 1
        return title

    def _convert_to_dataframe(self):
        deli = self.delimiter.decode(encoding=self.csv_encoding)
        df = None
        try:
            df = pd.read_csv(self.file, delimiter=deli, header=self.title, index_col=False, encoding=self.csv_encoding)
            self.basic_status = True
        except Exception as e:
            logger.info("----读取失败原因r3:%s----" % str(e))
            self.basic_status = False
            self.resp['resCode'] = '1'
            self.resp['resMsg'] = '失败'
            self.resp['data']['warningMsg'] = ['文件读取异常']
        return df

    def _single_cell_match(self, cell_value, cell_type):
        """
        单个单元格匹配,可能的格式:
            1.单个关键字,如:"账号","账   号","账   号:"等
            2.单个关键字+值,如:"账号:123456","账号123456"
            3.多个关键字+值,如:"户名:张三  账号:123456"
            4.空值
        :param cell_value: 单个单元格里面的字符串
        :param cell_type: 枚举值["raw", "account_no", "opponent_name", "start_date", "end_date"]
                "raw": 所有关键字均需要查找并匹配相应关键字格式
                "account_no": 仅匹配账户格式
                "opponent_name": 仅匹配户名格式
                "start_date": 仅匹配日期格式
                "end_date": 仅匹配日期格式
        :return:
        """
        cell_value = str(cell_value).strip()
        cell_list = cell_value.split()
        if len(cell_list) == 0:
            return
        if cell_type == "raw":
            for val in cell_list:
                sub_temp = re.sub(r'[^*?:：0-9\u4e00-\u9fa5]', '', val)
                # todo 除银行外还有其他信息 如xx银行交易明细之类
                # if '银行' in sub_temp and not self.title_params.__contains__('bank'):
                #     self.title_params['bank'] = sub_temp
                # todo 关键字卡号/卡号被空格隔开
                if '账号' in sub_temp or '卡号' in sub_temp:
                    acc_temp = re.sub(r'[*?]', '', sub_temp)
                    acc_temp = re.search(r'[3-9]\d{12,18}', acc_temp)
                    if acc_temp is not None:
                        if self.title_params.__contains__('account_no'):
                            self.title_params['account_no'].append(acc_temp.group(0))
                        else:
                            self.title_params['account_no'] = [acc_temp.group(0)]
                # todo 关键字户名/户名和姓名被空格隔开
                if '户名' in sub_temp or '姓名' in sub_temp:
                    name_temp = re.search(r'(?<=[\u4e00-\u9fa5][:：])[*?\u4e00-\u9fa5]{2,4}', sub_temp)
                    if name_temp is not None:
                        if self.title_params.__contains__('opponent_name'):
                            self.title_params['opponent_name'].append(name_temp.group(0))
                        else:
                            self.title_params['opponent_name'] = [name_temp.group(0)]
                # todo 关键字 本次查询时间段
                sub_temp = re.sub(r'[*?]', '', sub_temp)
                if re.search(r'[起|开]始', sub_temp):
                    start_temp = re.search(r'^20([01]\d|20)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])|^4\d{4}',
                                           sub_temp)
                    if start_temp is not None:
                        self.title_params['start_date'] = format(dttime_apply(start_temp.group(0)), '%Y-%m-%d')
                if re.search(r'(截止|结束|终止)', sub_temp):
                    end_temp = re.search(r'^20([01]\d|20)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])|^4\d{4}',
                                         sub_temp)
                    if end_temp is not None:
                        self.title_params['end_date'] = format(dttime_apply(end_temp.group(0)), '%Y-%m-%d')
        else:
            cell_value = ''.join(cell_list)
            cell_value = re.sub(r'[^*?\d\u4e00-\u9fa5]', '', cell_value)
            if cell_type == 'account_no':
                if re.match(r'[3-9]\d{12,18}', cell_value):
                    if self.title_params.__contains__('account_no'):
                        self.title_params['account_no'].append(cell_value)
                    else:
                        self.title_params['account_no'] = [cell_value]
            elif cell_type == 'opponent_name':
                if re.match(r'[*?\u4e00-\u9fa5]{2,}', cell_value):
                    if self.title_params.__contains__('opponent_name'):
                        self.title_params['opponent_name'].append(cell_value)
                    else:
                        self.title_params['opponent_name'] = [cell_value]
            elif cell_type == 'start_date':
                cell_value = re.sub(r'[*?]', '', cell_value)
                if re.match(r'^20([01]\d|20)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])|^4\d{4}', cell_value):
                    self.title_params['start_date'] = format(dttime_apply(cell_value), '%Y-%m-%d')
            elif cell_type == 'end_date':
                cell_value = re.sub(r'[*?]', '', cell_value)
                if re.match(r'^20([01]\d|20)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])|^4\d{4}', cell_value):
                    self.title_params['end_date'] = format(dttime_apply(cell_value), '%Y-%m-%d')
        return
