from fileparser.trans_flow.trans_config import MAX_TITLE_NUMBER, TRANS_TIME_PATTERN, \
    TRANS_AMT_PATTERN, TRANS_BAL_PATTERN, ACCOUNTING_DATE_PATTERN
import pandas as pd
import re

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


class TransXls:
    """
    若上传流水文件类型是xls*文件，将调用此类进行数据读取
    author:汪腾飞
    create_time:20201222
    """
    def __init__(self, file):
        self.file = file
        # self.param = param
        self.sheet_name = None
        self.title = None
        self.title_params = {}
        self.trans_data = None
        self.basic_status = True
        self.read_type = 'xls'
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def process(self):
        self.sheet_name, self.title = self._find_title()
        if self.sheet_name is None or self.sheet_name == '' or self.title is None:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            if self.sheet_name is None:
                self.resp['data']['warningMsg'] = ['文件读取异常']
            else:
                self.resp['data']['warningMsg'] = ['上传失败,无法找到标题行,流水文件内容有误']
            return
        self.trans_data = self._convert_to_dataframe()

    def _find_title(self):
        """
        从文件中的所有sheet中寻找存在流水标题行的文件，如果存在则跳出循环
        :return:
        """
        try:
            title_df = pd.read_excel(self.file, nrows=MAX_TITLE_NUMBER, header=None, sheet_name=None)
            index_list = title_df.keys()
        except Exception as e:
            logger.info("----读取失败原因r1:%s----" % str(e))
            try:
                title_df = pd.read_html(self.file, skiprows=range(MAX_TITLE_NUMBER, 100000), header=None)
                self.read_type = 'html'
                index_list = range(len(title_df))
            except Exception as e:
                logger.info("----读取失败原因r2:%s----" % str(e))
                return None, None
        # 遍历所有sheet
        for k in index_list:
            v = title_df[k]
            if v.shape[0] == 0:
                continue
            max_len = 0  # 最大列
            title = -1  # 标题行号
            cnt = 0  # 遍历行数计数
            for row in v.itertuples():
                temp = [re.sub(r'\s', '', str(x)) for x in row if re.sub(r'\s', '', str(x)) != '']
                temp_len = len(temp)
                string = ''.join(temp)
                if (re.search(TRANS_TIME_PATTERN, string) or re.search(ACCOUNTING_DATE_PATTERN, string)) and \
                        re.search(TRANS_AMT_PATTERN, string) and \
                        re.search(TRANS_BAL_PATTERN, string):
                    if temp_len > max_len:
                        title = cnt
                        max_len = temp_len
                cnt += 1
            if title != -1:
                return k, title
        return '', None

    def _convert_to_dataframe(self):
        df = None
        try:
            if self.read_type == 'xls':
                df = pd.read_excel(self.file, header=self.title, sheet_name=self.sheet_name)
            else:
                df = pd.read_html(self.file, header=self.title)[self.sheet_name]
            self.basic_status = True
        except Exception as e:
            logger.info("----读取失败原因r3:%s----" % str(e))
            self.basic_status = False
            self.resp['resCode'] = '1'
            self.resp['resMsg'] = '失败'
            self.resp['data']['warningMsg'] = ['文件读取异常']
        return df
