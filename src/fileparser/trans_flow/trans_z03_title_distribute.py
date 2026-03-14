
from fileparser.trans_flow.trans_config import TRANS_TIME_PATTERN, TRANS_AMT_PATTERN, TRANS_BAL_PATTERN, \
    TRANS_OPNAME_PATTERN, TRANS_OPACCT_PATTERN, TRANS_OPBANK_PATTERN, TRANS_CUR_PATTERN, TRANS_CHANNEL_PATTERN, \
    TRANS_TYP_PATTERN, TRANS_USE_PATTERN, TRANS_REMARK_PATTERN, ACCOUNTING_DATE_PATTERN
import re


class TransBasic:
    """
    流水标题行处理,将标题行按照正则匹配方式分配到各个对应的列中
    author:汪腾飞
    created_time:20200630
    updated_time_v1:现在所有的字符串参数都写到一个配置文件中
    """

    def __init__(self, trans_data):
        self.df = trans_data
        self.col_mapping = {
            'time_col': [],
            'amt_col': [],
            'bal_col': [],
            'cur_col': [],
            'opname_col': [],
            'opacc_col': [],
            'opbank_col': [],
            'chn_col': [],
            'typ_col': [],
            'use_col': [],
            'mark_col': [],
            'acc_time_col': []
        }

    def match_title(self):
        self._match_title_col()

    def _match_title_col(self):
        """
        将标题行粗略分配到对应的数据库标题列中
        :return:
        """
        time_pat = re.compile(TRANS_TIME_PATTERN)
        amt_pat = re.compile(TRANS_AMT_PATTERN)
        bal_pat = re.compile(TRANS_BAL_PATTERN)
        cur_pat = re.compile(TRANS_CUR_PATTERN)
        opname_pat = re.compile(TRANS_OPNAME_PATTERN)
        opacc_pat = re.compile(TRANS_OPACCT_PATTERN)
        opbank_pat = re.compile(TRANS_OPBANK_PATTERN)
        chn_pat = re.compile(TRANS_CHANNEL_PATTERN)
        typ_pat = re.compile(TRANS_TYP_PATTERN)
        use_pat = re.compile(TRANS_USE_PATTERN)
        mark_pat = re.compile(TRANS_REMARK_PATTERN)
        acc_pat = re.compile(ACCOUNTING_DATE_PATTERN)
        for col in self.df.columns:
            temp_col = re.sub(r'\s', '', str(col))
            if re.search(time_pat, temp_col):
                self.col_mapping['time_col'].append(col)
            elif re.search(amt_pat, temp_col):
                self.col_mapping['amt_col'].append(col)
            elif re.search(bal_pat, temp_col):
                self.col_mapping['bal_col'].append(col)
            elif re.search(cur_pat, temp_col):
                self.col_mapping['cur_col'].append(col)
            elif re.search(opname_pat, temp_col):
                self.col_mapping['opname_col'].append(col)
            elif re.search(opacc_pat, temp_col):
                self.col_mapping['opacc_col'].append(col)
            elif re.search(opbank_pat, temp_col):
                self.col_mapping['opbank_col'].append(col)
            elif re.search(chn_pat, temp_col):
                self.col_mapping['chn_col'].append(col)
            elif re.search(typ_pat, temp_col):
                self.col_mapping['typ_col'].append(col)
            elif re.search(use_pat, temp_col):
                self.col_mapping['use_col'].append(col)
            elif re.search(mark_pat, temp_col):
                self.col_mapping['mark_col'].append(col)
            elif re.search(acc_pat, temp_col):
                self.col_mapping['acc_time_col'].append(temp_col)
        return
