import re
import pandas as pd


def str_transfer(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:
            inside_code = 32
        elif 65281 <= inside_code <= 65374:
            inside_code -= 65248
        rstring += chr(inside_code)
    return rstring


class TransactionOtherInfo:
    """
    将流水文件中所有其他类型信息(包括:交易币种,交易渠道,交易类型,交易用途,交易备注)标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:20200818,去除所有列中无意义字符
    updated_time_v2:20201125,取出所有列中的引号,并删除交易币种是人民币以外的流水,并将所有全角字符转为半角字符
    """

    def __init__(self, trans_data, col_mapping):
        self.df = trans_data
        self.col_mapping = col_mapping

    def trans_info_match(self):
        self._trans_info_match('cur_col', 'currency')
        self._trans_info_match('chn_col', 'trans_channel')
        self._trans_info_match('typ_col', 'trans_type')
        self._trans_info_match('use_col', 'trans_use')
        self._remark_match()

    def _trans_info_match(self, trans_info, col_name):
        length = len(self.col_mapping[trans_info])
        if length:
            comp = re.compile(r'[\\\"\'\s^-]')
            string = ''
            for col in self.col_mapping[trans_info]:
                string += "self.df['" + col + "'].fillna('').astype(str)+"
            string = string[:-1]
            self.df[col_name] = eval(string).apply(lambda x: re.sub(comp, '', x))
        else:
            self.df[col_name] = ''
        if col_name == 'currency':
            self.df = self.df[(self.df[col_name].str.contains('¥|人|RMB|rmb|Rmb|CNY|cny')) |
                              (pd.isna(self.df[col_name])) |
                              (self.df[col_name] == '')]
            self.df.reset_index(drop=True, inplace=True)
        return

    def _remark_match(self):
        length = len(self.col_mapping['mark_col'])
        if length:
            comp = re.compile(r'[\\\"\'\s^-]')
            string = ''
            for col in self.col_mapping['mark_col']:
                if '对方信息' not in col:
                    string += "self.df['" + col + "'].fillna('').astype(str)+"
                    if len(self.df['opponent_name'].value_counts().index) == 1:
                        self.df['opponent_name'] = self.df[col].fillna('').astype(str). \
                            apply(lambda x: self._clean_no_op_name_remark(x))
                else:
                    string += "self.df['" + col + "'].fillna('').astype(str).apply(lambda x:x.split(':',1)[-1])+"
                    if len(self.df['opponent_name'].value_counts().index) == 1:
                        self.df['opponent_name'] = self.df[col].fillna('').astype(str).\
                            apply(lambda x: re.sub(comp, '', x.split(':', 1)[0]))
            string = string[:-1]
            self.df['remark'] = eval(string).apply(lambda x: str_transfer(re.sub(comp, '', x)))
        else:
            self.df['remark'] = ''
        return


    def _clean_no_op_name_remark(self, remark_str):
        if (remark_str.__contains__("跨行转出") or remark_str.__contains__("转账") or remark_str.__contains__("有限公司")) and \
                not remark_str.__contains__("微信转账"):
            return remark_str.split()[-1].replace("-","")
        elif len(remark_str) <=5:
            comp = re.compile(r'[\\\"\'\s^-]')
            return re.sub(comp, '', remark_str)
        else:
            return ""
