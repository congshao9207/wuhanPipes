#
# 数据映射到决策引擎包, 数据从gears数据库获取，然后做清洗转换成决策引擎需要的特征变量
#
# import pandas as pd
# import numpy as np
# # 设置显示选项
# pd.set_option('display.max_rows', None)    # 显示所有行；也可以设置一个很大的数字，如 999999
# pd.set_option('display.max_columns', None) # 显示所有列；也可以设置一个很大的数字
# pd.set_option('display.width', None)       # 不限制显示宽度，自动换行
# pd.set_option('display.max_colwidth', None) # 显示单元格最大宽度，None表示无限制
# from fileparser.trans_flow.trans_config import SECURITY_FINES, SECURITY_FINES_EXCEPT, SECURITY_EXPENSE_FINES
#
# df = pd.read_excel('治安罚款.xlsx')
# df['对方户名'] = df['对方户名'].fillna('')
# df['交易类型'] = df['交易类型'].fillna('')
# df['交易用途'] = df['交易用途'].fillna('')
# df['备注'] = df['备注'].fillna('')
# df['no_channel_str'] = df['对方户名'] + ';' + df['交易类型'] + ';' + \
#                                     df['交易用途'] + ';' + df['备注']
# df['concat_str'] = df['对方户名'] + ';' + df['备注']
#
# df['op_name'] = df['对方户名']
# print(df[['no_channel_str']])
# df['unusual_trans_type1'] = pd.Series(np.where(((df['对方户名'].str.contains(SECURITY_FINES) &
#                                  (df['对方户名'].str.contains(SECURITY_FINES_EXCEPT))) |
#                                 (df['对方户名'].str.contains(SECURITY_EXPENSE_FINES))) &
#                                (df['交易金额'] < 0), '治安罚款', ''))
#
# df['unusual_trans_type2'] = pd.Series(np.where(((df['no_channel_str'].str.contains(SECURITY_FINES) &
#                                 (~df['concat_str'].str.contains(SECURITY_FINES_EXCEPT))) |
#                                 (df['op_name'].str.contains(SECURITY_EXPENSE_FINES))) &
#                                               (df['交易金额'] < 0), '治安罚款', '')) + ';'
# print(df['unusual_trans_type2'])