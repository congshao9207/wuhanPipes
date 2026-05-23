import pandas as pd
import re
from mapping.tranformer import Transformer,structured

from util.mysql_reader import sql_to_df


class T11003(Transformer):
    '''
    财报预解析
    '''

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'exception_table_no': ''
        }

    def transform(self):
        #获取报表信息
        df_finance_table_info=self.fetch_financial_report_info()
        #获取报表详细数据
        df_finance_table_data=self.fetch_financial_report_details(df_finance_table_info)

        #数据转换，判断报表是否异常
        self.filter_report(df_finance_table_data,df_finance_table_info)


    def fetch_financial_report_details(self,df_finance_table_info):
        # 定义顺序
        order = ['YEAR', 'HALF_YEAR', 'QUARTER', 'MONTH']
        df_finance_table_info['cycle_type'] = pd.Categorical(df_finance_table_info['cycle_type'], categories=order,
                                                             ordered=True)

        # 主体上传的所有财务报表
        sql = '''
                            select * from finance_table_data where file_detail_no in %(file_detail_no)s
                        '''
        file_no_list = df_finance_table_info['req_no'].to_list()
        df_finance_table_data = sql_to_df(sql=sql, params={"file_detail_no": tuple(file_no_list)})
        return df_finance_table_data

    # 获取报表信息
    def fetch_financial_report_info(self):
        sql='''
            select a.*,b.req_no from finance_table_info a 
                join  finance_file_detail b 
                on a.table_no=b.table_no
                where a.table_no = %(table_no)s
        '''
        df_finance_table_info = sql_to_df(sql=sql, params={"table_no": self.cached_data['table_no']})
        if len(df_finance_table_info)==1:
            self.variables['common_table_type']=df_finance_table_info.loc[0,'table_type']
        df_finance_table_info=df_finance_table_info[df_finance_table_info['status']=='PERFECT']
        df_finance_table_info['end_date']=df_finance_table_info['end_date'].astype('str')
        return df_finance_table_info

    def filter_report(self,df_finance_table_data,df):
        #数据标准化
        df_t = structured(df_finance_table_data)

        if df.loc[0,'table_type'] == 'ASSET_DEBT':
            for index,row in df_t.iterrows():
                if row[0]=='资产':
                    df_t=df_t.loc[index:,:]
                    df_t.index=range(len(df_t))
                    break
        elif df.loc[0, 'table_type'] in ['PROFIT', 'CASH_FLOW']:
            for index1, row1 in df_t.iterrows():
                if row1[0] == '项目':
                    df_t = df_t.loc[index1:, :]
                    df_t.index = range(len(df_t))
                    break
        df_t = df_t.fillna('')
        df_t = df_t.loc[:, ~(df_t == '').all()]
        #第一行作为列名
        df_t.columns = ['识别科目' if i=='' and '识别科目' not in df_t.iloc[0] else i for i in df_t.iloc[0]]
        df_t = df_t[1:].reset_index(drop=True)
        if '行次' in list(df_t.columns) and df.loc[0,'table_type'] == 'ASSET_DEBT':
            df_t=df_t.drop(columns=['行次'])

        #兼容解析结果异常情况：表格列数小于3列；资产负债表列数不是2的倍数
        if len(df_t.columns)<3 or (df.loc[0,'table_type'] == 'ASSET_DEBT' and (len(df_t.columns)%2!=0 or len(df_t.columns)<6 or ('资产' not in str(df_t.columns) and '负债' not in str(df_t.columns)))):
            self.variables['exception_table_no']=self.cached_data['table_no']
