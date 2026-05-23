# 数据准备阶段， 避免同一数据多次IO交互
import pandas as pd
import re
from exceptions import DataPreparedException
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df
from mapping.tranformer import structured
from logger.logger_util import LoggerUtil
logger = LoggerUtil().logger(__name__)

class DataPreparedProcessor(ModuleProcessor):

    def process(self):
        #获取报表信息
        df_finance_table_info=self.fetch_financial_report_info()
        #获取报表详细数据
        df_finance_table_data=self.fetch_financial_report_details(df_finance_table_info)

        #数据转换，获取T年、T-1年、T-2年数据
        #资产负债表
        self.filter_report(df_finance_table_data,df_finance_table_info,'ASSET_DEBT')
        #现金流量表
        self.filter_report(df_finance_table_data, df_finance_table_info, 'CASH_FLOW')
        #利润表
        self.filter_report(df_finance_table_data, df_finance_table_info, 'PROFIT')


    # 获取报表信息
    def fetch_financial_report_info(self):
        # 财务标签
        sql = '''
                    select * from finance_label
                '''
        df_finance_label = sql_to_df(sql=sql)
        self.cached_data['finance_label']=df_finance_label


        if self.cached_data['product_code']=='11002':
            self.variables['name'] = self.user_name
            task_no = self.cached_data["task_no"]
            if task_no is None:
                raise DataPreparedException(description="入参数字段taskNo为空")

            sql = "select * from finance_task where task_no = %(task_no)s"

            df_finance_task = sql_to_df(sql=sql, params={"task_no": task_no})
            if df_finance_task.shape[0]==0:
                raise DataPreparedException(description="没有查得解析记录:" + task_no)

            status = df_finance_task.loc[0,'status']
            if "NONE_ANALYZED" == status:
                raise DataPreparedException(description="财务报表未解析：" + task_no + " Status:" + status)

            sql='''
                select a.*,b.req_no from finance_table_info a 
                    join  finance_file_detail b 
                    on a.table_no=b.table_no
                    where a.task_no = %(task_no)s and a.memo is null
            '''

            df_finance_table_info=sql_to_df(sql=sql, params={"task_no": task_no})
        else:
            sql='''
                select a.*,b.req_no from finance_table_info a 
                    join  finance_file_detail b 
                    on a.table_no=b.table_no
                    where a.table_no = %(table_no)s and a.memo is null
            '''
            df_finance_table_info = sql_to_df(sql=sql, params={"table_no": self.cached_data['table_no']})
            if len(df_finance_table_info)==1:
                self.variables['common_table_type']=df_finance_table_info.loc[0,'table_type']
        df_finance_table_info=df_finance_table_info[df_finance_table_info['status']=='PERFECT']
        df_finance_table_info['end_date']=df_finance_table_info['end_date'].astype('str')
        return df_finance_table_info

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



    # 筛选T年、T-1、T-2年财务报表
    # df_finance_table_data：主体上传的所有财务报表
    # df：财报报表信息
    # table_type：报表类型
    def filter_report(self,df_finance_table_data,df,table_type):
        self.cached_data[table_type]={}
        #财务标签-表头映射
        df_finance_label=self.cached_data['finance_label']
        df_finance_label=df_finance_label[(df_finance_label['label_type']=='表头') & (df_finance_label['table_type']==table_type)]

        df=df[df['table_type']==table_type]
        if df.shape[0]==0:
            return

        year_list = list(pd.Series(df['year'].unique()).sort_values(ascending=False))
        for i in year_list:
            #筛选同一年的报表，按时间和周期类型排序
            df_year = df[df['year'] == i].sort_values(by=['end_date','cycle_type'], ascending=[False,True])
            df_year.index=range(len(df_year))
            for index,row in df_year.iterrows():
                file_detail_no = df_year.loc[index, 'req_no']
                df_finance_table_data_year = df_finance_table_data[df_finance_table_data['file_detail_no'] == file_detail_no]

                #数据标准化
                df_t = structured(df_finance_table_data_year)

                if df_year.loc[index, 'table_type'] == 'ASSET_DEBT':
                    for index1, row1 in df_t.iterrows():
                        if row1[0] == '资产':
                            df_t = df_t.loc[index1:, :]
                            df_t.index = range(len(df_t))
                            break
                elif df_year.loc[index, 'table_type'] in ['PROFIT','CASH_FLOW']:
                    for index1, row1 in df_t.iterrows():
                        if row1[0] == '项目':
                            df_t = df_t.loc[index1:, :]
                            df_t.index = range(len(df_t))
                            break

                df_t = df_t.fillna('')
                df_t = df_t.loc[:, ~(df_t == '').all()]
                #第一行作为列名
                df_t.columns = ['识别科目' if i=='' and '识别科目' not in df_t.iloc[0] else i for i in df_t.iloc[0]]
                if '行次' in list(df_t.columns) and df_year.loc[index, 'table_type'] == 'ASSET_DEBT':
                    df_t = df_t.drop(columns=['行次'])
                #删除空字符串的列名
                if '' in df_t.columns:
                    df_t=df_t.drop(columns='')
                df_t = df_t[1:].reset_index(drop=True)
                map_columns = []

                #兼容多列（重复列）的数据现金表，重新组合数据
                if table_type == 'CASH_FLOW':
                    if len(df_t.columns)>len(set(df_t.columns)) and len(df_t.columns)%len(set(df_t.columns))==0:
                        num_unique=len(set(df_t.columns))
                        num_group=int(len(df_t.columns)/len(set(df_t.columns)))
                        df_concat=pd.DataFrame({})
                        for j in range(num_group):
                            df_concat=pd.concat([df_concat,df_t.iloc[:,num_unique*j:num_unique*(j+1)]],axis=0)
                        df_concat.index=range(len(df_concat))
                        df_t=df_concat

                # 列名映射
                for column in list(df_t.columns):
                    if column == '金额' and table_type == 'CASH_FLOW' and '本年累计金额' not in map_columns:
                        column = '本年累计金额'

                    pattern = r'(^|\|)'+str(column) + '(\||$)|^' + str(column)+'$'
                    label_definition = df_finance_label.loc[df_finance_label['mapping_content'].str.contains(pattern), ['label_definition']]
                    if len(label_definition) == 0:
                        if column not in ['识别类目','行次']:
                            logger.info("{}需要添加新的表头标签\"{}\"".format(table_type.replace('ASSET_DEBT','资产负债表').replace('CASH_FLOW','现金流量表').replace('PROFIT','利润表'),column))
                    # 本期数存在两条映射数据，区分年和季度
                    if len(label_definition) > 1:
                        if df_year.loc[index, 'cycle_type'] == 'YEAR':
                            label_definition = df_finance_label.loc[
                                (df_finance_label['mapping_content'].str.contains(column)) & (df_finance_label['label_definition'].str.contains('年')), ['label_definition']]
                        elif df_year.loc[index, 'cycle_type'] == 'QUARTER':
                            label_definition = df_finance_label.loc[
                                (df_finance_label['mapping_content'].str.contains(column)) & (df_finance_label[
                                    'label_definition'].str.contains('季')), ['label_definition']]
                        map_columns.append(label_definition.iloc[0, 0])
                    elif len(label_definition) > 0:
                        map_columns.append(label_definition.iloc[0, 0])
                    else:
                        map_columns.append(column)
                df_t.columns = map_columns

                # T年
                if year_list.index(i) == 0:
                    if table_type == 'ASSET_DEBT':
                        df_asset=df_t.iloc[:,:int(len(df_t.columns)/2)]
                        df_debt = df_t.iloc[:, int(len(df_t.columns)/2):]
                        df_debt.columns=df_asset.columns
                        df_t=pd.concat([df_asset,df_debt])
                        self.cached_data[table_type]['df_t'] = df_t
                        self.cached_data[table_type]['end_date_t'] = df_year.loc[index, 'end_date']
                        break
                    else:
                        if '本年累计金额' not in map_columns:
                            continue
                        else:
                            #处理特殊现金流量表
                            columns=pd.Series(df_t.columns).unique()
                            if table_type == 'CASH_FLOW' and len(df_t.columns)/len(columns)==2:
                                df_t = pd.concat([df_t.iloc[:, :int(len(df_t.columns) / 2)],df_t.iloc[:, int(len(df_t.columns) / 2):]])
                            self.cached_data[table_type]['df_t'] = df_t
                            self.cached_data[table_type]['end_date_t'] = df_year.loc[index, 'end_date']
                            break
                #T-1年
                elif year_list.index(i)==1 and int(year_list[0])-int(i)==1:
                    if table_type == 'ASSET_DEBT':
                        df_asset = df_t.iloc[:, :len(df_t.columns) // 2]
                        df_debt = df_t.iloc[:, len(df_t.columns) // 2:]
                        df_debt.columns = df_asset.columns
                        df_t = pd.concat([df_asset, df_debt])
                        self.cached_data[table_type]['df_t1'] = df_t
                        self.cached_data[table_type]['end_date_t1'] = df_year.loc[index, 'end_date']
                        break
                    else:
                        if '本年累计金额' not in map_columns:
                            continue
                        else:
                            #处理特殊现金流量表
                            columns=pd.Series(df_t.columns).unique()
                            if table_type == 'CASH_FLOW' and len(df_t.columns)/len(columns)==2:
                                df_t = pd.concat([df_t.iloc[:, :int(len(df_t.columns) / 2)],df_t.iloc[:, int(len(df_t.columns) / 2):]])
                            self.cached_data[table_type]['df_t1'] = df_t
                            self.cached_data[table_type]['end_date_t1'] = df_year.loc[index, 'end_date']
                            break
                #T-2年
                elif (year_list.index(i)==1 and int(year_list[0])-int(i)==2) or (year_list.index(i)==2 and int(year_list[0])-int(i)==2):
                    if table_type == 'ASSET_DEBT':
                        df_asset = df_t.iloc[:, :len(df_t.columns) // 2]
                        df_debt = df_t.iloc[:, len(df_t.columns) // 2:]
                        df_debt.columns = df_asset.columns
                        df_t = pd.concat([df_asset, df_debt])
                        self.cached_data[table_type]['df_t2'] = df_t
                        self.cached_data[table_type]['end_date_t2'] = df_year.loc[index, 'end_date']
                        break

                    else:
                        if '本年累计金额' not in map_columns:
                            continue
                        else:
                            #处理特殊现金流量表
                            columns=pd.Series(df_t.columns).unique()
                            if table_type == 'CASH_FLOW' and len(df_t.columns)/len(columns)==2:
                                df_t = pd.concat([df_t.iloc[:, :int(len(df_t.columns) / 2)],df_t.iloc[:, int(len(df_t.columns) / 2):]])
                            self.cached_data[table_type]['df_t2'] = df_t
                            self.cached_data[table_type]['end_date_t2'] = df_year.loc[index, 'end_date']
                            break

