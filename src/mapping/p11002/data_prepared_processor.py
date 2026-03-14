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
        try:
            #获取报表信息
            df_finance_table_info=self.fetch_financial_report_info()
            if df_finance_table_info.empty:
                logger.warning("未获取到财务报表信息")
                return
            #获取报表详细数据
            df_finance_table_data=self.fetch_financial_report_details(df_finance_table_info)
            if df_finance_table_data.empty:
                logger.warning("未获取到财务报表详细数据")
                return

            #数据转换，获取T年、T-1年、T-2年数据
            #资产负债表
            self.filter_report(df_finance_table_data,df_finance_table_info,'ASSET_DEBT')
            #现金流量表
            self.filter_report(df_finance_table_data, df_finance_table_info, 'CASH_FLOW')
            #利润表
            self.filter_report(df_finance_table_data, df_finance_table_info, 'PROFIT')
        except Exception as e:
            logger.error(f"数据处理过程中发生错误: {str(e)}")
            raise DataPreparedException(description=f"数据处理失败: {str(e)}")


    # 获取报表信息
    def fetch_financial_report_info(self):
        # 财务标签
        sql = '''
                    select * from finance_label
                '''
        df_finance_label = sql_to_df(sql=sql)
        if df_finance_label.empty:
            logger.warning("未获取到财务标签数据")
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

            status = df_finance_task.loc[0,'status'] if len(df_finance_task) > 0 else None
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
            table_no = self.cached_data.get('table_no')
            if not table_no:
                raise DataPreparedException(description="table_no为空")
            sql='''
                select a.*,b.req_no from finance_table_info a 
                    join  finance_file_detail b 
                    on a.table_no=b.table_no
                    where a.table_no = %(table_no)s and a.memo is null
            '''
            df_finance_table_info = sql_to_df(sql=sql, params={"table_no": table_no})
            if len(df_finance_table_info)==1:
                self.variables['common_table_type']=df_finance_table_info.loc[0,'table_type']
        # 数据验证
        if df_finance_table_info.empty:
            logger.warning("财务报表信息为空")
            return pd.DataFrame()
        df_finance_table_info=df_finance_table_info[df_finance_table_info['status']=='PERFECT']
        if df_finance_table_info.empty:
            logger.warning("没有状态为PERFECT的财务报表")
            return pd.DataFrame()
        # 安全地处理end_date列
        if 'end_date' in df_finance_table_info.columns:
            df_finance_table_info['end_date']=df_finance_table_info['end_date'].astype('str')
        return df_finance_table_info

    def fetch_financial_report_details(self,df_finance_table_info):
        try:
            # 定义顺序
            order = ['YEAR', 'HALF_YEAR', 'QUARTER', 'MONTH']
            df_finance_table_info['cycle_type'] = pd.Categorical(df_finance_table_info['cycle_type'], categories=order,
                                                                 ordered=True)

            # 主体上传的所有财务报表
            sql = '''
                        select * from finance_table_data where file_detail_no in %(file_detail_no)s
                    '''
            file_no_list = df_finance_table_info['req_no'].to_list()
            # 确保列表不为空
            if not file_no_list:
                return pd.DataFrame()

            # 处理单元素情况
            if len(file_no_list) == 1:
                # 修改SQL为等于条件
                sql = '''
                                select * from finance_table_data where file_detail_no = %(file_detail_no)s
                            '''
                params = {"file_detail_no": file_no_list[0]}
            else:
                params = {"file_detail_no": tuple(file_no_list)}

            df_finance_table_data = sql_to_df(sql=sql, params=params)
            return df_finance_table_data
        except Exception as e:
            logger.error(f"获取财务报表详细数据时发生错误: {str(e)}")
            raise



    # 筛选T年、T-1、T-2年财务报表
    # df_finance_table_data：主体上传的所有财务报表
    # df：财报报表信息
    # table_type：报表类型
    def filter_report(self,df_finance_table_data,df,table_type):
        try:
            self.cached_data[table_type]={}
            #财务标签-表头映射
            df_finance_label=self.cached_data['finance_label']
            df_finance_label=df_finance_label[(df_finance_label['label_type']=='表头') & (df_finance_label['table_type']==table_type)]

            df=df[df['table_type']==table_type]
            if df.shape[0]==0:
                return

            # 安全地处理year列
            if 'year' not in df.columns:
                logger.warning("数据框中缺少year列")
                return
            year_list = list(pd.Series(df['year'].unique()).sort_values(ascending=False))
            if not year_list:
                logger.warning("没有有效的年份数据")
                return
            for i in year_list:
                #筛选同一年的报表，按时间和周期类型排序
                df_year = df[df['year'] == i].sort_values(by=['end_date','cycle_type'], ascending=[False,True])
                df_year.index=range(len(df_year))
                if df_year.empty:
                    continue
                for index,row in df_year.iterrows():
                    try:
                        file_detail_no = df_year.loc[index, 'req_no']
                        if not file_detail_no:
                            continue
                        df_finance_table_data_year = df_finance_table_data[df_finance_table_data['file_detail_no'] == file_detail_no]

                        #数据标准化
                        df_t = structured(df_finance_table_data_year)

                        if df_year.loc[index, 'table_type'] == 'ASSET_DEBT':
                            df_t = self._process_asset_debt_table(df_t)
                        elif df_year.loc[index, 'table_type'] in ['PROFIT','CASH_FLOW']:
                            df_t = self._process_profit_cash_flow_table(df_t)

                        df_t = self._clean_and_standardize_data(df_t, df_year, index, table_type, df_finance_label)

                        # 处理不同年份的数据
                        self._handle_different_years_data(df_t, df_year, index, table_type, year_list, i)
                        break  # 找到符合条件的报表后跳出内层循环
                    except Exception as e:
                        logger.error(f"处理{table_type} {i}年数据时发生错误: {str(e)}")
                        continue
        except Exception as e:
            logger.error(f"筛选{table_type}报表时发生错误: {str(e)}")

    def _process_asset_debt_table(self, df_t):
        """处理资产负债表"""
        for index1, row1 in df_t.iterrows():
            if row1.iloc[0] == '资产' if len(row1) > 0 else False:
                df_t = df_t.loc[index1:, :]
                df_t = df_t.reset_index(drop=True)
                # 将资产行作为列名
                df_t.columns = df_t.iloc[0]  # 第一行作为列名
                df_t = df_t.iloc[1:].reset_index(drop=True)  # 删除原资产行
                break
        return df_t

    def _process_profit_cash_flow_table(self, df_t):
        """处理利润表和现金流量表"""
        for index1, row1 in df_t.iterrows():
            if row1.iloc[0] == '项目' if len(row1) > 0 else False:
                df_t = df_t.loc[index1:, :]
                df_t = df_t.reset_index(drop=True)
                df_t.columns = df_t.iloc[0]  # 第一行作为列名
                df_t = df_t.iloc[1:].reset_index(drop=True)
                break
        return df_t

    def _clean_and_standardize_data(self, df_t, df_year, index, table_type, df_finance_label):
        """清理和标准化数据"""
        # 填充空值并删除全空列
        df_t = df_t.fillna('')
        df_t = df_t.loc[:, ~(df_t == '').all()]

        # 安全地设置列名
        if len(df_t) > 0:
            # new_columns = []
            # for i, col in enumerate(df_t.iloc[0]):
            #     if col == '' and '识别科目' not in df_t.iloc[0].values:
            #         new_columns.append('识别科目')
            #     else:
            #         new_columns.append(col)
            # df_t.columns = new_columns

            # 删除行次列（如果存在）
            if '行次' in df_t.columns and df_year.loc[index, 'table_type'] == 'ASSET_DEBT':
                df_t = df_t.drop(columns=['行次'])

            # 删除空字符串的列名
            if '' in df_t.columns:
                df_t = df_t.drop(columns='')

            # # 删除第一行（列名行）
            # df_t = df_t.iloc[1:].reset_index(drop=True)

        # 添加：重命名第一列为"项目"（针对现金流量表和利润表）
        df_t = self._rename_first_column_to_item(df_t, table_type)

        # 列映射处理
        df_t = self._map_columns(df_t, df_year, index, table_type, df_finance_label)
        return df_t

    def _rename_first_column_to_item(self, df_t, table_type):
        """将第一列重命名为'项目'（针对现金流量表和利润表）"""
        try:
            if table_type in ['CASH_FLOW', 'PROFIT'] and not df_t.empty and len(df_t.columns) > 0:
                first_column = df_t.columns[0]

                # 如果第一列不是'项目'，则重命名
                if first_column != '项目':
                    logger.info(f"{table_type}表第一列 '{first_column}' 重命名为 '项目'")

                    # 创建新的列名列表
                    new_columns = list(df_t.columns)
                    new_columns[0] = '项目'
                    df_t.columns = new_columns

            return df_t
        except Exception as e:
            logger.error(f"重命名第一列为'项目'时发生错误: {str(e)}")
            return df_t

    def _map_columns(self, df_t, df_year, index, table_type, df_finance_label):
        """处理列名映射"""
        if df_t.empty:
            return df_t

        map_columns = []
        original_columns = list(df_t.columns)

        # 兼容多列（重复列）的数据现金表，重新组合数据
        if table_type == 'CASH_FLOW':
            if '识别科目' in list(df_t.columns):
                del df_t['识别科目']
            # 去掉列名中的标点符号空格等
            df_t.columns = [re.sub(r'[^\w\s]', '', col) for col in df_t.columns]
            unique_columns = set(df_t.columns)
            if len(df_t.columns) > len(unique_columns) and len(df_t.columns) % len(unique_columns) == 0:
                num_unique = len(unique_columns)
                num_group = len(df_t.columns) // num_unique
                df_concat = pd.DataFrame()
                for j in range(num_group):
                    df_concat = pd.concat([df_concat, df_t.iloc[:, num_unique * j:num_unique * (j + 1)]], axis=0)
                df_concat = df_concat.reset_index(drop=True)
                df_t = df_concat
                original_columns = list(df_t.columns)

        # 列名映射
        for column in original_columns:
            mapped_column = self._get_mapped_column(column, table_type, df_year, index, df_finance_label)
            map_columns.append(mapped_column)

        df_t.columns = map_columns
        return df_t

    def _get_mapped_column(self, column, table_type, df_year, index, df_finance_label):
        """获取映射后的列名"""
        if df_finance_label.empty:
            return column

        if column == '金额' and table_type == 'CASH_FLOW':
            column = '本年累计金额'

        try:
            pattern = r'(^|\|)' + re.escape(str(column)) + r'(\||$)|^' + re.escape(str(column)) + r'$'
            label_definition = df_finance_label[
                df_finance_label['mapping_content'].str.contains(pattern, na=False)
            ][['label_definition']]

            if len(label_definition) == 0:
                if column not in ['识别类目', '行次']:
                    table_name = table_type.replace('ASSET_DEBT', '资产负债表') \
                        .replace('CASH_FLOW', '现金流量表') \
                        .replace('PROFIT', '利润表')
                    logger.info(f"{table_name}需要添加新的表头标签\"{column}\"")
                return column

            # 本期数存在两条映射数据，区分年和季度
            if len(label_definition) > 1:
                cycle_type = df_year.loc[index, 'cycle_type'] if 'cycle_type' in df_year.columns else ''
                if cycle_type == 'YEAR':
                    label_definition = df_finance_label[
                        (df_finance_label['mapping_content'].str.contains(re.escape(str(column)), na=False)) &
                        (df_finance_label['label_definition'].str.contains('年', na=False))
                        ][['label_definition']]
                elif cycle_type == 'QUARTER':
                    label_definition = df_finance_label[
                        (df_finance_label['mapping_content'].str.contains(re.escape(str(column)), na=False)) &
                        (df_finance_label['label_definition'].str.contains('季', na=False))
                        ][['label_definition']]

            if not label_definition.empty:
                return label_definition.iloc[0, 0]
            else:
                return column

        except Exception as e:
            logger.error(f"列名映射失败: {column}, 错误: {str(e)}")
            return column

    def _handle_different_years_data(self, df_t, df_year, index, table_type, year_list, current_year):
        """处理不同年份的数据"""
        try:
            year_idx = year_list.index(current_year)

            # T年
            if year_idx == 0:
                self._store_year_data(df_t, df_year, index, table_type, 't')
            # T-1年
            elif year_idx == 1 and int(year_list[0]) - int(current_year) == 1:
                self._store_year_data(df_t, df_year, index, table_type, 't1')
            # T-2年
            elif (year_idx == 1 and int(year_list[0]) - int(current_year) == 2) or \
                    (year_idx == 2 and int(year_list[0]) - int(current_year) == 2):
                self._store_year_data(df_t, df_year, index, table_type, 't2')

        except (ValueError, IndexError) as e:
            logger.error(f"处理年份数据时发生错误: {str(e)}")

    def _store_year_data(self, df_t, df_year, index, table_type, year_suffix):
        """存储特定年份的数据"""
        try:
            if table_type == 'ASSET_DEBT':
                df_t = self._process_balance_sheet_split(df_t)

            # 检查必要的列是否存在
            required_columns = ['本年累计金额'] if table_type != 'ASSET_DEBT' else []
            if required_columns and not all(col in df_t.columns for col in required_columns):
                logger.info(f"{table_type} {year_suffix} 缺少必要列{required_columns}，跳过")
                return

            # 处理特殊现金流量表
            if table_type == 'CASH_FLOW':
                df_t = self._process_special_cash_flow(df_t)

            self.cached_data[table_type][f'df_{year_suffix}'] = df_t
            self.cached_data[table_type][f'end_date_{year_suffix}'] = df_year.loc[index, 'end_date']

        except Exception as e:
            logger.error(f"存储{table_type} {year_suffix}数据时发生错误: {str(e)}")

    def _process_balance_sheet_split(self, df_t):
        """处理资产负债表的资产和负债拆分"""
        try:
            if len(df_t.columns) >= 2:
                split_point = len(df_t.columns) // 2
                df_asset = df_t.iloc[:, :split_point]
                df_debt = df_t.iloc[:, split_point:]
                df_debt.columns = df_asset.columns
                return pd.concat([df_asset, df_debt], axis=0)
            return df_t
        except Exception as e:
            logger.error(f"处理资产负债表拆分时发生错误: {str(e)}")
            return df_t

    def _process_special_cash_flow(self, df_t):
        """处理特殊现金流量表"""
        try:
            if not df_t.empty:
                columns = pd.Series(df_t.columns).unique()
                if len(df_t.columns) / len(columns) == 2:
                    split_point = len(df_t.columns) // 2
                    df_part1 = df_t.iloc[:, :split_point]
                    df_part2 = df_t.iloc[:, split_point:]
                    return pd.concat([df_part1, df_part2], axis=0)
                elif len(df_t.columns) == 8:
                    # 处理有补充资料的情况
                    # 获取前4列的列名作为新的列名
                    new_columns = df_t.columns[:4].tolist()
                    logger.info(f"新的列名: {new_columns}")

                    # 创建两个DataFrame，分别包含前4列和后4列
                    df_part1 = df_t.iloc[:, :4].copy()  # 前4列
                    df_part2 = df_t.iloc[:, 4:].copy()  # 后4列

                    # 为第二部分数据设置相同的列名
                    df_part2.columns = new_columns

                    # 垂直拼接两个DataFrame
                    df_combined = pd.concat([df_part1, df_part2], axis=0, ignore_index=True)

                    logger.info(f"转换完成: {len(df_t)}行×8列 -> {len(df_combined)}行×4列")
                    return df_combined
            return df_t
        except Exception as e:
            logger.error(f"处理特殊现金流量表时发生错误: {str(e)}")
            return df_t