
from faker import Faker

from data.process_excel_case import Process
from util.mysql_reader import sql_insert


class unit_multi_deposit_v2(Process):

    def read_excel_as_df(self):
        path = self.read_path
        return super().read_excel_as_df(path)

    def write_df_into_excel(self, df=None):
        path = self.write_path
        return super().write_df_into_excel(path,df=df)

    def run_processor(self, path):
        return super().run_processor(path=path, method=self.method)

    def insert_data(self, df=None):
        if df is not None and len(df) > 0:
            # 去掉用例标题空行
            no_empty_df = df.dropna(subset=['用例标题'], how='any')
            # 获取标题的list集合
            title_list = no_empty_df.columns.values.tolist()
            # 遍历no_empty_df逐条插入数据
            key_value_main_1 = []
            key_list = []
            for index, row in no_empty_df.iterrows():
                df_main_id = 0
                df_main_1_key_array = []
                key_main = str(row['main_key'])
                key_main_1 = str(row['main_1_key'])
                channel_api_no = str(row['测试模块'])
                for title in title_list:
                    if 'table_main_1' == title:
                        # 插入第二张主表的数据，返回多条数据的主表子表关联主键
                        title_value = str(row[title])
                        if title_value is not None and title_value != 'nan' and len(title_value) > 0:
                            df_main_1_key_array, key = self._insert_main_1_table_data(title_value, key_main_1, channel_api_no)
                            key_list.append(key)
                    if title.find('table_main_1_sub') >= 0:
                        # 插入第二张主表关联的子表数据
                        title_value = str(row[title])
                        if title_value is not None and title_value != 'nan' and len(title_value) > 0:
                            self._insert_main_1_table_sub_data(title_value, df_main_1_key_array)
            no_empty_df['key_value_main'] = key_list
            return no_empty_df

    # 处理第二张主表数据
    def _insert_main_1_table_data(self, title, key, channel_api_no):
        title = title.replace('\n', '').replace('\r', '')
        key = key.replace('\n', '').replace('\r', '')
        title_array = title.split(';')
        key_array = key.split(';')
        # 查询条件-多组
        query_key_value_array = []
        fake = Faker(locale='zh_CN')
        # 表名
        table_name = title_array[0].split('.')[0]
        # 关联子表的主键
        key_word_sub_table = title_array[0].split('.')[1]
        # 封装插入主表的字段
        insert_key_array = []
        # 封装插入主表的数据-多组
        insert_value_array = []
        # 插入成功后查询出主键
        key_word_sub_table_array = []
        # 描述有多少组数据
        data_group_count = 0
        # 每组字段种类
        field_count = 0

        name_key_word = ''
        name_key_value = ''
        id_no_key_word = ''
        id_no_key_value = ''
        insert_key_array = []
        insert_value_array = []
        for key_value in key_array:
            if key_value in ['unique_name', 'user_name', 'name', 'ent_name'] and key_value not in title:
                name_key_word = key_value
                name_key_value = '\'' + str(fake.name()) + '\''
            if key_value in ['unique_id_no', 'id_card_no', 'credit_code'] and key_value not in title:
                id_no_key_word = key_value
                id_no_key_value = '\'' + str(fake.ssn()) + '\''
            if ('phone' == key_value) and key_value not in title:
                phone_key_word = key_value
                phone_key_value = '\'' + str(fake.phone_number()) + '\''

        if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
            insert_key_array.append(key_word_sub_table)
            # 主表要插入字段
        if len(title_array) > 0:
            for info in title_array:
                key_value_array = []
                if info.find('[0]') >= 0:
                    field_count = field_count + 1
                    insert_key_array.append(info.split('[0]')[0].split('.')[1])
            if field_count > 0:
                data_group_count = (len(title_array) - 1) / field_count
                data_group_count = int(data_group_count)
            if data_group_count > 0:
                for i in range(data_group_count):
                    data = []
                    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
                        key_word_sub_sub_table_value = str(fake.random_int()) + str(fake.random_int())
                        data.append(key_word_sub_sub_table_value)
                    for detail in title_array:
                        if detail.find('[' + str(i) + ']') >= 0:
                            info_value = str(detail.split('=')[1])
                            if len(info_value) > 0:
                                data.append(info_value)
                            else:
                                data.append('Null')
                    data.append('\'' + channel_api_no.split('.')[0] + '\'')
                    data.append(name_key_value)
                    data.append(id_no_key_value)
                    insert_value_array.append(data)
        insert_key_array.append('channel_api_no')
        insert_key_array.append(name_key_word)
        insert_key_array.append(id_no_key_word)
        for data in insert_value_array:
            # 拼接sql
            main_table_sql = """
                insert into 
                """
            main_table_sql += ' ' + table_name + ' ('
            main_table_sql += ','.join(insert_key_array)
            main_table_sql += ') values '
            # for data in insert_value_array:
            main_table_sql += '(' + ','.join(data) + ')'
            print('insert-sql--' + main_table_sql)
            r = sql_insert(sql=main_table_sql)
            print("r======================", r)
            self.record_tab(table_name, r.lastrowid)
            key_word_sub_table_array.append(r.lastrowid)

        key = '{'
        if len(name_key_word) > 0:
            key += '\"' + name_key_word + '\"' + ':'
            key += name_key_value + ','
        if len(id_no_key_word) > 0:
            key += '\"' + id_no_key_word + '\"' + ':'
            key += id_no_key_value
        key += '}'

        return key_word_sub_table_array, key

    # 处理第二张主表数据对应子表数据
    def _insert_main_1_table_sub_data(self, title, df_main_id_array):
        title = title.replace('\n', '').replace('\r', '')
        value_array = title.split(';')
        table_name = ''
        relation_mian_table_key = ''
        # 字段种类
        field_count = 0
        insert_key_array = []
        if len(value_array) > 0:
            table_name = value_array[0].split('.')[0]
            relation_mian_table_key = value_array[0].split('.')[1]
            for i in range(5):
                count = 0
                for detail in value_array:
                    if detail.find('[' + str(i) + '-0]') >= 0:
                        field_count = field_count + 1
                        insert_key_array.append(detail.split('[' + str(i) + '-0]')[0].split('.')[1])
                        count = 1
                if count > 0:
                    break
            insert_key_array.append(relation_mian_table_key)
            for i in range(len(df_main_id_array)):
                insert_value_array = []
                for j in range(5):
                    data = []
                    number = 0
                    for detail in value_array:
                        if detail.find('[' + str(i) + '-' + str(j) + ']') >= 0:
                            detail_value = str(detail.split('=')[1]).strip()
                            if len(detail_value) > 0:
                                data.append(detail_value)
                            else:
                                data.append('Null')
                            number = 1
                    if number > 0:
                        data.append(str(df_main_id_array[i]))
                        insert_value_array.append(data)
                if len(insert_value_array) > 0:
                    sql = """
                             insert into 
                             """
                    sql += ' ' + table_name + ' ('
                    sql += ','.join(insert_key_array)
                    sql += ') values '
                    for data in insert_value_array:
                        sql += '(' + ','.join(data) + '),'
                    sql = sql[0:len(sql) - 1]
                    print(sql)
                    r = sql_insert(sql=sql)
                    self.record_tab(table_name, r.lastrowid)

    def do_process_case(self, read_path=None, write_path=None):
        df = self.read_excel_as_df()
        df_write_excel = self.insert_data(df=df)
        path = self.write_df_into_excel(df=df_write_excel)
        return self.run_processor(path=path)
