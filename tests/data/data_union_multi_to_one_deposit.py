from faker import Faker

from data.process_excel_case import Process
from util.mysql_reader import sql_insert
from util.mysql_reader import sql_to_df



# 处理第二张主表数据
def _insert_main_1_table_data(title, key, channel_api_no, expired_at='2030-12-20'):
    title.replace('\n', '').replace('\r', '')
    title_array = title.split(';')
    key_array = key.split(';')
    # 查询条件
    query_key_value_array = []
    fake = Faker(locale='zh_CN')
    # 表名
    table_name = title_array[0].split('.')[0]
    # 关联子表的主键
    key_word_sub_table = title_array[0].split('.')[1]
    insert_key_array = []
    insert_value_array = []

    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        insert_key_array.append(key_word_sub_table)
        insert_value_array.append(str(fake.random_int()) + str(fake.random_int()))
        # 主表要插入字段
    if (len(title_array) > 1):
        count = 0
        for info in title_array:
            count = count + 1
            if count > 1:
                key_value_array = []
                insert_key_array.append(info.split('=')[0].split('.')[1])
                insert_value_array.append(info.split('=')[1])
                for key in key_array:
                    if key == info.split('=')[0].split('.')[1]:
                        key_value_array.append(info.split('=')[0].split('.')[1])
                        # key_value_array.append(info.split('=')[1])
                        value = str(info.split('=')[1])
                        if len(value) > 0:
                            key_value_array.append(value)
                        else:
                            key_value_array.append('Null')
                        query_key_value_array.append(key_value_array)

    insert_key_array.append('expired_at')
    insert_key_array.append('channel_api_no')
    insert_value_array.append('\'' + expired_at + '\'')
    insert_value_array.append('\'' + channel_api_no.split('.')[0] + '\'')
    # 拼接sql
    main_table_sql = """
        insert into 
        """
    main_table_sql += ' ' + table_name + ' ('
    main_table_sql += ','.join(insert_key_array)
    main_table_sql += ') values ('
    main_table_sql += ','.join(insert_value_array) + ')'
    print('insert-sql--' + main_table_sql)
    sql_insert(sql=main_table_sql)

    # 插入成功后查询出主键
    info_main_table_sql = """
        select 
        """
    if len(key_word_sub_table) > 0:
        info_main_table_sql += key_word_sub_table
    info_main_table_sql += ' from ' + table_name + ' where '
    for key_value_array in query_key_value_array:
        info_main_table_sql += str(key_value_array[0]) + '=' + str(key_value_array[1]) + ' and '
    info_main_table_sql = info_main_table_sql[0:len(info_main_table_sql) - 4]
    info_main_table_sql += ' order by id desc limit 1'
    print('query-sql' + info_main_table_sql)
    df = sql_to_df(sql=info_main_table_sql)
    df['key_sub'] = df[key_word_sub_table]
    return df


# 处理第一张主表数据
def _insert_main_table_data(title, key, channel_api_no, expired_at='2030-12-20'):
    title = title.replace('\n', '').replace('\r', '')
    key = key.replace('\n', '').replace('\r', '')
    title_array = title.split(';')
    key_array = key.split(';')
    # 表名
    table_name = title_array[0].split('.')[0]
    # 关联子表的主键
    key_word_sub_table = title_array[0].split('.')[1]
    fake = Faker(locale='zh_CN')
    # 用faker生成查询条件对应的字段
    name_key_word = ''
    name_key_value = ''
    id_no_key_word = ''
    id_no_key_value = ''
    phone_key_word = ''
    phone_key_value = ''
    insert_key_array = []
    insert_value_array = []
    query_key_array = []
    for key_value in key_array:
        if key_value in ['unique_name', 'user_name', 'name','ent_name']:
            name_key_word = key_value
            name_key_value = fake.name()
        if key_value in ['unique_id_no', 'id_card_no','credit_code']:
            id_no_key_word = key_value
            id_no_key_value = fake.ssn()
        if ('phone' == key_value):
            phone_key_word = key_value
            phone_key_value = fake.phone_number()
    # 关联主键不是id，faker一个数字
    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        insert_key_array.append(key_word_sub_table)
        insert_value_array.append(str(fake.random_int()) + str(fake.random_int()))
    query_key_array.append(key_word_sub_table)
    if len(name_key_word) > 0:
        insert_key_array.append(name_key_word)
        insert_value_array.append('\'' + str(name_key_value) + '\'')
        query_key_array.append(name_key_word)
    if len(id_no_key_word) > 0:
        insert_key_array.append(id_no_key_word)
        insert_value_array.append('\'' + str(id_no_key_value) + '\'')
        query_key_array.append(id_no_key_word)
    if len(phone_key_word) > 0:
        insert_key_array.append(phone_key_word)
        insert_value_array.append('\'' + str(phone_key_value) + '\'')
        query_key_array.append(phone_key_word)
    # 主表要插入字段
    if (len(title_array) > 1):
        count = 0
        for info in title_array:
            count = count + 1
            if count > 1:
                insert_key_array.append(info.split('=')[0].split('.')[1])
                value = str(info.split('=')[1]).strip()
                if len(value) > 0:
                    insert_value_array.append(value)
                else:
                    insert_value_array.append('Null')
    insert_key_array.append('expired_at')
    insert_key_array.append('channel_api_no')
    insert_value_array.append('\'' + expired_at + '\'')
    insert_value_array.append('\'' + channel_api_no.split('.')[0] + '\'')
    # 拼接sql
    main_table_sql = """
                    insert into 
                    """
    main_table_sql += ' ' + table_name + ' ('
    main_table_sql += ','.join(insert_key_array)
    main_table_sql += ') values ('
    main_table_sql += ','.join(insert_value_array)
    main_table_sql += ')'
    print('insert-sql--' + main_table_sql)
    sql_insert(sql=main_table_sql)

    # 插入成功后查询出主键
    info_main_table_sql = """
                select 
                """
    info_main_table_sql += ','.join(query_key_array)
    info_main_table_sql += ' from ' + table_name + ' where '
    if len(name_key_word) > 0:
        info_main_table_sql += name_key_word + '=' + '\'' + name_key_value + '\'' + ' and '
    if len(id_no_key_word) > 0:
        info_main_table_sql += id_no_key_word + '=' + '\'' + id_no_key_value + '\'' + ' and '
    if len(phone_key_word) > 0:
        info_main_table_sql += phone_key_word + '=' + '\'' + phone_key_value + '\'' + ' and '
    info_main_table_sql = info_main_table_sql[0:len(info_main_table_sql) - 4]
    info_main_table_sql += ' order by id desc limit 1'
    df = sql_to_df(sql=info_main_table_sql)
    key = '{'
    if len(name_key_word) > 0:
        key += '\"' + name_key_word + '\"' + ':'
        key += '\"' + df[name_key_word][0] + '\"' + ','
    if len(id_no_key_word) > 0:
        key += '\"' + id_no_key_word + '\"' + ':'
        key += '\"' + df[id_no_key_word][0] + '\"' + ','
    if len(phone_key_word) > 0:
        key += '\"' + phone_key_word + '\"' + ':'
        key += '\"' + df[phone_key_word][0] + '\"' + ','
    key = key[0:len(key) - 1]
    key += '}'
    df['key'] = key
    df['key_sub'] = df[key_word_sub_table]
    return df


# 处理第一张主表数据对应的子表数据
def _insert_main_table_sub_data(title, df_main_id):
    title = title.replace('\n', '').replace('\r', '')
    value_array = title.split(';')
    table_name = ''
    relation_mian_table_key = ''
    # 字段种类
    field_count = 0
    field_array = []
    # 描述有多少组数据
    data_group_count = 0
    sql = """
         insert into 
         """
    key_value_array = []
    if len(value_array) > 0:
        table_name = value_array[0].split('.')[0]
        relation_mian_table_key = value_array[0].split('.')[1]
        for detail in value_array:
            if detail.find('[0]') >= 0:
                field_count = field_count + 1
                field_array.append(detail.split('[0]')[0].split('.')[1])
        if field_count > 0:
            data_group_count = (len(value_array) - 1) / field_count
            data_group_count = int(data_group_count)

        if data_group_count > 0:
            for i in range(data_group_count):
                data = []
                for detail in value_array:
                    if detail.find('[' + str(i) + ']') >= 0:
                        value = str(detail.split('=')[1])
                        if len(value) > 0:
                            data.append(value)
                        else:
                            data.append('Null')
                key_value_array.append(data)
    sql += ' ' + table_name + ' ('
    if len(field_array) > 0:
        sql += ','.join(field_array) + ','
    sql += relation_mian_table_key
    sql += ') values '
    if len(key_value_array) > 0:
        for data in key_value_array:
            sql += '(' + ','.join(data) + ',' + str(df_main_id) + '),'
    else:
        sql += '(' + str(df_main_id) + '),'
    sql = sql[0:len(sql) - 1]
    print(sql)
    sql_insert(sql=sql)


class unit_deposit(Process):

    def read_excel_as_df(self):
        path = self.read_path
        return super().read_excel_as_df(path)

    def write_df_into_excel(self, df=None):
        path = self.write_path
        return super().write_df_into_excel(path, df=df)

    def run_processor(self, path):
        return super().run_processor(path=path,method=self.method)

    def insert_data(self, df=None):
        if df is not None and len(df) > 0:
            # 去掉用例标题空行
            no_empty_df = df.dropna(subset=['用例标题'], how='any')
            # 获取标题的list集合
            title_list = no_empty_df.columns.values.tolist()
            # 遍历no_empty_df逐条插入数据
            key_value_main_1 = []
            key_value_main_2 = []
            for index, row in no_empty_df.iterrows():
                df_main_id = 0
                df_mian_1_id = 0
                for title in title_list:
                    if 'table_main' == title:
                        # 插入第一张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        key = str(row['main_key'])
                        channel_api_no = str(row['测试模块'])
                        df_main = _insert_main_table_data(title, key, channel_api_no)
                        df_main_id = df_main['key_sub'][0]
                        key_value_main_1.append(df_main['key'][0])
                    if title.find('table_main_sub') >= 0:
                        # 插入第一张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_table_sub_data(title, df_main_id)
                    if 'table_main_1' == title:
                        # 插入第二张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            key = str(row['main_1_key'])
                            channel_api_no = str(row['测试模块'])
                            if (len(channel_api_no)) < 5:
                                channel_api_no = '0' + channel_api_no
                            df_main = _insert_main_1_table_data(title, key, channel_api_no)
                            df_mian_1_id = df_main['key_sub'][0]
                    if title.find('table_main_1_sub') >= 0:
                        # 插入第二张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_table_sub_data(title, df_mian_1_id)
            no_empty_df['key_value_main'] = key_value_main_1
            return no_empty_df

    def do_process_case(self, read_path=None, write_path=None):
        df = self.read_excel_as_df()
        df_write_excel = self.insert_data(df=df)
        path = self.write_df_into_excel(df=df_write_excel)
        return self.run_processor(path=path)
