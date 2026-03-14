from faker import Faker

from data.process_excel_case import Process
from util.mysql_reader import sql_insert
from util.mysql_reader import sql_to_df


# 处理主表数据
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
        if key_value in ['unique_name', 'user_name', 'name','ent_name'] and key_value not in title:
            name_key_word = key_value
            name_key_value = '\'' + str(fake.name())+ '\''
        if key_value in ['unique_id_no', 'id_card_no','credit_code'] and key_value not in title:
            id_no_key_word = key_value
            id_no_key_value = '\'' +str(fake.ssn())+ '\''
        if ('phone' == key_value) and key_value not in title:
            phone_key_word = key_value
            phone_key_value =  '\'' +str(fake.phone_number())+ '\''
    # 关联主键不是id，faker一个数字
    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        insert_key_array.append(key_word_sub_table)
        insert_value_array.append(str(fake.random_int()) + str(fake.random_int()))
    query_key_array.append(key_word_sub_table)
    if len(name_key_word) > 0:
        insert_key_array.append(name_key_word)
        insert_value_array.append(name_key_value)
        query_key_array.append(name_key_word)
    if len(id_no_key_word) > 0:
        insert_key_array.append(id_no_key_word)
        insert_value_array.append(id_no_key_value)
        query_key_array.append(id_no_key_word)
    if len(phone_key_word) > 0:
        insert_key_array.append(phone_key_word)
        insert_value_array.append(phone_key_value)
        query_key_array.append(phone_key_word)
    # 主表要插入字段
    if (len(title_array) > 1):
        count = 0
        for info in title_array:
            count = count + 1
            if count > 1:
                key = info.split('=')[0].split('.')[1]
                value = info.split('=')[1]
                if key in ['unique_name', 'user_name', 'name','ent_name']:
                    query_key_array.append(key)
                    name_key_word = key
                    name_key_value = value
                elif key in ['unique_id_no', 'id_card_no','credit_code']:
                    query_key_array.append(key)
                    id_no_key_word = key
                    id_no_key_value = value
                elif key == 'phone':
                    query_key_array.append(key)
                    phone_key_word = key
                    phone_key_value = value
                insert_key_array.append(key)
                if len(str(value).strip()) > 0:
                    insert_value_array.append(str(value))
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
        info_main_table_sql += name_key_word + '=' + name_key_value + ' and '
    if len(id_no_key_word) > 0:
        info_main_table_sql += id_no_key_word + '='+ id_no_key_value + ' and '
    if len(phone_key_word) > 0:
        info_main_table_sql += phone_key_word + '=' + phone_key_value + ' and '
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


# 处理主表对应的子表数据
def _insert_main_table_sub_data(title, df_main_id):
    title = title.replace('\n', '').replace('\r', '')
    value_array = title.split(';')
    table_name = ''
    relation_mian_table_key = ''
    # 字段种类
    field_count = 0
    # 描述有多少组数据
    data_group_count = 0
    insert_value_array = []
    insert_key_array = []
    if len(value_array) > 0:
        table_name = value_array[0].split('.')[0]
        relation_mian_table_key = value_array[0].split('.')[1]
        insert_key_array.append(relation_mian_table_key)
        for detail in value_array:
            if detail.find('[0]') >= 0:
                field_count = field_count + 1
                insert_key_array.append(detail.split('[0]')[0].split('.')[1])
        if field_count > 0:
            data_group_count = (len(value_array) - 1) / field_count
            data_group_count = int(data_group_count)

        if data_group_count > 0:
            for i in range(data_group_count):
                data = []
                data.append(str(df_main_id))
                for detail in value_array:
                    if detail.find('[' + str(i) + ']') >= 0:
                        insert_value = detail.split('=')[1]
                        if len(insert_value) >0:
                            data.append(insert_value)
                        else:
                            data.append('Null')
                insert_value_array.append(data)
        else:
            data = []
            data.append(str(df_main_id))
            insert_value_array.append(data)
    sql = """
         insert into 
         """
    sql += ' ' + table_name + ' ('
    if len(insert_key_array) > 0:
        sql += ','.join(insert_key_array)
    sql += ') values '
    for data in insert_value_array:
        sql += '(' + ','.join(data) + '),'
    sql = sql[0:len(sql) - 1]
    print(sql)
    sql_insert(sql=sql)


class deposit(Process):

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
            key_value = []
            for index, row in no_empty_df.iterrows():
                df_main_id = 0
                for title in title_list:
                    if 'table_main' == title:
                        # 插入第一张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        key = str(row['测试用例key'])
                        channel_api_no = str(row['测试模块'])
                        df_main = _insert_main_table_data(title, key, channel_api_no)
                        df_main_id = df_main['key_sub'][0]
                        key_value.append(df_main['key'][0])
                    if title.find('table_main_sub') >= 0:
                        # 插入第一张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_table_sub_data(title, df_main_id)
            no_empty_df['key_value_main'] = key_value
            return no_empty_df

    def do_process_case(self, read_path=None, write_path=None):
        df = self.read_excel_as_df()
        df_write_excel = self.insert_data(df=df)
        path = self.write_df_into_excel(df=df_write_excel)
        return self.run_processor(path=path)
