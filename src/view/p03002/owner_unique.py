# @Time : 2020/10/21 3:31 PM
# @Author : lixiaobo
# @File : owner.py.py
# @Software: PyCharm
import datetime
import json
import re

import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.common_util import get_query_data, get_all_related_company, logger
from util.mysql_reader import sql_to_df


def translate_marry_state(marry_state):
    if marry_state == 'MARRIED':
        return "已婚"
    elif marry_state == 'UNMARRIED':
        return "未婚"
    elif marry_state == 'MARRIAGE':
        return "初婚"
    elif marry_state == 'REMARRIAGE':
        return "再婚"
    elif marry_state == 'REMARRY':
        return "复婚"
    elif marry_state == 'WIDOWED':
        return "丧偶"
    elif marry_state == 'DIVORCE':
        return "离婚"
    elif marry_state == 'NO_DESC':
        return "未说明的婚姻状况"
    elif marry_state == 'SINGLE':
        return "单身"
    elif marry_state == 'UNKNOWN':
        return "未知"
    else:
        return "未说明的婚姻状况"


class Owner(GroupedTransformer):
    """
    企业主分析_owner
    """

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "owner"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'owner_info_cnt': 1,
            'owner_tax_cnt': 0,
            'owner_list_cnt': 0,
            'owner_app_cnt': 0,
            'owner_age': 0,
            'owner_resistence': '',
            'owner_marriage_status': '',
            'owner_education': '',
            'owner_criminal_score_level': '',
            'owner_list_name': [],
            'owner_list_type': [],
            'owner_list_case_no': [],
            'owner_list_detail': [],
            'owner_job_year': 0,
            'owner_major_job_year': 0,
            'owner_tax_name': [],
            'owner_tax_amt': [],
            'owner_tax_type': [],
            'owner_tax_date': [],
            'owner_app_game': 0,
            'owner_app_shop': 0,
            'owner_app_tel': 0,
            'owner_app_photo': 0,
            'owner_app_system_tool': 0,
            'owner_app_news': 0,
            'owner_app_entertainment': 0,
            'owner_app_tntelligentAI': 0,
            'owner_app_female_parent_child': 0,
            'owner_app_car_service': 0,
            'owner_app_social_networks': 0,
            'owner_app_life_service': 0,
            'owner_app_utilities': 0,
            'owner_app_live_video': 0,
            'owner_app_read': 0,
            'owner_app_office_business': 0,
            'owner_app_manufacturer_ecology': 0,
            'owner_app_health': 0,
            'owner_app_edu': 0,
            'owner_app_financial': 0,
            'owner_app_travel': 0,
            'owner_app_music': 0,
        }
        self.person_list = None
        self.company_list = None
        self.per_type = None

    # 获取对应主体及主体的关联企业对应的欠税信息
    def _info_court_id(self, idno):
        input_info = self.per_type.get(idno)
        if input_info is not None:
            unique_idno = input_info['idno']
            unique_idno_str = '"' + '","'.join(unique_idno) + '"'
            sql = """
                select id 
                from info_court 
                where unique_id_no in (%s) 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                """ % unique_idno_str
            df = sql_to_df(sql=sql)
            if df is not None and df.shape[0] > 0:
                id_list = df['id'].to_list()
                court_id_list = ','.join([str(x) for x in id_list])
                sql = """
                    select *
                    from info_court_tax_arrears
                    where court_id in (%s)
                """ % court_id_list
                df = sql_to_df(sql=sql)
                if df.shape[0] > 0:
                    self.variables['owner_tax_cnt'] = df.shape[0]
                    df.sort_values(by='taxes_time', ascending=False, inplace=True)
                    self.variables['owner_tax_name'] = df['name'].to_list()
                    self.variables['owner_tax_amt'] = df['taxes'].to_list()
                    self.variables['owner_tax_type'] = df['taxes_type'].to_list()
                    self.variables['owner_tax_date'] = df['taxes_time'].apply(
                        lambda x: "" if pd.isna(x) else x.strftime('%Y-%m-%d')).to_list()

    # 获取个人基本信息
    def _indiv_base_info(self, idno):
        idno = str(idno)
        now = datetime.datetime.now()
        self.variables['owner_age'] = now.year - int(idno[6:10]) + \
                                      (now.month - int(idno[10:12]) + (now.day - int(idno[12:14])) // 100) // 100
        self.variables['owner_resistence'] = idno[:6]
        for index in self.person_list:
            temp_id = index.get('id_card_no')
            if str(temp_id) == idno:
                self.variables['owner_marriage_status'] = translate_marry_state(index.get('marry_state'))
                self.variables['owner_education'] = index.get('education')
                break

    # 获取个人公安重点评分
    def _info_criminal_score(self, idno):
        sql = """
            select score 
            from info_criminal_case
            where id_card_no = %(unique_id_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df = sql_to_df(sql=sql, params={'unique_id_no': idno})
        if df.shape[0] == 0:
            return
        score = df['score'].to_list()[0]
        try:
            score = float(score)
        except ValueError:
            score = None
        if score is not None:
            if score > 60:
                level = "A"
            elif score == 60:
                level = "B"
            elif score > 20:
                level = "C"
            else:
                level = "D"
            self.variables['owner_criminal_score_level'] = level

    # 不良记录条数和详情
    def _info_bad_behavior_record(self, idno):
        court_sql = """
            select id 
            from info_court 
            where unique_id_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        court_df = sql_to_df(sql=court_sql, params={'unique_id_no': str(idno)})
        if court_df is not None and court_df.shape[0] > 0:
            court_id = court_df['id'].to_list()[0]
            behavior_sql = """
                select name, '罪犯及嫌疑人' as type, case_no as case_no, criminal_reason as detail 
                from info_court_criminal_suspect where court_id = %(court_id)s
                union all 
                select name, '失信老赖' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_deadbeat where court_id = %(court_id)s and execute_status != '已结案'
                union all 
                select name, '限制高消费' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_limit_hignspending where court_id = %(court_id)s
                union all 
                select name, '限制出入境' as type, execute_no as case_no, execute_content as detail 
                from info_court_limited_entry_exit where court_id = %(court_id)s
            """
            df = sql_to_df(sql=behavior_sql, params={'court_id': court_id})
            if df is not None and df.shape[0] > 0:
                self.variables['owner_list_cnt'] = df.shape[0]
                self.variables['owner_list_name'] = df['name'].to_list()
                self.variables['owner_list_type'] = df['type'].to_list()
                self.variables['owner_list_case_no'] = df['case_no'].to_list()
                self.variables['owner_list_detail'] = df['detail'].apply(lambda x: re.sub(r'\s', '', x)).to_list()

    # 获取每个主体的从业年限和主营业年限
    def _info_operation_period(self, idno):
        main_indu = self.full_msg['strategyParam'].get('industry')
        if main_indu is not None:
            temp_idno = self.per_type.get(idno)
            if temp_idno is not None:
                code_str = '"' + '","'.join(temp_idno['idno']) + '"'
                sql = """
                    select * 
                    from info_com_bus_face 
                    where basic_id in (
                        select id 
                        from info_com_bus_basic 
                        where credit_code in (%s) 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    )
                """ % code_str
                df = sql_to_df(sql=sql)
                df = df[pd.notna(df['es_date'])]
                if df.shape[0] == 0:
                    return
                df['industry_phy_code'] = df['industry_phy_code'].fillna('').astype(str)
                df['industry_code'] = df['industry_code'].fillna('').astype(str)
                df['industry'] = df['industry_phy_code'] + df['industry_code']
                min_es_date = df['es_date'].min()
                self.variables['owner_job_year'] = datetime.datetime.now().year - min_es_date.year
                main_df = df[df['industry'] == main_indu]
                if main_df.shape[0] == 0:
                    return
                temp_min_es_date = main_df['es_date'].min()
                self.variables['owner_major_job_year'] = datetime.datetime.now().year - temp_min_es_date.year

    # 获取对应客户的极光数据
    def _info_jg_v5(self, mobile):
        jg_mapping = {
            'owner_app_game': ['HB_GAME_ALL',
                               'HB_GAME_SPZL',
                               'HB_GAME_AFTG',
                               'HB_GAME_FSTG',
                               'HB_GAME_RTS',
                               'HB_GAME_RPG',
                               'HB_GAME_CB',
                               'HB_GAME_SLG',
                               'HB_GAME_PRAC',
                               'HB_GAME_TDFS',
                               'HB_GAME_TYJJ',
                               'HB_GAME_ELMNT',
                               'HB_GAME_CPL',
                               'HB_GAME_MSC',
                               'HB_GAME_PLF',
                               'HB_GAME_ASST'],
            'owner_app_shop': ['HB_SHOPPING_ALL',
                               'HB_SHOPPING_B2B',
                               'HB_SHOPPING_VERTICAL',
                               'HB_SHOPPING_SECOND_HAND',
                               'HB_SHOPPING_SHARING',
                               'HB_SHOPPING_CROSS_BORDER',
                               'HB_SHOPPING_O2O',
                               'HB_SHOPPING_SELLER_TOOLS',
                               'HB_SHOPPING_RURAL',
                               'HB_SHOPPING_BRAN',
                               'HB_SHOPPING_SOCIAL',
                               'HB_SHOPPING_FRESH_FOOD',
                               'HB_SHOPPING_DISCOUNT_REBATE',
                               'HB_SHOPPING_MALL'],
            'owner_app_tel': ['HB_PHONE_ALL',
                              'HB_PHONE_EMAIL',
                              'HB_PHONE_CS',
                              'HB_PHONE_ADDRESS_BOOK',
                              'HB_PHONE_NETWORK_PHONE',
                              'HB_PHONE_NETWORK_SMS'],
            'owner_app_photo': ['HB_PHOTO_ALL',
                                'HB_PHOTO_SCREENSHOT',
                                'HB_PHOTO_BEAUTIFICATION',
                                'HB_PHOTO_PHOTO_SHARING',
                                'HB_PHOTO_ALBUM_GALLERY'],
            'owner_app_system_tool': ['HB_SYS_ALL',
                                      'HB_SYS_ROOT',
                                      'HB_SYS_WIFI',
                                      'HB_SYS_SM',
                                      'HB_SYS_BACKUP_RECOVERY',
                                      'HB_SYS_BATTERY_MANAGEMENT',
                                      'HB_SYS_DTM',
                                      'HB_SYS_FILE_TRANSFER',
                                      'HB_SYS_FILE_MANAGEMENT',
                                      'HB_SYS_OPTIMIZATION',
                                      'HB_SYS_APP_STORE'],
            'owner_app_news': ['HB_NEWS_ALL',
                               'HB_NEWS_LOCAL',
                               'HB_NEWS_FINANCIAL',
                               'HB_NEWS_OVERSEAS',
                               'HB_NEWS_INDUSTRY',
                               'HB_NEWS_TECHNOLOGY',
                               'HB_NEWS_SPORTS',
                               'HB_NEWS_ENT',
                               'HB_NEWS_POL_MIL',
                               'HB_NEWS_GENERAL'],
            'owner_app_entertainment': ['HB_ENT_ALL',
                                        'HB_ENT_ANIMATION_COMIC',
                                        'HB_ENT_ACGCOMMUNITY',
                                        'HB_ENT_DRAMA_FOLK_ARTS',
                                        'HB_ENT_DOLL_MACHINE',
                                        'HB_ENT_TICKETING'],
            'owner_app_tntelligentAI': ['HB_AI_ALL',
                                        'HB_AI_IOT',
                                        'HB_AI_REMOTE_CONTROL',
                                        'HB_AI_WEAR',
                                        'HB_AI_HOME',
                                        'HB_AI_ACCESSORIES',
                                        'HB_AI_VEHICLE'],
            'owner_app_female_parent_child': ['HB_FAM_ALL',
                                              'HB_FAM_MP',
                                              'HB_FAM_BEAUTY',
                                              'HB_FAM_CHILD_REARING',
                                              'HB_FAM_PARENTING_TOOLS',
                                              'HB_FAM_PC',
                                              'HB_FAM_PBH'],
            'owner_app_car_service': ['HB_CAR_ALL',
                                      'HB_CAR_INSURANCE',
                                      'HB_CAR_CP',
                                      'HB_CAR_INT_CAR',
                                      'HB_CAR_LICENSE',
                                      'HB_CAR_TRANSACTION',
                                      'HB_CAR_COMMUNITY',
                                      'HB_CAR_MAINTENANCE',
                                      'HB_CAR_NEWS',
                                      'HB_CAR_PARKING',
                                      'HB_CAR_SERVICE',
                                      'HB_CAR_RENTAL'],
            'owner_app_social_networks': ['HB_SOCIAL_ALL',
                                          'HB_SOCIAL_DATING',
                                          'HB_SOCIAL_IM',
                                          'HB_SOCIAL_FORUM',
                                          'HB_SOCIAL_CI',
                                          'HB_SOCIAL_ASST',
                                          'HB_SOCIAL_COMMUNITY',
                                          'HB_SOCIAL_GAYDATING',
                                          'HB_SOCIAL_BLOGS_MICROBLOGS'],
            'owner_app_life_service': ['HB_LIFE_ALL',
                                       'HB_LIFE_E_GOVERNMENT',
                                       'HB_LIFE_PROPERTY_AGENT',
                                       'HB_LIFE_LOCAL_INFORMATION',
                                       'HB_LIFE_WEDDING_INVITATION',
                                       'HB_LIFE_HOUSE_DECORATION',
                                       'HB_LIFE_EXPRESS_LOGISTICS',
                                       'HB_LIFE_ESTATE',
                                       'HB_LIFE_FOOD_RECIPES',
                                       'HB_LIFE_EMPLOYMENT',
                                       'HB_LIFE_LOCAL_MARKETPLACE',
                                       'HB_LIFE_MLS',
                                       'HB_LIFE_OPERATOR_SERVICE'],
            'owner_app_utilities': ['HB_TOOL_ALL',
                                    'HB_TOOL_MEASURING',
                                    'HB_TOOL_CALCULATION',
                                    'HB_TOOL_BROWSER',
                                    'HB_TOOL_ALARM_CLOCK_TIMER',
                                    'HB_TOOL_OTHER_TOOLS',
                                    'HB_TOOL_CALENDAR',
                                    'HB_TOOL_FLASHLIGHT',
                                    'HB_TOOL_MOBILE_LOCATION',
                                    'HB_TOOL_MOBILE_LOCK_SCREEN',
                                    'HB_TOOL_KEYBOARD',
                                    'HB_TOOL_SEARCH_DOWNLOAD',
                                    'HB_TOOL_WEATHER_ENVIRONMENT',
                                    'HB_TOOL_BARCODE_QR_CODE',
                                    'HB_TOOL_ASTROLOGY_DIVINATION',
                                    'HB_TOOL_VOICE',
                                    'HB_TOOL_COMPASS',
                                    'HB_TOOL_DESKTOP_THEME'],
            'owner_app_live_video': ['HB_VEDIO_ALL',
                                     'HB_VEDIO_VR',
                                     'HB_VEDIO_TV_SHOW',
                                     'HB_VEDIO_SHORT',
                                     'HB_VEDIO_AGGREGATION',
                                     'HB_VEDIO_TOOLS',
                                     'HB_VEDIO_SPORTS_LIVE',
                                     'HB_VEDIO_WEBCAST',
                                     'HB_VEDIO_GAME_LIVE',
                                     'HB_VEDIO_ONLINE_VIDEO'],
            'owner_app_read': ['HB_READ_ALL',
                               'HB_READ_ENCYCLOPEDIA_QA',
                               'HB_READ_LIGHT_READING',
                               'HB_READ_CULTURE_RELIGION',
                               'HB_READ_AUDIO',
                               'HB_READ_TOOLS',
                               'HB_READ_ONLINE'],
            'owner_app_office_business': ['HB_OB_ALL',
                                          'HB_OB_OFFICE',
                                          'HB_OB_NOTEPAD',
                                          'HB_OB_INVOICING',
                                          'HB_OB_ATTENDANCE',
                                          'HB_OB_CARD',
                                          'HB_OB_APP',
                                          'HB_OB_PRINT',
                                          'HB_OB_CLOUD_STORAGE'],
            'owner_app_manufacturer_ecology': ['HB_FIRM_ALL',
                                               'HB_FIRM_OPPOVIVO',
                                               'HB_FIRM_SMARTISAN',
                                               'HB_FIRM_ASUS',
                                               'HB_FIRM_HUAWEI',
                                               'HB_FIRM_MEIZU',
                                               'HB_FIRM_OTHER',
                                               'HB_FIRM_SAMSUNG',
                                               'HB_FIRM_XIAOMI'],
            'owner_app_health': ['HB_HLTH_ALL',
                                 'HB_HLTH_REGISTER',
                                 'HB_HLTH_TOOL',
                                 'HB_HLTH_SPORTS',
                                 'HB_HLTH_CONSULT',
                                 'HB_HLTH_CARE',
                                 'HB_HLTH_SHOPPING',
                                 'HB_HLTH_MEDICINE'],
            'owner_app_edu': ['HB_EDU_ALL',
                              'HB_EDU_K12',
                              'HB_EDU_DICTIONARY',
                              'HB_EDU_HIGHEREDU',
                              'HB_EDU_TOOL',
                              'HB_EDU_PLATFORM',
                              'HB_EDU_STU_ASSISTANT',
                              'HB_EDU_LANGUAGE',
                              'HB_EDU_EARLYEDU',
                              'HB_EDU_CAREER',
                              'HB_EDU_KNOWLEDGE'],
            'owner_app_financial': ['HB_FIN_ALL',
                                    'HB_FIN_LOAN_CALCULATOR',
                                    'HB_FIN_INSTALLMENT_LOAN',
                                    'HB_FIN_SUPPLY_CHAIN_FINANCE',
                                    'HB_FIN_INTERNET_INSURANCE',
                                    'HB_FIN_CURRENCY_EXCHANGE',
                                    'HB_FIN_FUND_SECURITIES_BROKER',
                                    'HB_FIN_BOOKKEEPING',
                                    'HB_FIN_MEDIA_COMMUNITY',
                                    'HB_FIN_BLOCKCHAIN',
                                    'HB_FIN_SOCIAL_SECURITY_FUND',
                                    'HB_FIN_MOBILE_BANKING',
                                    'HB_FIN_INVESTMENT_FINANCING',
                                    'HB_FIN_CREDIT',
                                    'HB_FIN_PAYMENT',
                                    'HB_FIN_CROWDFUNDING',
                                    'HB_FIN_MAKE_MONEY'],
            'owner_app_travel': ['HB_TRAVEL_ALL',
                                 'HB_TRAVEL_METRO_COMMUTE',
                                 'HB_TRAVEL_NAVIGATION_MAP',
                                 'HB_TRAVEL_TRAVEL_GUIDE',
                                 'HB_TRAVEL_RIDE_SHARING',
                                 'HB_TRAVEL_AIR_SERVICES',
                                 'HB_TRAVEL_TRAIN_BUS',
                                 'HB_TRAVEL_HOTEL_RESERVATION',
                                 'HB_TRAVEL_ONLINE'],
            'owner_app_music': ['HB_MUSIC_ALL',
                                'HB_MUSIC_THEORY_INSTRUMENTS',
                                'HB_MUSIC_KARAOKE',
                                'HB_MUSIC_RADIO',
                                'HB_MUSIC_EDITING',
                                'HB_MUSIC_PLAYER',
                                'HB_MUSIC_RECOGNITION',
                                'HB_MUSIC_ONLINE']
        }
        mobile = str(mobile)
        sql = """
                    select * 
                    from info_audience_tag_item
                    where audience_tag_id = (
                        select id 
                        from info_audience_tag 
                        where mobile = %(mobile)s 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        order by id desc limit 1
                    )
                """
        jg_df = sql_to_df(sql=sql, params={'mobile': mobile})
        if jg_df.shape[0] == 0:
            return
        # 统计样本中高、中、低总数
        high_cnt = 0
        middle_cnt = 0
        low_cnt = 0
        # cnt = 0
        jg_hb_df = jg_df.set_index(['audience_tag_id', 'field_name'])['field_value'].unstack()
        for index, row in jg_df.iterrows():
            for i in jg_mapping.keys():
                third_label = jg_mapping[i]
                for j in third_label:
                    if row['field_name'] == j and row['field_value'] == '高':
                        high_cnt += 1
                    elif row['field_name'] == j and row['field_value'] == '中':
                        middle_cnt += 1
                    elif row['field_name'] == j and row['field_value'] == '低':
                        low_cnt += 1
        jg_hb_df['high_cnt'] = high_cnt
        jg_hb_df['middle_cnt'] = middle_cnt
        jg_hb_df['low_cnt'] = low_cnt
        self.variables['owner_app_cnt'] = 1 if high_cnt + middle_cnt + low_cnt > 0 else 0

        # 计算每个二类标签的分值
        for k, v in jg_mapping.items():
            woe_score = 0
            temp_high_cnt = 0
            temp_middle_cnt = 0
            temp_low_cnt = 0
            temp_df = jg_df[jg_df['field_name'].isin(v)]
            if temp_df.shape[0] == 0:
                continue
            # cnt += 1
            for index, row in jg_df.iterrows():
                for i in v:
                    if row['field_name'] == i and row['field_value'] == '高':
                        temp_high_cnt += 1
                    elif row['field_name'] == i and row['field_value'] == '中':
                        temp_middle_cnt += 1
                    elif row['field_name'] == i and row['field_value'] == '低':
                        temp_low_cnt += 1
            if high_cnt > 0:
                woe_score += temp_high_cnt / high_cnt * 0.6
            if middle_cnt > 0:
                woe_score += temp_middle_cnt / middle_cnt * 0.3
            if low_cnt > 0:
                woe_score += temp_low_cnt / low_cnt * 0.1
            self.variables[k] = round(woe_score * 100, 1)

    def transform(self):
        logger.info("owner_unique_debug start")
        try:
            logger.info("full_msg :%s", json.dumps(self.full_msg))
        except:
            logger.info("full_msg exception")
            logger.info(self.full_msg)
        self.person_list = get_query_data(self.full_msg, 'PERSONAL', '01')
        self.company_list = get_query_data(self.full_msg, 'COMPANY', '01')
        self.per_type = get_all_related_company(self.full_msg)
        id_no = self.id_card_no
        base_type = self.base_type
        phone = self.phone
        if "PERSONAL" in base_type:
            self._indiv_base_info(id_no)
            self._info_court_id(id_no)
            # self._info_criminal_score(id_no)
            self._info_bad_behavior_record(id_no)
            self._info_operation_period(id_no)
            self._info_jg_v5(phone)
