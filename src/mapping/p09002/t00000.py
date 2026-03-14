from mapping.tranformer import Transformer
from dateutil.parser import parse
import pandas as pd
import math
import time
from datetime import datetime

from resources.resource_util import get_config_content, get_config_content_gbk
from util.id_card_info import GetInformation


def trans_string_time_to_datetime(order_time):
    dt = datetime.strptime(order_time, '%Y-%m-%d %H:%M:%S')
    return dt


def get_native_place(area_code):
    # f = open('resources/areaCode_hash.txt',encoding='gbk')
    f = get_config_content_gbk('areaCode_hash.txt')
    native_place = ''
    is_find = 0
    for line in f:
        line = line.strip('\n')  # 去掉换行符
        line = line.split('+')  # 每一行以"+"分隔
        s = line[0]
        s_p = "{" + s.replace("\t,", "") + "}"
        s_p_dict = eval(s_p)
        dict_keys = s_p_dict.keys()
        for key in dict_keys:
            if key == area_code:
                native_place = s_p_dict[key]
                is_find = 1
                break
        if is_find > 0:
            break
    f.close()
    return native_place


class T00000(Transformer):

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'legal_age': 0,  # 法人年龄
            'legal_native_place': None,  # 法人籍贯
            'med_inst_lis_end_ddl': None,  # 医疗执业许可证-有效期距离到期
            'biz_lis_end_ddl': None,  # 营业执照-营业期限到期
            'legal_diff': 0,  # 法定代表人是否同一人
            'is_eme_contact_info_empty': 0,  # 紧急联系人信息缺失
            'is_phone_equal': 0,  # 紧急联系人信息虚假
            'stageIndex': 1,
            'base_type': None
        }

    def transform(self):
        self.policy_var_clean()

    def policy_var_clean(self):
        user_type = self.user_type
        self.variables['base_type'] = self.base_type
        extra_param = self.full_msg['strategyParam']['extraParam']
        self.variables['stageIndex'] = extra_param['stageIndex']
        if user_type == 'PERSONAL':
            information = GetInformation(self.id_card_no)
            self.variables['legal_age'] = information.get_age()
            if extra_param["personalBasicInfo"].get("legalName") == extra_param["registerBasicInfo"].get(
                    "busLicencesLegal"):
                self.variables['legal_diff'] = 1
            id_card_no = self.id_card_no
            area_code = id_card_no[0:4]
            legal_native_place = get_native_place(area_code)
            self.variables['legal_native_place'] = legal_native_place
        else:
            biz_lis_end_dt = extra_param["registerBasicInfo"].get("busLicencesEndAt")
            if biz_lis_end_dt is not None:
                biz_lis_end_ddl = (
                        pd.datetime.now() - trans_string_time_to_datetime(biz_lis_end_dt)).days
                if biz_lis_end_ddl < 0:
                    biz_lis_end_ddl = - biz_lis_end_ddl
                self.variables['biz_lis_end_ddl'] = biz_lis_end_ddl
            med_inst_lis_end_dt = extra_param["registerBasicInfo"].get("insLicencesEndAt")
            if med_inst_lis_end_dt is not None:
                med_inst_lis_end_ddl = (
                        pd.datetime.now() - trans_string_time_to_datetime(med_inst_lis_end_dt)).days
                if med_inst_lis_end_ddl < 0:
                    med_inst_lis_end_ddl = - med_inst_lis_end_ddl
                self.variables['med_inst_lis_end_ddl'] = med_inst_lis_end_ddl
            contactAName = extra_param["personalBasicInfo"].get("contactAName")
            contactAPhone = extra_param["personalBasicInfo"].get("contactAPhone")
            contactBName = extra_param["personalBasicInfo"].get("contactBName")
            contactBPhone = extra_param["personalBasicInfo"].get("contactBPhone")
            if contactAName is None and contactAPhone is None and contactBName is None and contactBPhone is None:
                self.variables['is_eme_contact_info_empty'] = 1
            legalPhone = extra_param["personalBasicInfo"].get("legalPhone")
            if contactAPhone is not None and contactBPhone is not None and legalPhone is not None and (
                    contactAPhone == legalPhone or contactBPhone == legalPhone):
                self.variables['is_phone_equal'] = 1
