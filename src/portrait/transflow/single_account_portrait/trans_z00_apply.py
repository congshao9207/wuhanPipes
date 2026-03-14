
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
# from portrait.transflow.single_account_portrait.trans_mapping import base_type_mapping
from util.mysql_reader import sql_to_df
# import pandas as pd
import datetime


class TransApply:
    """
    业务申请表,将业务相关的信息存入业务申请表中
    author:汪腾飞
    created_time:20200708
    updated_time_v1:20200709 单个关联人有多个账号均需要落库
    """
    def __init__(self, trans_flow):
        self.report_req_no = trans_flow.report_req_no
        # self.month_interval = trans_flow.month_interval
        self.app_no = trans_flow.app_no
        self.cus_name = trans_flow.user_name
        self.query_data_array = trans_flow.query_data_array
        self.product_code = trans_flow.product_code
        self.db = trans_flow.db
        self.role_list = list()

    @staticmethod
    def _get_object_attr(dict_object, attr_name):
        attr_value = dict_object.get(attr_name, '')
        if attr_value is not None and attr_value != '':
            return attr_value

    def save_trans_apply_data(self):
        """
        将业务申请编号与客户上传流水关联在一起并落库
        :return:
        """
        length = len(self.query_data_array)
        sql_compile = """
                select id as account_id, file_id   
                from trans_account where id_card_no='%s' and account_no='%s' and bank = '%s'
            """
        # limit_time = months_ago(datetime.datetime.now(), self.month_interval)
        # limit_time = datetime.datetime.strftime(limit_time, '%Y-%m-%d %H:%M:%S')
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        apply_exist_id = []
        for i in range(length):
            temp = self.query_data_array[i]
            temp_dict = dict()
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['apply_no'] = self.app_no
            temp_dict['cus_name'] = self.cus_name
            related_name = self._get_object_attr(temp, 'name')
            if related_name is not None:
                temp_dict['related_name'] = related_name
            relationship = self._get_object_attr(temp, 'baseTypeDetail')
            # 不转成中文，直接存英文
            # if relationship is not None:
            #     temp_rel = base_type_mapping.get(relationship)
            #     if temp_rel is None:
            #         continue
            #     temp_dict['relationship'] = temp_rel
            temp_dict['relationship'] = relationship
            id_card_no = self._get_object_attr(temp, 'idno')
            if id_card_no is not None:
                temp_dict['id_card_no'] = id_card_no
                # if id_card_no not in apply_exist_id and relationship != 'G_PERSONAL':
                #     apply_exist_id.append(id_card_no)
            id_type = self._get_object_attr(temp, 'userType')
            if id_type is not None:
                if id_type == 'PERSONAL':
                    temp_dict['id_type'] = 'ID_CARD_NO'
                elif id_type == 'COMPANY':
                    temp_dict['id_type'] = 'CREDIT_CODE'
                else:
                    temp_dict['id_type'] = 'OTHER'

            if temp.__contains__('extraParam'):
                industry = self._get_object_attr(temp['extraParam'], 'industryName')
                if industry is not None:
                    temp_dict['industry'] = industry
                if temp['extraParam'].__contains__('accounts') and \
                        type(temp['extraParam']['accounts']) == list and len(temp['extraParam']['accounts']) > 0:
                    if temp['extraParam'].__contains__('fileIds') and temp['extraParam']['fileIds'] is not None and \
                            len(temp['extraParam']['fileIds']) > 0:
                        fileids = temp['extraParam']['fileIds']
                    else:
                        fileids = []

                    # 此处修改  担保人和担保企业包含在内
                    if id_card_no not in apply_exist_id:
                        apply_exist_id.append(id_card_no)
                        for j in range(len(temp['extraParam']['accounts'])):
                            if temp_dict.__contains__('account_id'):
                                temp_dict.pop('account_id')
                            temp_data = temp['extraParam']['accounts'][j]
                            bank_no = self._get_object_attr(temp_data, 'bankAccount')
                            bank = self._get_object_attr(temp_data, 'bankName')
                            if bank_no is not None and id_card_no is not None:
                                temp_df = sql_to_df(sql_compile % (id_card_no, bank_no, bank))
                                if len(temp_df) > 0:
                                    # 20230117，非实名版若传了file_id，但是该账号的流水与file_id没有交集，则该账号数据不落库
                                    # 20240620 流水3.0 不再区分product_code，统一筛选file_id
                                    if len(fileids) > 0 and temp_df[temp_df['file_id'].isin(fileids)].shape[0] == 0:
                                        continue
                                    temp_dict['account_id'] = temp_df['account_id'].tolist()[0]
                            temp_dict['create_time'] = create_time
                            temp_dict['update_time'] = create_time
                            # role = transform_class_str(temp_dict, 'TransApply')
                            self.role_list.append(temp_dict.copy())
                    else:
                        temp_dict['create_time'] = create_time
                        temp_dict['update_time'] = create_time
                        # role = transform_class_str(temp_dict, 'TransApply')
                        self.role_list.append(temp_dict.copy())

                else:
                    temp_dict['create_time'] = create_time
                    temp_dict['update_time'] = create_time
                    # role = transform_class_str(temp_dict, 'TransApply')
                    self.role_list.append(temp_dict.copy())
            else:
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time
                # role = transform_class_str(temp_dict, 'TransApply')
                self.role_list.append(temp_dict.copy())
        transform_class_str(self.role_list, 'TransApply')
        # self.db.session.bulk_save_objects(self.role_list)
        # self.db.session.add_all(self.role_list)
        # self.db.session.commit()
