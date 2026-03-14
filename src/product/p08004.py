#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :p08004_v.py.py
# @Time      :2025/2/20 15:35
# @Author    :chenwen

import traceback

import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.mapper import translate_for_strategy
from portrait.portrait_processor import PortraitProcessor
from portrait.transflow.single_portrait import SinglePortrait
from portrait.transflow.union_portrait import UnionPortrait
from product.generate import Generate
from product.p_config import product_codes_dict
from product.p_utils import _build_request, score_to_int, _get_biz_types, _relation_risk_subject, _append_rules
from service.base_type_service_v4 import BaseTypeServiceV4
from strategy_config import obtain_strategy_url
from util.mysql_reader import sql_to_df
from view.mapper_detail import translate_for_report_detail

logger = LoggerUtil().logger(__name__)


# 流水贷授信报告产品处理
class P08004(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}
        self.sql_db = None

    def shake_hand_process(self):
        """
        json_data主体的关联关系
        需要根据关联关系，处理**portrait的相关数据
        """
        try:
            json_data = request.get_json()

            req_no = json_data.get('reqNo')
            report_req_no = json_data.get("preReportReqNo")
            product_code = json_data.get('productCode')
            is_single = json_data.get("single")
            query_data_array = json_data.get('queryData')

            # 转换base_type
            base_type_service = BaseTypeServiceV4(query_data_array)

            main_node = None
            response_array = []
            for data in query_data_array:
                # 入参封装流水账户号
                id_card_no = data['idno']
                # 实名版不再传递担保人流水到画像阶段
                if product_code == '08004' and 'GUAR' in str(data.get('relation')):
                    data['extraParam']['accounts'] = []
                else:
                    sql_trans_acc = '''
                        select id_card_no,bank,account_no from trans_account where id_card_no = %(id_card_no)s
                    '''
                    df_trans_acc = sql_to_df(sql=sql_trans_acc,
                                             params={"id_card_no": id_card_no})
                    if not df_trans_acc.empty:
                        df_trans_acc.drop_duplicates(subset=["bank", "account_no"], inplace=True)
                        accounts = []
                        for index, row in df_trans_acc.iterrows():
                            bank = row['bank']
                            account_no = row['account_no']
                            dict_temp = {"bankAccount": account_no, "bankName": bank}
                            accounts.append(dict_temp)
                        data['extraParam']['accounts'] = accounts
                #
                base_type = base_type_service.parse_base_type(data)
                data["baseTypeDetail"] = base_type
                if data.get("relation") == "MAIN":
                    main_node = data
                else:
                    response_array.append(data)

            # 握手阶段处理流水入口
            resp = self._query_entity_hand_shake(json_data, main_node, query_data_array)
            response_array.append(resp)

            resp = {
                'reqNo': req_no,
                'reportReqNo': report_req_no,
                'productCode': product_code,
                "single": is_single,
                'queryData': response_array
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def _query_entity_hand_shake(self, json_data, data, query_data_array):
        """
        流水握手的相关信息处理
        """
        cached_data = {'label_tree': self._get_label_tree()}
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        base_type = data.get("baseType")

        req_no = json_data.get('reqNo')
        report_req_no = json_data.get("preReportReqNo")
        product_code = json_data.get('productCode')
        is_single = json_data.get("single")

        public_param = {
            "reqNo": req_no,
            "reportReqNo": report_req_no,
            "productCode": product_code,
            "isSingle": is_single,
            "outApplyNo": json_data.get("outApplyNo"),
            "applyAmt": json_data.get("applyAmt"),
            "renewLoans": json_data.get("renewLoans"),
            "historicalBiz": json_data.get("historicalBiz")
        }

        var_item = {
            "bizType": product_codes_dict[json_data.get("productCode")]
        }

        var_item.update(data)
        single, union = self._obtain_portrait_processor(is_single)

        single.sql_db = self.sql_db
        single.init(var_item, query_data_array, user_name, user_type, base_type,
                    id_card_no, phone, data, public_param, cached_data)
        single.process()

        if union is not None:
            union.sql_db = self.sql_db
            union.out_req_no_list = single.out_req_no_list
            union.init(var_item, query_data_array, user_name, user_type, base_type,
                       id_card_no, phone, data, public_param, cached_data)
            union.process()

        return var_item

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            logger.info("1. 流水贷授信报告：获取策略引擎结果，流程开启")
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            step_req_no = strategy_param.get('stepReqNo')
            version_no = strategy_param.get('versionNo')
            pre_report_req_no = strategy_param.get('preReportReqNo')
            query_data_array = strategy_param.get('queryData')
            is_single = strategy_param.get("single")
            renew_loans = strategy_param.get("renewLoans")
            # 上一笔贷款的业务申请流水号
            previous_out_apply_no = strategy_param.get("previousOutApplyNo")

            # 遍历query_data_array调用strategy
            base_type_service = BaseTypeServiceV4(query_data_array)
            main_query_data = None
            subjects = []
            for data in query_data_array:
                # 入参封装流水账户号
                id_card_no = data['idno']
                # 实名版不再传递担保人流水到决策阶段
                if product_code == '08004' and 'GUAR' in str(data.get('relation')):
                    data['extraParam']['accounts'] = []
                else:
                    sql_trans_acc = '''
                        select id_card_no,bank,account_no from trans_account where id_card_no = %(id_card_no)s
                    '''
                    df_trans_acc = sql_to_df(sql=sql_trans_acc,
                                             params={"id_card_no": id_card_no})
                    if not df_trans_acc.empty:
                        df_trans_acc.drop_duplicates(subset=["bank", "account_no"], inplace=True)
                        accounts = []
                        for index, row in df_trans_acc.iterrows():
                            bank = row['bank']
                            account_no = row['account_no']
                            dict_temp = {"bankAccount": account_no, "bankName": bank}
                            accounts.append(dict_temp)
                        data['extraParam']['accounts'] = accounts
                data["preReportReqNo"] = pre_report_req_no
                data["baseTypeDetail"] = base_type_service.parse_base_type(data)
                data["previousOutApplyNo"] = previous_out_apply_no
                subjects.append(data)

                if data.get("relation") == "MAIN":
                    main_query_data = data

            # 决策调用及view变量清洗
            resp = self.strategy(is_single, self.df_client, subjects, main_query_data, product_code, req_no, previous_out_apply_no, renew_loans)

            item_data_list = []
            for subject in subjects:
                item_data = {
                    "queryData": subject
                }

                if subject.get("relation") == "MAIN":
                    item_data.update(resp)

                item_data_list.append(item_data)

            self.response = self.create_strategy_resp(product_code, req_no, step_req_no, version_no, item_data_list)

            temp_resp = self.response.copy()
            # 取决策引擎的final_limit
            s_final_limit = 0
            for item in temp_resp.get('subject'):
                if item['queryData']['relation'] == 'MAIN':
                    s_final_limit = item['strategyResult']['StrategyOneResponse']['Body']['Application']['Variables']['final_limit']
                    item['queryData']['strategyInputVariables']['final_limit'] = s_final_limit
                    break

            self.response = temp_resp
            logger.info("2. 流水贷授信报告，决策调用完成")
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy(self, is_single, df_client, subjects, main_query_data, product_code, req_no, previous_out_apply_no, renew_loans, code_info=None,
                 clean_view_var=True):
        user_name = main_query_data.get('name')
        id_card_no = main_query_data.get('idno')
        phone = main_query_data.get('phone')
        user_type = main_query_data.get('userType')
        codes = product_codes_dict.get(product_code)
        if code_info:
            codes = code_info
        base_type = self.calc_base_type(user_type)
        biz_types = codes.copy()
        biz_types.append('00000')
        out_decision_code = {}
        strategy_resp = {}

        data_repository = {"input_param": subjects, "single": is_single, "previous_out_apply_no": previous_out_apply_no}
        origin_input = {}
        if not is_single:
            variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                                  user_type, base_type, df_client, main_query_data, data_repository)
            origin_input = main_query_data.get("strategyInputVariables")
            if not origin_input:
                origin_input = {}
            origin_input['out_strategyBranch'] = ','.join(codes)
            # 合并新的转换变量
            origin_input.update(variables)
            origin_input['segment_name'] = 'trans'
            origin_input["tracking_trans"] = 1

            logger.info("1. 流水贷授信报告-开始策略引擎封装入参")
            strategy_request = _build_request(req_no, product_code, origin_input)
            logger.info("strategy_request:%s", strategy_request)
            strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
            logger.info("3. 流水贷授信报告-策略引擎返回结果")

            status_code = strategy_response.status_code
            if status_code != 200:
                raise Exception("strategyOne错误:" + strategy_response.text)
            strategy_resp = strategy_response.json()
            logger.info("4. 流水贷授信报告-策略引擎调用成功")
            error = jsonpath(strategy_resp, '$..Error')
            if error:
                raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
            score_to_int(strategy_resp)
            biz_types, categories = _get_biz_types(strategy_resp)
            logger.info(biz_types)
            main_query_data['bizType'] = biz_types

        origin_input["single"] = is_single
        origin_input["renewLoans"] = renew_loans
        resp = {}

        main_query_data["baseType"] = base_type
        main_query_data['strategyInputVariables'] = origin_input
        # 最后返回报告详情
        if clean_view_var:
            detail = translate_for_report_detail(product_code, user_name, id_card_no, phone, user_type,
                                                 base_type, main_query_data, data_repository)
            resp['reportDetail'] = [detail]
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)

        data_repository.clear()
        del data_repository
        return resp

    @staticmethod
    def calc_base_type(user_type):
        if user_type == "PERSONAL":
            return "U_PERSONAL"
        elif user_type == "COMPANY":
            return "U_COMPANY"
        else:
            raise ServerException(code=400, description="不识别的用户类型:" + user_type)

    @staticmethod
    def _obtain_portrait_processor(is_single) -> PortraitProcessor:
        if is_single:
            return SinglePortrait(), None
        return SinglePortrait(), UnionPortrait()

    @staticmethod
    def _get_label_tree():
        sql = "select label_code, label_explanation from label_logic where label_type = 'LABEL'"
        df = sql_to_df(sql)
        res = {getattr(row, 'label_code'): getattr(row, 'label_explanation') for row in df.itertuples()}
        return res
