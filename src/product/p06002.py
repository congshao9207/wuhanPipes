# @Time : 3/1/21 11:30 AM 
# @Author : lixiaobo
# @File : p06002.py 
# @Software: PyCharm
import json
import traceback

import pandas as pd
import requests
from flask import request
from jsonpath import jsonpath

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_union, invoke_each
from mapping.mapper import translate_for_strategy
from mapping.t00000 import T00000
from mapping.utils.np_encoder import NpEncoder
from product.generate import Generate
from product.p_utils import _build_request, _get_biz_types, _append_rules, score_to_int, _relation_risk_subject
from service.base_type_service_v2 import BaseTypeServiceV2
from strategy_config import obtain_strategy_url
from util.type_converter import format_var
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE

logger = LoggerUtil().logger(__name__)


# 贷后2.0
class P06002(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        try:
            json_data = request.get_json()
            app_id = json_data.get('appId')
            req_no = json_data.get('reqNo')
            product_code = json_data.get('productCode')
            query_data_array = json_data.get('queryData')
            industry = json_data.get("industry")
            pre_report_req_no = json_data.get("preReportReqNo")
            pre_biz_date = json_data.get("preBizDate")
            base_type_service = BaseTypeServiceV2(query_data_array)

            response_array = []
            for data in query_data_array:
                response_array.append(self._shake_hand_response(base_type_service, data, product_code, req_no))
            resp = {
                'appId': app_id,
                'productCode': product_code,
                'reqNo': req_no,
                'industry': industry,
                'queryData': response_array,
                'preReportReqNo': pre_report_req_no,
                'preBizDate': pre_biz_date
            }
            self.response = resp
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    def strategy_process(self):
        # 获取请求参数
        try:
            json_data = request.get_json()
            strategy_param = json_data.get('strategyParam')
            req_no = strategy_param.get('reqNo')
            product_code = strategy_param.get('productCode')
            query_data_array = strategy_param.get('queryData')
            subject = []
            cache_array = []
            # 遍历query_data_array调用strategy
            logger.info("loop invoke begin: %s", req_no)
            index = -1
            count = len(query_data_array)
            for data in query_data_array:
                index = index + 1
                logger.info("loop %s index %d//%d begin", req_no, index, count)
                array, resp = self._strategy_hand(json_data, data, product_code, req_no)
                logger.info("loop %s index %d//%d end", req_no, index, count)
                subject.append(resp)
                cache_array.append(array)
                # 最后返回报告详情
            logger.info("view_variables_scheduler begin :%s", req_no)
            common_detail = view_variables_scheduler(product_code, json_data,
                                                     None, None, None, None, None, None, invoke_union)

            logger.info("view_variables_scheduler end :%s", req_no)
            # 封装第二次调用参数
            variables = self._create_strategy_second_request(cache_array)

            logger.info("final invoke_strategy begin :%s ", req_no)
            strategy_resp = self.invoke_strategy(variables, product_code, req_no)
            logger.info("final invoke_strategy end :%s ", req_no)
            score_to_int(strategy_resp)
            # 封装最终返回json
            resp_end = self._create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data)
            format_var(None, None, -1, resp_end)
            logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
            self.response = resp_end
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data):
        resp = {
            'strategyInputVariables': variables,
            'strategyResult': strategy_resp,
            'commonDetail': common_detail,
            'subject': subject
        }
        return resp

    @staticmethod
    def _create_strategy_second_request(cache_array):
        """
        :param cache_array:
        :return:
        """
        df = pd.DataFrame(cache_array)
        # 取前10行数据
        df_person = df.query('userType=="PERSONAL" and strategy=="01"') \
            .sort_values(by=["fundratio"], ascending=False)

        # 删除重复的数据
        df_person.drop_duplicates(subset=["name", "idno"], inplace=True)

        logger.info("-------df_person\n%s", df_person)

        # 拼接入参variables
        variables = {}
        person_index = 0
        for index, row in df_person.iterrows():
            if row["phantomRelation"]:
                continue

            person_index = person_index + 1
            variables['score_p' + str(person_index)] = row['score']
            variables['score_fraud_p' + str(person_index)] = row['score_fraud']
            variables['score_owner_p' + str(person_index)] = row['score_owner']
            variables['score_bus_p' + str(person_index)] = row['score_bus']
            variables['score_black_p' + str(person_index)] = row['score_black']
            variables['score_fin_p' + str(person_index)] = row['score_fin']
            variables['model_pred_p' + str(person_index)] = row['model_pred']
            variables['td_pred_p' + str(person_index)] = row['td_pred']
            variables['td_pred_new_p' + str(person_index)] = row['td_pred_new']
        # 公司作为入参参与报告分
        df_company = df.query('userType=="COMPANY" and strategy=="01"')
        logger.info("-------df_company\n%s", df_company)
        company_index = 0
        for index, row in df_company.iterrows():
            if row["phantomRelation"] or row['score'] <= 0:
                continue

            company_index = company_index + 1
            variables['score_c' + str(company_index)] = row['score']
            variables['model_pred_c' + str(company_index)] = row['model_pred']
        variables['base_type'] = 'UNION'
        return variables

    def _strategy_hand(self, json_data, data, product_code, req_no):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = data.get('bizType')
        base_type = data.get('baseType')
        relation = data.get('relation')
        fund_ratio = data.get('fundratio')
        strategy = data.get('extraParam').get('strategy')
        biz_types = codes.copy()
        biz_types.append('00000')
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data)
        if user_type == "PERSONAL" and strategy == "01":
            if data.get('extraParam').get('passthroughMsg') is not None:
                main_br_id = data.get('extraParam').get('passthroughMsg').get('mainBrId')
                variables['main_br_id'] = main_br_id
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        origin_input['out_strategyBranch'] = ','.join(filter(lambda e: e != "00000", codes))
        # 合并新的转换变量
        origin_input.update(variables)

        strategy_resp = self.invoke_strategy(origin_input, product_code, req_no)
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)

        resp = {}
        self._calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone,
                                  product_code,
                                  resp, strategy_resp, user_name, user_type, variables)
        array = self._get_strategy_second_array(data, fund_ratio, relation, strategy_resp, user_name, user_type,
                                                variables)
        return array, resp

    @staticmethod
    def invoke_strategy(variables, product_code, req_no):
        strategy_request = _build_request(req_no, product_code, variables)
        logger.info("strategy_request:%s", strategy_request)
        strategy_response = requests.post(obtain_strategy_url(product_code), json=strategy_request)
        logger.debug("strategy_response%s", strategy_response)
        if strategy_response.status_code != 200:
            raise Exception("strategyOne错误:" + strategy_response.text)
        strategy_resp = strategy_response.json()
        error = jsonpath(strategy_resp, '$..Error')
        if error:
            raise Exception("决策引擎返回的错误：" + ';'.join(jsonpath(strategy_resp, '$..Description')))
        return strategy_resp

    def _get_strategy_second_array(self, data, fundratio, relation, strategy_resp, user_name, user_type,
                                   variables):
        array = {
            'name': user_name,
            'idno': data.get('idno'),
            'userType': user_type,
            'strategy': data.get("extraParam").get("strategy"),
            'phantomRelation': data.get("extraParam").get("phantomRelation")
        }
        if fundratio is None or fundratio == '':
            array['fundratio'] = 0.00
        else:
            array['fundratio'] = float(fundratio)

        array['relation'] = relation
        array['per_face_relent_indusCode1'] = self._get_json_path_value(variables, '$..per_face_relent_indusCode1')
        array['com_bus_face_outwardindusCode1'] = self._get_json_path_value(variables,
                                                                            '$..com_bus_face_outwardindusCode1')
        array['com_bus_industrycode'] = self._get_json_path_value(variables, '$..com_bus_industrycode')
        array['score_black'] = self._get_json_path_value(strategy_resp, '$..score_black')
        array['score_owner'] = self._get_json_path_value(strategy_resp, '$..score_owner')
        array['score_fin'] = self._get_json_path_value(strategy_resp, '$..score_fin')
        array['score_fraud'] = self._get_json_path_value(strategy_resp, '$..score_fraud')
        array['score_bus'] = self._get_json_path_value(strategy_resp, '$..score_bus')
        array['score'] = self._get_json_path_value(strategy_resp, '$..score')
        array['model_pred'] = self._get_json_path_value(strategy_resp, '$..model_pred')
        array['td_pred'] = self._get_json_path_value(strategy_resp, '$..td_pred')
        array['td_pred_new'] = self._get_json_path_value(strategy_resp, '$..td_pred_new')
        array["id"] = data.get("id")
        array["parentId"] = data.get("parentId")
        return array

    @staticmethod
    def _get_json_path_value(strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    @staticmethod
    def _calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone, product_code,
                             resp, strategy_resp, user_name, user_type, variables):
        """
        每次循环后封装每个主体的resp信息
        """
        data['strategyInputVariables'] = variables
        if STRATEGE_DONE in biz_types:
            detail = view_variables_scheduler(product_code, json_data, user_name, id_card_no, phone, user_type,
                                              base_type,
                                              data, invoke_each)
            resp['reportDetail'] = detail
        # 处理关联人
        _relation_risk_subject(strategy_resp, out_decision_code)
        resp['strategyResult'] = strategy_resp
        resp['rules'] = _append_rules(biz_types)
        resp['queryData'] = data

    def _shake_hand_response(self, base_type_service, data, product_code, req_no):
        """
        和决策交互，封装response
        :param data:
        :param product_code:
        :param req_no:
        :return:
        """
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        # 获取base_type
        base_type = base_type_service.parse_base_type(data)
        variables = T00000().run(user_name, id_card_no, phone, user_type, base_type, data)['variables']
        # 决策要求一直要加上00000，用户基础信息。
        variables["product_code"] = product_code
        variables['out_strategyBranch'] = '00000'
        logger.info("variables:%s", variables)

        resp_json = self.invoke_strategy(variables, product_code, req_no)
        biz_types, categories = _get_biz_types(resp_json)
        rules = _append_rules(biz_types)

        resp = {}
        resp.update(data)
        resp['baseType'] = base_type
        resp['bizType'] = biz_types
        resp['rules'] = rules
        resp['categories'] = categories

        return resp
