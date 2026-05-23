# @Time : 12/11/20 10:12 AM 
# @Author : lixiaobo
# @File : micro_loan_flowable.py 
# @Software: PyCharm

from jsonpath import jsonpath

from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_each
from mapping.mapper import translate_for_strategy
from product.p_utils import score_to_int, _get_biz_types, _relation_risk_subject, _append_rules, \
    _get_resp_field_value
from util.mysql_reader import sql_to_df
from util.strategy_invoker import invoke_strategy
from view.grouped_mapper_detail import view_variables_scheduler
from view.mapper_detail import STRATEGE_DONE
import pandas as pd
from datetime import datetime

logger = LoggerUtil().logger(__name__)


class MicroLoanFlow(object):
    def __init__(self, json_data):
        self.sql_db = None
        self.df_client = None
        self.json_data = json_data
        self.strategy_param = self.json_data.get('strategyParam')
        self.req_no = self.strategy_param.get('reqNo')
        self.step_req_no = self.strategy_param.get('stepReqNo')
        self.version_no = self.strategy_param.get('versionNo')
        self.product_code = self.strategy_param.get('productCode')
        self.query_data_array = self.strategy_param.get('queryData')

    @staticmethod
    def _create_strategy_resp(strategy_resp, variables, common_detail, subject, json_data):
        resp = {
            'commonDetail': common_detail,
            'subject': subject
        }

        if strategy_resp:
            resp['strategyResult'] = strategy_resp
        if variables:
            resp['strategyInputVariables'] = variables
        return resp

    def operation_year(self, ent_code, main_name, sp_name):
        if ent_code is None or len(ent_code) == 0:
            return 0
        ent_str = '"' + '","'.join(ent_code) + '"'
        sql = """select basic_id, ent_type, es_date from info_com_bus_face where basic_id in 
        (select max(id) as id from info_com_bus_basic where credit_code in (%s) group by credit_code)""" % ent_str
        basic_df = sql_to_df(sql)
        if basic_df.shape[0] == 0:
            return
        # 个体户经营年限
        ind_ent = basic_df[(basic_df['ent_type'].isin(['个体', '个体户'])) &
                           (pd.notna(basic_df['es_date']))]
        ind_year = 0
        if ind_ent.shape[0] > 0:
            for row in ind_ent.itertuples():
                es_date = getattr(row, 'es_date')
                try:
                    temp_date = pd.to_datetime(es_date)
                except ValueError:
                    temp_date = datetime.now()
                temp_year = datetime.now().year - temp_date.year + (datetime.now().month - temp_date.month) / 12
                ind_year = max(ind_year, temp_year)
        # 其他企业经营年限
        nor_ent = basic_df[~basic_df['ent_type'].isin(['个体', '个体户'])]
        alt_year = 0
        con_year = 0
        nor_year = 0
        if nor_ent.shape[0] > 0:
            # 查找出资日期
            nor_id = nor_ent['basic_id'].tolist()
            id_str = ','.join([str(i) for i in nor_id])
            name_str = main_name
            if sp_name is not None:
                name_str += '|' + sp_name
            con_sql = """select * from info_com_bus_shareholder where basic_id in (%s)""" % id_str
            con_df = sql_to_df(con_sql)
            con_df = con_df[(con_df['share_holder_name'].str.contains(name_str)) &
                            (pd.notna(con_df['con_date']))]
            con_id = con_df['basic_id'].tolist()
            for row in con_df.itertuples():
                con_date = getattr(row, 'con_date')
                try:
                    temp_date = pd.to_datetime(con_date)
                except ValueError:
                    temp_date = datetime.now()
                if temp_date >= datetime.now():
                    con_id.remove(getattr(row, 'basic_id'))
                temp_year = datetime.now().year - temp_date.year + (datetime.now().month - temp_date.month) / 12
                con_year = max(con_year, temp_year)
            # 查找变更日期
            alt_sql = """select * from info_com_bus_alter where basic_id in (%s)""" % id_str
            alt_df = sql_to_df(alt_sql)
            alt_df = alt_df[(alt_df['alt_item'].str.contains('股东|股权|出资|投资人|法人|法定代表人')) &
                            (alt_df['alt_af'].str.contains(name_str)) &
                            (~alt_df['alt_be'].str.contains(name_str)) &
                            (pd.notna(alt_df['alt_date']))]
            for row in alt_df.itertuples():
                alt_date = getattr(row, 'alt_date')
                try:
                    temp_date = pd.to_datetime(alt_date)
                except ValueError:
                    temp_date = datetime.now()
                temp_year = datetime.now().year - temp_date.year + (datetime.now().month - temp_date.month) / 12
                alt_year = max(alt_year, temp_year)
            # 剩余企业继续查看成立日期
            nor_df = nor_ent[(~nor_ent['basic_id'].isin(con_id)) &
                             (~nor_ent['basic_id'].isin(alt_df['basic_id'].tolist())) &
                             (pd.notna(nor_ent['es_date']))]
            for row in nor_df.itertuples():
                es_date = getattr(row, 'es_date')
                try:
                    temp_date = pd.to_datetime(es_date)
                except ValueError:
                    temp_date = datetime.now()
                temp_year = datetime.now().year - temp_date.year + (datetime.now().month - temp_date.month) / 12
                nor_year = max(nor_year, temp_year)
        return max(ind_year, nor_year, con_year, alt_year)

    def _strategy_hand(self, json_data, data, product_code, req_no):
        user_name = data.get('name')
        id_card_no = data.get('idno')
        phone = data.get('phone')
        user_type = data.get('userType')
        codes = data.get('bizType')
        base_type = data.get('baseType')
        relation = data.get('relation')
        fund_ratio = data.get('fundratio')
        biz_types = codes.copy()
        biz_types.append('00000')
        strategy_param = json_data.get('strategyParam')
        extra_param = strategy_param.get("extraParam")
        query_data = strategy_param.get('queryData')
        channel_source = 0
        if "channelSource" in extra_param.keys():
            channel_source = extra_param.get("channelSource")
        variables, out_decision_code = translate_for_strategy(product_code, biz_types, user_name, id_card_no, phone,
                                                              user_type, base_type, self.df_client, data, None,
                                                              json_data)
        origin_input = data.get('strategyInputVariables')
        if origin_input is None:
            origin_input = {}
        origin_input['out_strategyBranch'] = ','.join(filter(lambda e: e != "00000", codes))
        # 合并新的转换变量
        variables["segment_name"] = data.get("nextSegmentName")
        # 借款人法院阶段拼接经营年限opera_year
        if data.get("nextSegmentName") == "court" and base_type == "U_PERSONAL":
            ent_code = []
            main_name = None
            sp_name = None
            if query_data is not None:
                for queryer in query_data:
                    if queryer.get('userType') == 'COMPANY':
                        ent_code.append(queryer.get('idno'))
                    if queryer.get('baseType') == 'U_PERSONAL':
                        main_name = queryer.get('name')
                    if queryer.get('baseType') == 'U_PER_SP_PERSONAL':
                        sp_name = queryer.get('name')
            opera_year = self.operation_year(ent_code, main_name, sp_name)
            variables["opera_year"] = round(opera_year,2)
        origin_input["tracking_" + variables["segment_name"]] = 1
        origin_input.update(variables)
        origin_input['channel_source'] = channel_source

        strategy_resp = invoke_strategy(origin_input, product_code, req_no)
        score_to_int(strategy_resp)
        biz_types, categories = _get_biz_types(strategy_resp)
        segment_name = _get_resp_field_value(strategy_resp, "$..segment_name")
        data["segmentName"] = data.get("nextSegmentName")
        data["nextSegmentName"] = segment_name
        data["bizType"] = biz_types

        resp = {}
        self.resp_vars_to_input_vars(strategy_resp, origin_input)
        self._calc_view_variables(base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone,
                                  product_code,
                                  resp, strategy_resp, user_name, user_type, origin_input)

        array = self._get_strategy_second_array(data, fund_ratio, relation, strategy_resp, user_name, user_type)
        return array, resp

    def _get_strategy_second_array(self, data, fundratio, relation, strategy_resp, user_name, user_type):
        array = {
            'name': user_name,
            'idno': data.get('idno'),
            'userType': user_type
        }

        if fundratio is None or fundratio == '':
            array['fundratio'] = 0.00
        else:
            array['fundratio'] = float(fundratio)

        array['relation'] = relation
        array["id"] = data.get("id")
        array["parentId"] = data.get("parentId")

        resp_vars = jsonpath(strategy_resp, "$..Variables")
        if resp_vars and len(resp_vars) > 0:
            array.update(resp_vars[0])

        return array

    @staticmethod
    def _get_json_path_value(strategy_resp, path):
        res = jsonpath(strategy_resp, path)
        if isinstance(res, list) and len(res) > 0:
            return res[0]
        else:
            return 0

    def _calc_view_variables(self, base_type, biz_types, json_data, data, id_card_no, out_decision_code, phone,
                             product_code,
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

    def resp_vars_to_input_vars(self, strategy_resp, variables):
        pass
