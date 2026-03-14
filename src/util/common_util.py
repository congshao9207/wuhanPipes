import json
import traceback

import pandas as pd
from numpy import int64
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None and pd.notna(obj):
        return obj.strftime('%Y-%m-%d')
    else:
        return ''


def exception(describe):
    def robust(actual_do):
        def add_robust(*args, **keyargs):
            try:
                return actual_do(*args, **keyargs)
            except Exception as e:
                logger.error(describe)
                logger.error(traceback.format_exc())

        return add_robust

    return robust


def replace_nan(values):
    v_list = [x if pd.notna(x) else 0 for x in values]
    result = []
    for v in v_list:
        if isinstance(v, int64):
            result.append(int(str(v)))
        else:
            result.append(v)

    return result


def get_query_data(msg, query_user_type, query_strategy):
    logger.info("full_msg :%s", json.dumps(msg))

    query_data_list = jsonpath(msg, '$..queryData[*]')
    resp = []
    query_data_list = query_data_list if query_data_list else []
    for query_data in query_data_list:
        if query_data is not None:
            name = query_data.get("name")
            idno = query_data.get("idno")
            user_type = query_data.get("userType")
            strategy = query_data.get("extraParam")['strategy']
            education = query_data.get("extraParam")['education']
            mar_status = query_data.get('extraParam')['marryState']
            priority = query_data.get('extraParam')['priority']
            phone = query_data.get("phone")
            if pd.notna(query_user_type) and user_type == query_user_type and strategy == query_strategy:
                resp_dict = {"name": name, "id_card_no": idno, 'phone': phone,
                             'education': education, 'marry_state': mar_status, 'priority':priority}
                resp.append(resp_dict)
            if pd.isna(query_user_type) and strategy == query_strategy:
                resp_dict = {"name": name, "id_card_no": idno}
                resp.append(resp_dict)
    return resp


def get_all_related_company(msg):
    query_data_list = jsonpath(msg, '$..queryData[*]')
    per_type = dict()
    resp = dict()
    query_data_list = query_data_list if query_data_list else []
    for query_data in query_data_list:
        if query_data is not None:
            name = query_data.get("name")
            idno = query_data.get("idno")
            user_type = query_data.get("userType")
            base_type = query_data.get("baseType")
            strategy = query_data.get("extraParam")['strategy']
            industry = query_data.get("extraParam")['industry']
            if user_type == 'PERSONAL' and strategy == '01':
                resp[idno] = {'name': [name], 'idno': [idno], 'industry': [industry]}
                if base_type == 'U_PERSONAL':
                    per_type['main'] = idno
                elif 'SP' in base_type:
                    per_type['spouse'] = idno
                elif 'CT' in base_type:
                    per_type['controller'] = idno
                # else:
                #     per_type[base_type] = idno
    query_data_list = query_data_list if query_data_list else []
    for query_data in query_data_list:
        if query_data is not None:
            name = query_data.get("name")
            idno = query_data.get("idno")
            user_type = query_data.get("userType")
            base_type = query_data.get("baseType")
            strategy = query_data.get("extraParam")['strategy']
            industry = query_data.get("extraParam")['industry']
            temp_code = None
            if user_type == 'COMPANY' and strategy == '01':
                if 'SP' in base_type:
                    temp_code = per_type.get('spouse')
                if 'CT' in base_type and temp_code is None:
                    temp_code = per_type.get('controller')
                if temp_code is None:
                    temp_code = per_type.get('main')
                if temp_code is not None:
                    resp[temp_code]['name'].append(name)
                    resp[temp_code]['idno'].append(idno)
                    resp[temp_code]['industry'].append(industry)
    return resp


# def get_industry_risk_level(industry_code):
#     '''
#     行业国标由2011版升级至2017版，此版本行业风险作废
#     '''
#     if industry_code in ['H623', 'H622', 'H621', 'L727', 'C183', 'C366', 'P821', 'L726', 'H629', 'C231', 'F523',
#                          'P829', 'F513', 'O795', 'O794']:
#         return "D"
#     elif industry_code in ['F525', 'F514', 'E492', 'C223', 'E501', 'N781', 'A014', 'F518', 'C219',
#                            'F528', 'E470', 'H612', 'C339', 'L729', 'L724', 'H611', 'A021', 'Q839',
#                            'O801', 'F515', 'E502', 'A015', 'A041', 'C175', 'C182', 'C201']:
#         return "C"
#     elif industry_code in ['G543', 'C203', 'F529', 'O799', 'E489', 'L721', 'F511', 'C331', 'F524',
#                            'F517', 'H619', 'F522', 'E499', 'F527', 'C211', 'F526', 'F516', 'L711',
#                            'C292', 'F519', 'G582', 'C352', 'C336', 'E481', 'C335', 'C326', 'C338',
#                            'C342', 'C348', 'C382', 'C419', 'E503', 'G581', 'G599', 'K702', 'L712',
#                            'O811']:
#         return "B"
#     elif industry_code in ['F521', 'F512', 'C135']:
#         return "A"
#     else:
#         return "暂无风险评级"


# def get_industry_risk_tips(industry_code):
#     '''
#     行业国标由2011版升级至2017版，此版本行业风险作废
#     '''
#     resp_list = []
#     if industry_code in ['E501', 'E5010']:
#         resp_list.append("1、如果企业规模小、仍运用传统的、主要以体力为支出的运作模式，抗风险能 力较弱。")
#         resp_list.append("2、请关注该行业的挂靠和转包、分包现象，以及是否有行业等级资质。")
#         resp_list.append("3、请关注隐形负债，该行业垫资多，且多为民营企业，企业发展资金主要依靠自身积累，融资渠道也有限，民间借贷普遍存在。")
#         resp_list.append("4、如果应收账款超年营业额70%以上，请关注应收账款质量，存在坏账可能。")
#         resp_list.append("5、企业主实际经营年限不足3年，风险较高。如果有实力较好的从事相关行业的经营者给与支持或者合作，可适当降低风险。")
#     elif industry_code in ['A041']:
#         resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害风险。")
#         resp_list.append("2、该行业生产周期长，季节性明显，建议将贷款到期日设置在销售旺季。")
#         resp_list.append("3、请关注饲料供应是否有稳定的来源及稳定的价格。")
#         resp_list.append("4、请关注苗种的来源和质量是否稳定。")
#         resp_list.append("5、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
#     elif industry_code in ['F528', 'F5281']:
#         resp_list.append("1、请关注客户所代理的品牌在当地或者在行业内是否有一定的知名度。")
#         resp_list.append("2、请关注客户线下店面所在地的专业市场是否人气不足，整体不景气。")
#         resp_list.append("3、如果销售的五金产品种类少及规模小，请谨慎授信。")
#         resp_list.append("4、3年内经营场地搬迁2次以上，固定资产保障性较弱的客群，请谨慎授信。")
#     elif industry_code in ['E48']:
#         resp_list.append("1、如果经营主体资质不足，一般采用挂靠、转包、分包形式施工的，应收账款回款周期较慢。")
#         resp_list.append("2、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
#         resp_list.append("3、请关注隐形负债，该行业普遍存在应收款质押、设备融资租赁、民间负债多等现象。")
#         resp_list.append("4、请关注法律风险，该行业易产生合同纠纷、劳务纠纷、借款纠纷。")
#         resp_list.append("5、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
#         resp_list.append("6、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
#     elif industry_code in ['H62']:
#         resp_list.append("1、请关注客户的资质完备情况，餐饮行业基本资质证件含《营业执照》、《食品经营许可证》、《卫生安全许可证》、《消防安全许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、如果营业执照非本人，通过租赁场地经营的，该类客户经营稳定性差。")
#         resp_list.append("3、请关注客户从业年限和经营店面开业年限，同一店面经营时间3年以上违约风险会显著降低。")
#         resp_list.append("4、请关注店铺实际口碑情况，可关注大众点评等网评差评内容。")
#         resp_list.append("5、请关注该行业的核心员工稳定度（管理人员、店长、主厨等）。")
#         resp_list.append("6、餐饮行业不建议将装修投资记录资产，并请谨慎评估原始投资资金来源。")
#     elif industry_code in ['F5212']:
#         resp_list.append("1、请关注客户的资质完备情况，商超行业基本资质证件含《营业执照》、《食品经营许可证》、《消防安全许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、请关注公司名下行政处罚是否涉及食品安全问题及货款纠纷等。")
#         resp_list.append("3、请关注客户经营地段是否在人口流动集聚的社区及地域。")
#         resp_list.append("4、请关注经营超市的品牌是否在经营当地具备一定认可度，尤其关注客户自有品牌的市场认可度。")
#         resp_list.append("5、请关注单体商超的实际股份构成，以及企业主其他多元化对外投资情况。")
#         resp_list.append("6、如果企业主实际经营商超行业不足5年，请谨慎授信。")
#     elif industry_code in ['G543', 'G5430']:
#         resp_list.append("1、请关注客户的资质完备情况，物流运输行业基本资质证件含《营业执照》、《道路运输经营许可证》或根据各省市实际情况判定。")
#         resp_list.append("2、请关注客户是否购买车辆保险且是否足额，防范交通事故赔偿风险。")
#         resp_list.append("3、请核实客户公司名下车辆所有权及融资情况，关注是否有已报废的车辆继续营运的情况。")
#         resp_list.append("4、请关注客户近期或者历史有无未结清的交通案件，如果案件涉及金额较大或情节较严重的，请谨慎授信。")
#         resp_list.append("5、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
#         resp_list.append("6、请关注客户在经营当地固定资产的保障情况，该行业的资产投入较集中在经营性资产上（车辆及应收）。")
#     elif industry_code in ['C2438', 'F5245']:
#         resp_list.append("1、请关注品牌在经营当地的知名度和市场接受度。")
#         resp_list.append("2、请关注隐形负债，该行业内拆借及民间借贷情况普遍存在。")
#         resp_list.append("3、请关注多头信贷，该行业普遍存在暗股的情况。")
#         resp_list.append("4、该行业存货价值较高、变现能力强，且存货体积较小，存挪比较方便，对于发生风险后的实际处置力有一定隐患，建议有增信措施。")
#         resp_list.append("5、请关注国际金价波动对企业经营的影响。")
#         resp_list.append("6、如果客户在经营当地无固定资产，请谨慎授信。")
#         resp_list.append("7、如果客户为福建籍，请谨慎授信。")
#     elif industry_code in ['F5261']:
#         resp_list.append("1、请关注隐形负债，该行业车辆融资租赁以及金融机构特殊授信产品普遍存在。")
#         resp_list.append("2、请核实库存车辆的所有权属，建议收集车辆行驶证等资产证明材料。")
#         resp_list.append("3、请关注实时行业政策，该行业受国家消费、能耗、技术等方面政策影响较大。例如：取消新能源汽车的补贴。")
#     elif industry_code in ['F5123']:
#         resp_list.append("1、请关注水果市场变化，该行业受需求供给状况、行业政策、气候变化的影响较大。")
#         resp_list.append("2、请关注客户的存货周转率是否过低，注意与同行业同类型产品比较。")
#         resp_list.append("3、如果客户进货渠道单一，请谨慎授信。")
#         resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
#     elif industry_code in ['E4813']:
#         resp_list.append("1、请关注客户的资质完备情况，没有建筑施工资质的客户往往采用挂靠的形式开展业务，可以核实客户的挂靠协议或项目合同。")
#         resp_list.append("2、请关注法律风险，该行业易产生交通事故、买卖合同纠纷、劳务纠纷、借款纠纷。")
#         resp_list.append("3、请关注隐形负债，该行业内垫资、拆借及民间借贷情况普遍存在。")
#         resp_list.append("4、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
#         resp_list.append("5、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
#     elif industry_code in ['P82']:
#         resp_list.append("1、请关注客户的资质完备情况，教育行业办学资质证件含《办学许可证》、《营业执照》等。")
#         resp_list.append("2、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂。")
#         resp_list.append("3、请关注该行业的声誉以及信誉风险，可通过员工素质、知识水平以及管理规范等方面了解。")
#         resp_list.append("4、请关注客户的机构品牌的市场认可度，如品牌为自创品牌或市场受众较小，请谨慎授信。")
#     elif industry_code in ['F516']:
#         resp_list.append("1、请关注客户经营产品品牌的市场认可度，如产品品牌较为小众，请谨慎授信。")
#         resp_list.append("2、请关注产品市场价格波动风险。")
#         resp_list.append("3、请核实应收账款质量。一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
#         resp_list.append("4、该行业客户资产存货和应收占比高，经营风险相对较大，如果客户固定资产少，保障性弱，请谨慎授信。")
#         resp_list.append("5、该行业的日均与资金调动能力一般高于其他行业，如果客户资金调动能力较差，请谨慎授信。")
#     elif industry_code in ['H61']:
#         resp_list.append("1、请关注客户的资质完备情况，酒店行业基本资质证件含《营业执照》、《消防证》、《特种行业许可证》、《卫生许可证齐全》，且证件地址需与酒店地址一致。")
#         resp_list.append("2、请关注酒店物业的剩余租期是否在贷款期限内，警惕到期不能续租的情况。")
#         resp_list.append("3、请关注酒店经营的年限，以及经营者酒店行业的从业年限，如果酒店经营1年以内，或经营者酒店行业从业2年以内的，请谨慎授信。")
#         resp_list.append("4、请关注酒店投资时间以及金额，酒店价值随投资年限增长而下降。")
#         resp_list.append("5、该行业一般会有多人合伙投资占股情况，请核实酒店的实际控制人、真实股份构成、分红方式以及企业主其他多元化对外投资情况。")
#         resp_list.append("6、如果客户经营非连锁品牌的低端宾馆或一般旅馆，请谨慎授信。")
#         resp_list.append("7、如果客户不参与名下所有占股酒店的实际经营，请谨慎授信。")
#     elif industry_code in ['F5124']:
#         resp_list.append("1、请关注客户的资质完备情况，肉类批发行业基本资质证件含《营业执照》、《食品流通许可证齐全》。")
#         resp_list.append("2、请关注产品市场价格波动风险。")
#         resp_list.append("3、请关注应收账款质量和账期长短，该行业普遍存在应收账款坏账风险和账期不稳定情况。")
#         resp_list.append("4、请关注客户的存货周转率与同行业同类型产品相比是否过低。")
#         resp_list.append(
#             "5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。牛羊肉、鸡鸭肉冻品旺季一般为10-3月；水产虾蟹生鲜的销售旺季一般为4-7月、9-12月，以及端午、中秋、国庆等节假日；海鲜冻品的销售旺季一般在休渔期。")
#     elif industry_code in ['F5274']:
#         resp_list.append("1、请关注企业主手机门店是否有品牌授权以及是否在授权期限内。")
#         resp_list.append("2、请关注客户销售手机的品牌，国内目前华为、vivo、OPPO、小米、苹果5个品牌的市场占有率达90%，如果客户主营产品非主流品牌，建议谨慎授信。")
#         resp_list.append("3、请关注是否有存货积压的风险。")
#         resp_list.append("4、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂，甚至过度举债经营。")
#         resp_list.append("5、请关注门店地段的人口密集程度。")
#     elif industry_code in ['F512', 'F5127']:
#         resp_list.append("1、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
#         resp_list.append("2、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
#         resp_list.append("3、请关注客户的存货周转率与同行业同类型产品是否过低。")
#         resp_list.append("4、如果客户经营产品为饮料、啤酒等，产品保质期要求较严格的，请关注固定资产保障。")
#         resp_list.append("5、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
#         resp_list.append("6、该行业淡旺季明显，建议将贷款到期日设置在销售旺季")
#     elif industry_code in ['F5283']:
#         resp_list.append("1、请关注库存积压情况。")
#         resp_list.append("2、请关注线下零售门店所在专业市场的成熟度及客流量。")
#         resp_list.append("3、请关注零售品牌的市场认可度和销售渠道。")
#         resp_list.append("4、请关注仓库的防火措施是否到位。")
#         resp_list.append("5、该行业属于夕阳行业，请谨慎授信。")
#     elif industry_code in ['F5137']:
#         resp_list.append("1、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
#         resp_list.append("2、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
#         resp_list.append("3、请关注线下零售门店所在专业市场的客流量。")
#         resp_list.append("4、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
#         resp_list.append("5、请关注库存积压情况。")
#         resp_list.append("6、如果客户经营的产品种类多，销量低，请谨慎授信。")
#     elif industry_code in ['F513', 'F5132']:
#         resp_list.append("1、请关注应收账款质量和账期长短，该行业普遍存在应收账款金额较大、账期长、坏账率高的情况。")
#         resp_list.append("2、请关注该行业客户是否有选款不慎，压货过多的情况。")
#         resp_list.append("3、请关注隐形负债，该行业库存、应收资金占用较大，厂房、设备投入较多，人工工资等运营成本较高，设备融资租赁、民间借贷等情况普遍存在。")
#         resp_list.append("4、请关注资产保障性和体外担保设置的合理性，建议有增信措施。")
#     return resp_list


def get_industry_risk_level(industry_code):
    '''
    国标行业升级至2017版，行业风险等级映射
    由于新版本中有部分行业映射成1、2、4级，所以将原本加工成三级行业入参改为四级行业入参，分别进行对照
    :param industry_code: 行业代码
    :return: 风险等级
    '''
    industry_code_3rd = industry_code[:4]
    if industry_code in ["L7291", "O8051"]:
        return "D"
    elif industry_code_3rd in ["C183", "C231", "C367", "F513", "F523", "H621", "H622", "H623", "H629", "L726", "O804", "P831", "P839"]:
        return "D"
    elif "E47" in industry_code:
        return "C"
    elif industry_code_3rd in ["A014", "A015", "A021", "A041", "C141", "C149", "C175", "C182", "C201", "C212", "C219",
                               "C223", "C304", "C339", "E491", "E492", "E501", "E502", "F514", "F515", "F518",
                               "F525", "F528", "H611", "H612", "L725", "L729", "N781", "O811", "Q849", 'H61']:
        return "C"
    elif "A03" in industry_code:
        return "B"
    elif industry_code_3rd in ["A011", "A012", "A013", "A016", "A017",
                               "C142", "C143", "C144", "C145", "C146", "C203", "C213", "C214", "C211", "C221", "C222",
                               "C232", "C233", "C292", "C301", "C303", "C305", "C306", "C307", "C308", "C309", "C302",
                               "C325", "C332", "C333", "C334", "C337", "C331", "C335", "C336", "C338", "C342", "C348",
                               "C352", "C382", "C419", "E482", "E483", "E484", "E485", "E481", "E489", "E499", "E503",
                               "F511", "F516", "F517", "F519", "F522", "F524", "F526", "F527", "F529", "G541", "G542",
                               "G544", "G543", "G591", "G582", "G599", "H619", "K702", "L711", "L712", "L723", "L724",
                               "M752", "L727", "L721", "O801", "O802", "O803", "O805", "O807", "O808", "O809", "O821",
                               "P832", "P833", "P834", "P835"]:
        return "B"
    elif "I" in industry_code:
        return "A"
    elif industry_code_3rd in ["C135", "F512", "F521"]:
        return "A"
    else:
        return "暂无风险评级"


def get_industry_risk_tips(industry_code):
    '''
    国标行业升级至2017版，行业风险话术映射
    :param industry_code: 行业代码
    :return: resp_list：话术列表，默认为空列表
    '''
    resp_list = []
    if "E501" in industry_code:
        # 建筑装饰行业
        resp_list.append("1、请关注该客户经营主体是否具有建筑资质、行业资质证书，了解经营模式是总承包（施工总承包、工程总承包）、分包（专业分包、劳务）、转包还是挂靠，一般情况下挂靠、转包在出现纠纷时，在诉讼地位上处于劣势，若该企业与挂靠、转包方无紧密关系，则需谨慎授信。")
        resp_list.append("2、请关注该客户承接项目类型，分析涉房地产业务（触发两道红线及以上房企）占比，若涉房地产业务占比超50%，则需审慎授信。若应收账款≥年营业额70%，请关注应收账款质量，可能存在坏账或回款周期较长。")
        resp_list.append("3、请关注民间借贷等隐形负债，该行业垫资多，存在较多三角债现象，若应付≥应收金额的50%，请关注该企业资金压力。")
        resp_list.append("4、该行业回款周期长且不稳定，若该客户或实际控制人固定资产少、担保设置弱，请谨慎授信。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif "A041" in industry_code:
        # 水产养殖行业
        resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害等不可抗力风险。")
        resp_list.append("2、该行业生产周期长，季节性明显，建议将贷款到期日设置在销售旺季。")
        resp_list.append("3、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
        resp_list.append("4、请关注该行业是否在当地已形成产业群、是否已有成熟的产业链。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif industry_code in ["F528", "F5281"]:
        # 五金行业 精确匹配
        resp_list.append("1、请关注该客户所代理的品牌在当地或者在行业内是否有一定的知名度。")
        resp_list.append("2、若该客户销售的五金产品种类少且规模小，请谨慎授信。")
        resp_list.append("3、3年内经营场地搬迁2次以上，固定资产保障性较弱的客群，请谨慎授信。")
        resp_list.append("4、请关注该客户工装占比，若工装占比超50%且可核实回款较少，请谨慎授信。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif "E48" in industry_code:
        # 土木工程建筑行业
        resp_list.append("1、请关注该客户涉房地产业务（触发两道红线及以上房企）占比，若房开业务占比超50%，则需审慎授信。")
        resp_list.append("2、请关注该客户经营主体是否具有建筑资质、行业资质证书，了解经营模式是总承包（施工总承包、工程总承包）、分包（专业分包、劳务）、转包还是挂靠，一般情况下挂靠、转包在出现纠纷时，在诉讼地位上处于劣势，若该企业与挂靠、转包方无紧密关系，则需谨慎授信。")
        resp_list.append("3、该行业普遍存在应收款质押、设备融资租赁、民间负债多等现象，请关注中登网登记信息及流水中的拆借记录。")
        resp_list.append("4、请关注法律风险，该行业易产生合同纠纷、劳务纠纷、借款纠纷。")
        resp_list.append("5、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
        resp_list.append("6、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
    elif "H62" in industry_code:
        # 餐饮行业
        resp_list.append("1、请关注客户的资质完备情况，餐饮行业基本资质证件含《营业执照》、《食品经营许可证》、《卫生安全许可证》、《消防安全许可证》等，或根据各省市实际情况判定。")
        resp_list.append("2、该行业从业门槛低，风险普遍较大，若固定资产保障性弱、现金流差，建议谨慎授信。")
        resp_list.append("3、请关注店铺实际口碑情况，可关注大众点评等APP的网评内容。")
        resp_list.append("4、若该客户实际经营年限不足3年，则不建议授信。")
    elif "F521" in industry_code:
        # 综合零售
        resp_list.append("1、请关注客户的资质完备情况，商超行业基本资质证件含《营业执照》、《食品经营许可证》、《消防安全许可证》等，或根据各省市实际情况判定。")
        resp_list.append("2、请关注客户经营地段是否在人口流动集聚的社区及地域。")
        resp_list.append("3、请关注主营超市的实际股份构成，核实是否存在暗股。")
        resp_list.append("4、若该客户实际经营商超行业不足3年，请谨慎授信。")
        resp_list.append("5、该行业供应商铺货现象普遍存在，请关注库存权属及应付规模。")
    elif "G54" in industry_code:
        # 道路运输业
        resp_list.append("1、请关注客户的资质完备情况，物流运输行业基本资质证件含《营业执照》、《道路运输经营许可证》等，或根据各省市实际情况判定。")
        resp_list.append("2、请关注客户是否购买车辆保险且是否足额，防范交通事故赔偿风险。")
        resp_list.append("3、该行业车辆价值占总资产比例较高，请通过发票、购买合同等核实车辆权属。")
        resp_list.append("4、请关注客户近期或者历史有无未结清的交通案件，如果涉案金额较大或情节较严重的，请谨慎授信。")
        resp_list.append("5、请核实下游客户数量及合作年限，关注回款稳定性。")
    elif industry_code in ["C2438", "F5245"]:
        # 黄金珠宝行业
        resp_list.append("1、请关注代理品牌在经营地知名度及市场占有量。")
        resp_list.append("2、请关注该客户从业年限及名下门店数量。")
        resp_list.append("3、请关注隐形负债，该行业展厅铺货、业内拆借及民间借贷情况普遍存在。")
        resp_list.append("4、该行业终端门店通常有多个合伙人，且暗股较多，福建莆田系此现象较为突出，需建立强关联关系统一授信，避免多头授信。")
        resp_list.append("5、该行业主要资产为存货，请重点关注存货价格波动。")
    elif industry_code in ["F5261", "F5262"]:
        # 汽车零售行业
        resp_list.append("1、请关注代理品牌在经营地知名度及市场占有量。")
        resp_list.append("2、请关注隐形负债，该行业车辆融资租赁以及金融机构特殊授信产品普遍存在。")
        resp_list.append("3、请核实主营业务合伙人及库存车辆的所有权归属。")
        resp_list.append("4、请关注实时行业政策，该行业受国家消费、能耗、技术等方面政策影响较大。例如：取消新能源汽车的补贴、国六标准对国五新车价格影响。")
    elif industry_code == "F5123":
        # 果品、蔬菜批发
        resp_list.append("1、请关注水果市场变化，该行业受需求供给状况、行业政策、气候变化的影响较大。")
        resp_list.append("2、请关注客户的存货周转率是否过低，注意与同行业同类型产品比较。")
        resp_list.append("3、如果客户进货渠道单一，请谨慎授信。")
        resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
        resp_list.append("5、请关注客户销售渠道是否稳定，二批、商超等下游回款是否稳定。")
    elif industry_code == "E4813":
        # 市政道路工程
        resp_list.append("1、请关注该客户经营主体是否具有建筑资质、行业资质证书，了解经营模式是总承包（施工总承包、工程总承包）、分包（专业分包、劳务）、转包还是挂靠，一般情况下挂靠、转包在出现纠纷时，在诉讼地位上处于劣势，若该企业与挂靠、转包方无紧密关系，则需谨慎授信。")
        resp_list.append("2、请关注该客户承接项目类型，若项目所在省份政府财政收入情况欠佳，则需审慎授信。若应收账款≥年营业额70%，请关注应收账款的回款周期。")
        resp_list.append("3、请关注民间借贷等隐形负债，该行业垫资多，存在较多三角债现象，若应付≥应收金额的50%，请关注该企业资金压力。")
        resp_list.append("4、该行业回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
        resp_list.append("5、若该客户实际经营年限不足3年，则谨慎授信。")
    elif "P83" in industry_code:
        # 教育行业
        resp_list.append("1、请关注客户的资质完备情况，教育行业办学资质证件含《办学许可证》、《营业执照》等。")
        resp_list.append("2、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂。")
        resp_list.append("3、请关注该行业的声誉以及信誉风险，可通过员工素质、知识水平以及管理规范等方面了解。")
        resp_list.append("4、请关注客户的机构品牌的市场认可度，如品牌为自创品牌或市场受众较小，请谨慎授信。")
        resp_list.append("5、若该客户涉及K12学科类基础教育培训，则不建议授信。")
        resp_list.append("6、在人口负增长，新生儿出生率低的大背景下，若客户为幼儿教育行业，则需谨慎授信。")
    elif "F516" in industry_code:
        # 矿产品、建材及化工产品批发
        resp_list.append("1、请关注该客户涉房地产业务（触发两道红线及以上房企）占比，若房开业务占比超50%，则需审慎授信。")
        resp_list.append("2、请关注客户经营产品品牌的区域市场认可度，如产品品牌较为小众，请谨慎授信。")
        resp_list.append("3、若该行业存货占总资产比例较高，请关注产品市场价格波动风险。")
        resp_list.append("4、请关注应收对象类型，一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
    elif "H61" in industry_code:
        # 住宿业
        resp_list.append("1、请关注该户经营的酒店品牌在经营地知名度及市场占有量，若非连锁品牌的低端宾馆或一般旅馆，建议谨慎授信。")
        resp_list.append("2、请关注客户的资质完备情况，酒店行业基本资质证件含《营业执照》、《消防证》、《特种行业许可证》、《卫生许可证》等，且证件地址需与酒店地址一致。")
        resp_list.append("3、该行业多由品牌方负责运营，占用实控人时间较少，请关注实控人对外投资情况。")
        resp_list.append("4、该行业初始投资较大，门店通常有多个合伙人且暗股情况较普遍，需建立强关联关系统一授信，避免多头授信。")
        resp_list.append("5、请关注酒店剩余租期是否在贷款期限内，合理设置到期日及评估到期续租持续经营能力。")
        resp_list.append("6、若主营酒店开业不足1年或经营者独立运营经验不足3年，则不建议授信。")
    elif industry_code == "F5124":
        # 肉类批发行业
        resp_list.append("1、请关注客户具体从事何种细分肉类批发，若为进口肉类，需关注是否具备报关手续及相关检疫资料。")
        resp_list.append("2、请关注应收/预付账款周期，该行业受上游海运及下游客户影响，可能存在预付款损失或应收款回款周期长及不能全额回收风险。")
        resp_list.append("3、若客户有存货，请实地走访盘点库存价值及归属权，该行业有针对库存做监管仓库存融资贷款。")
        resp_list.append("4、若客户存货周转率与同行业同类型产品相比过低，则重点了解原因及合理评估存货价值。")
        resp_list.append(
            "5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。牛羊肉、鸡鸭肉冻品旺季一般为10-3月；水产虾蟹生鲜的销售旺季一般为4-7月、9-12月，以及端午、中秋、国庆等节假日；海鲜冻品的销售旺季一般在休渔期。")
    elif industry_code == "F5274":
        # 通信设备零售
        resp_list.append("1、目前国内市场苹果、华为、vivo、OPPO及小米等5个品牌的市场占有率达90%，若客户代理的产品为小众，非主流品牌，建议谨慎授信。")
        resp_list.append("2、请关注企业主手机门店是否有有效期内的品牌授权。")
        resp_list.append("3、该行业产品更新换代较快，在计算存货价值时应充分考虑贬值因素合理谨慎估值。")
        resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif industry_code in ["F512", "F5127", "F5226"]:
        # 酒水、饮品批发零售行业 精确匹配
        resp_list.append("1、请关注代理的产品在经营地知名度及市场占有量，销售渠道是否稳定。")
        resp_list.append("2、请关注企业主经营产品品类是否有有效期内的品牌授权。")
        resp_list.append("3、该行业厂家金融及小贷等非银机构介入较多，请重点关注。")
        resp_list.append("4、若客户经营品类为啤酒、饮料等保质期较短的品类，请重点关注客户的存货周转率及品牌方临期产品回购政策。")
        resp_list.append("5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
    elif "C21" in industry_code or industry_code == "F5283":
        # 家具制造及零售行业
        resp_list.append("1、请关注家具制造企业的经营模式是自有品牌还是代工品牌，了解在经营地知名度及市场占有量，销售渠道是否稳定。")
        resp_list.append("2、请关注该企业是否有相关生产资质及环评资质等，若无上述资质，则不建议授信。")
        resp_list.append("3、该行业属于夕阳行业，市场体量逐年收缩，请增加担保谨慎授信。")
    elif industry_code in ["F5138", "F5272"]:
        # 家电批发零售行业
        resp_list.append("1、请关注客户经营产品品牌的区域市场认可度，如产品品牌小众、非主流品牌，请谨慎授信。")
        resp_list.append("2、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
        resp_list.append("3、若客户有存货，请实地走访盘点库存价值及归属权，该行业有针对库存做监管仓库存融资贷款。")
        resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
        resp_list.append("5、若该客户实际经营年限不足3年，则谨慎授信。")
    elif industry_code in ["F513", "F5132", "F523", "F5232"]:
        # 服装批发零售行业 精确匹配
        resp_list.append("1、该行业受电商冲击较大，经营规模及利润下滑，请谨慎授信。")
        resp_list.append("2、请关注该行业客户过季、过时货品处置流通渠道（上游/厂家回购还是自有渠道处理）。")
        resp_list.append("3、请关注存货归属（上游铺货）及应收账款真实有效性（对下游的铺货未完成售卖的计入应收）。")
        resp_list.append("4、该行业融资能力偏弱，若固定资产保障性弱，建议加强担保设置。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif "L711" in industry_code:
        # 机械设备租赁
        resp_list.append("1、该行业设备投入较大，请关注客户民间融资、设备直租/回租等隐形负债。")
        resp_list.append("2、请关注设备的出租率，核实经营性租赁收入净流入是否可以支付融资租赁款项。")
        resp_list.append("3、该行业设备价值占总资产比例较高，请通过发票、购买合同等核实设备权属。")
        resp_list.append("4、请关注该客户设备租赁方类型，分析涉房地产业务（触发两道红线及以上房企）占比，若涉房地产业务占比超30%，则需审慎授信。若应收账款≥年营业额50%，请关注应收账款质量，可能存在坏账或回款周期较长。")
        resp_list.append("5、该行业回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
    elif "E47" in industry_code:
        # 房屋建筑业
        resp_list.append("1、请关注该客户经营主体是否具有建筑资质、行业资质证书，了解经营模式是总承包（施工总承包、工程总承包）、分包（专业分包、劳务）、转包还是挂靠，一般情况下挂靠、转包在出现纠纷时，在诉讼地位上处于劣势，若该企业与挂靠、转包方无紧密关系，则需谨慎授信。")
        resp_list.append("2、请关注该客户承接项目类型，分析涉房地产业务（触发两道红线及以上房企）占比，若涉房地产业务占比超50%，则需审慎授信。若应收账款≥年营业额70%，请关注应收账款质量，可能存在坏账或回款周期较长。")
        resp_list.append("3、请关注民间借贷等隐形负债，该行业垫资多，存在较多三角债现象，若应付≥应收金额的50%，请关注该企业资金压力。")
        resp_list.append("4、该行业回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
        resp_list.append("5、若该客户实际经营年限不足3年，则不建议授信。")
    elif "E49" in industry_code:
        # 建筑安装业
        resp_list.append("1、请关注该客户经营主体是否具有建筑安装资质、行业资质证书，了解经营模式是总承包（施工总承包、工程总承包）、分包（专业分包、劳务）、转包还是挂靠，一般情况下挂靠、转包在出现纠纷时，在诉讼地位上处于劣势，若该企业与挂靠、转包方无紧密关系，则需谨慎授信。")
        resp_list.append("2、请关注该客户承接项目类型，分析涉房地产业务（触发两道红线及以上房企）占比，若涉房地产业务占比超50%，则需审慎授信。若应收账款≥年营业额70%，请关注应收账款质量，可能存在坏账或回款周期较长。")
        resp_list.append("3、请关注应收对象类型，一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
        resp_list.append("4、若该客户实际经营年限不足3年，则谨慎授信。")
    elif "L72" in industry_code:
        # 商业服务业
        resp_list.append("1、该行业轻资产运营，对资金需求较少，需重点核实贷款用途与经营模式是否匹配，若不匹配，则建议谨慎授信。")
        resp_list.append("2、该行业需要较强的人脉关系，行业经验、客户资源、请关注客户的经营管理能力。")
        resp_list.append("3、该行业融资能力偏弱，若实控人及其关联公司名下无不动产积累，请引入强担保谨慎授信。")
        resp_list.append("4、若该客户实际经营年限不足3年，则谨慎授信。")
    elif "C33" in industry_code:
        # 金属制品业
        resp_list.append("1、请关注该企业是否有相关生产资质及环评资质等，若无，则不建议授信。")
        resp_list.append("2、请关注该客户承接项目类型，分析涉房地产业务（触发两道红线及以上房企）占比，若涉房地产业务占比超50%，则需审慎授信。若应收账款≥年营业额70%，请关注应收账款质量，可能存在坏账或回款周期较长。")
        resp_list.append("3、该行业回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
        resp_list.append("4、若该客户实际经营年限不足3年，则不建议授信。")
    elif "C23" in industry_code or "C22" in industry_code:
        # 印刷、造纸、纸制品业
        resp_list.append("1、请关注国家行业政策及当地新规，若为高污染企业，则不建议授信。")
        resp_list.append("2、请关注该企业是否有相关生产资质及环评资质等。")
        resp_list.append("3、该行业中大型规模的头部企业垄断现象普遍，请关注是否有稳定的合作伙伴及销售渠道。")
        resp_list.append("4、该行业设备投资较大且更新换代较快，请注意核实征信上未体现融资租赁负债。")
        resp_list.append("5、该行业回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
    elif "C30" in industry_code:
        # 非金属矿物制品业
        resp_list.append("1、请关注国家行业政策及当地新规，细分行业是否属于夕阳行业或原料加工过程中是否存在高污染高耗能。")
        resp_list.append("2、请关注该企业是否有相关生产资质及环评资质等。")
        resp_list.append("3、请关注该企业销售渠道是否稳定，应收款回收是否及时。")
        resp_list.append("4、关注客户是否存在对外投资，特别是房屋建筑相关行业对外投资。")
        resp_list.append("5、该行业应收账款多，回款周期长且不稳定，若借款人或实际控制人固定资产少、担保设置弱，请谨慎授信。")
    elif "O80" in industry_code:
        # 居民服务业
        resp_list.append("1、该行业初始投资大，影响营收的因素较多，若客源不足，营收不达预期，短时间可能出现闭店。")
        resp_list.append("2、请关注实际经营中是否存在灰色业务，若有合规经营风险，则不建议授信。")
        resp_list.append("3、该行业属于人员密集型行业，人员固定成本高且流动性大，资金压力较大，请关注现金流是否充足。")
        resp_list.append("4、该行业从业门槛低，风险较大，融资能力偏弱，若固定资产保障性弱、现金流差，则不建议授信。")
        resp_list.append("5、该行业股东结构复杂，暗股情况较普遍，需建立强关联关系统一授信，避免多头授信。")
        resp_list.append("6、若经营门店开业不足3年或经营者独立运营经验不足5年，则不建议授信。")
    elif "C14" in industry_code:
        # 食品制造业
        resp_list.append("1、请关注该企业及从业人员是否具备食品生产许可证，食品流通许可证、健康证、环境评估报告等持续、合规生产证书。")
        resp_list.append("2、请关注应收账款质量和账期长短，该行业普遍存在应收账款金额较大、账期长、坏账率高的情况。")
        resp_list.append("3、请关注该企业的食品物流风险，如若在运输过程中，发生有食品损坏、或者因处理时间长、运输环境未达标而导致的食品变质等等都会让该企业蒙受损失亏损。")
    elif "K702" in industry_code:
        # 物业管理
        resp_list.append("1、若该客户从事小区居民物业，一般情况下资金需求较少，若贷款用途不合理，则建议谨慎授信。")
        resp_list.append("2、请关注客户物业经营权、消防验收报告是否在有效期内。")
        resp_list.append("3、若是二房东、经营性物业请关注出租率，若出租率低于50%则不建议授信。")
        resp_list.append("4、请关注该户户籍，从业历史，一般此行业福建蒲城较多，福建籍二房东此行业民间借贷严重，需要核实隐性负债。")
        resp_list.append("5、该行业前期投资大，通常有多个合伙人，且暗股较多，需建立强关联关系统一授信，避免多头授信风险。")
    elif "A01" in industry_code:
        # 农业
        resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害等不可抗力风险。")
        resp_list.append("2、请关注该行业是否在当地已形成产业群、是否已有成熟的产业链，企业主是否有成熟销售渠道。")
        resp_list.append("3、请关注农业产品生长周期及出货时间等，合理设置贷款到期日。")
        resp_list.append("4、该行业融资能力偏弱，建议加强担保设置。")
        resp_list.append("5、请关注该客户的种植经验，种植经验3年以内的，请谨慎授信。")
    elif "A03" in industry_code:
        # 畜牧业
        resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害及疾病等不可抗力风险。")
        resp_list.append("2、请关注该行业是否在当地已形成产业群、是否已有成熟的产业链，企业主是否有成熟销售渠道。")
        resp_list.append("3、请关注禽畜生长周期及出货时间等，合理设置贷款到期日。")
        resp_list.append("4、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
        resp_list.append("5、请关注该客户所养品类的实际养殖经验，若实际经营年限不足3年，则不建议授信。")
    elif "I" in industry_code:
        # 信息传输、软件和信息技术服务业
        resp_list.append("1、请关注客户教育经历、从业年限及是否具有相关的专业技能和专业背景，该细分行业需要有相应的知识储备。")
        resp_list.append("2、该行业一般人员成本和研发成本支出较高，若固定资产保障性弱且现金流较差，则不建议授信。")
        resp_list.append("3、请关注该户人工支出占比及社保公积金缴纳情况，一般来说社保及公积金支出越多，该公司经营风险越低。")
        resp_list.append("4、请关注客户所属细分行业发展前景、自身产品的核心竞争力及合作方的稳定性。")
        resp_list.append("5、若客户为积累较少的初创企业，实际经营年限不足3年，建议增加担保谨慎授信。")
    return resp_list
