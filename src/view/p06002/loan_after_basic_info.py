import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.mysql_reader import sql_to_df
from view.p06002.loan_after import LoanAfter


class LABasicInfo(GroupedTransformer):
    """
    新版贷后报告主页信息，此文件仅调用一次
    """
    def __init__(self):
        super().__init__()
        self.loan_before_obj = None
        self.loan_after_obj = None
        self.last_report_time = None
        self.variables = {
            "basic_increase_company": None,  # 贷后新增查询对象-企业
            "basic_increase_person": None,  # 贷后新增查询对象-个人
            "basic_decrease_obj": None,  # 贷后减少查询对象
            # 贷后新增企业基本信息
            "basic_inc_com_ent_name": None,  # 贷后新增企业企业名称
            "basic_inc_com_ent_type": None,  # 贷后新增企业企业类型
            "basic_inc_com_fr_name": None,  # 贷后新增企业法定代表人
            "basic_inc_com_credit_code": None,  # 贷后新增企业统一社会信用代码
            "basic_inc_com_es_date": None,  # 贷后新增企业成立日期
            "basic_inc_com_reg_cap": None,  # 贷后新增企业注册资本
            "basic_inc_com_appr_date": None,  # 贷后新增企业核准日期
            "basic_inc_com_ent_status": None,  # 贷后新增企业经营状态
            "basic_inc_com_industry_phyname": None,  # 贷后新增企业行业门类
            "basic_inc_com_open_from": None,  # 贷后新增企业营业期限
            "basic_inc_com_reg_addr": None,  # 贷后新增企业注册地址
            "basic_inc_com_zs_ops_code": None,  # 贷后新增企业经营范围
            # 贷后新增企业股东信息
            "basic_inc_com_share_holder_name": [],  # 贷后新增企业股东名称
            "basic_inc_com_share_holder_type": [],  # 贷后新增企业股东类型
            "basic_inc_com_sub_conam": [],  # 贷后新增企业认缴出资额
            "basic_inc_com_funded_ratio": [],  # 贷后新增企业出资比例
            "basic_inc_com_con_date": [],  # 贷后新增企业出资日期
            "basic_inc_com_con_form": [],  # 贷后新增企业出资方式
            # 贷后新增个人基本信息
            "basic_inc_per_cus_name": None,  # 贷后新增个人姓名
            "basic_inc_per_indiv_brt_place": None,  # 贷后新增个人籍贯
            "basic_inc_per_mobile": None,  # 贷后新增个人手机号
            "basic_inc_per_cert_code": None,  # 贷后新增个人身份证号
            # 贷后新增对象个数
            "basic_inc_obj_cnt": None,  # 贷后新增关联关系个数
            "basic_dec_obj_cnt": None,  # 贷后减少关联关系个数
            # 原有企业新增股东信息
            "basic_original_com_ent_name": [],  # 企业名称
            "basic_original_com_share_holder_name": [],  # 股东名称
            "basic_original_com_share_holder_type": [],  # 股东类型、
            "basic_original_com_sub_conam": [],  # 认缴出资额
            "basic_original_com_funded_ratio": [],  # 出资比例
            "basic_original_com_con_date": [],  # 出资日期
            "basic_original_com_con_form": []  # 出资方式
        }

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "basic"

    def obj_inc_dec(self):
        after_df = pd.DataFrame(self.loan_after_obj)
        if self.loan_before_obj is not None:
            before_df = pd.DataFrame(self.loan_before_obj)
        else:
            before_df = pd.DataFrame(columns=after_df.columns)
        inc_df = after_df[~after_df['idno'].isin(before_df['idno'].tolist())]
        dec_df = before_df[~before_df['idno'].isin(after_df['idno'].tolist())]
        intersection_df = after_df[(after_df['idno'].isin(before_df['idno'].tolist())) &
                                   (after_df['userType'] == 'COMPANY')]
        self.variables['basic_inc_obj_cnt'] = inc_df.shape[0]
        self.variables['basic_dec_obj_cnt'] = dec_df.shape[0]
        if inc_df.shape[0] > 0:
            inc_com_df = inc_df[inc_df['userType'] == 'COMPANY']
            inc_per_df = inc_df[inc_df['userType'] == 'PERSONAL']
            if len(inc_com_df) > 0:
                inc_com_list = inc_com_df['idno'].tolist()
                # basic_id_list = inc_com_df['basic_id'].tolist()
                self.variables['basic_increase_company'] = inc_com_df['name'].tolist()
                basic_id_list = self.inc_com_bus_face_info(inc_com_list)
                self.inc_com_bus_shareholder_info(basic_id_list)
            if len(inc_per_df) > 0:
                self.variables['basic_increase_person'] = inc_per_df['name'].tolist()
                self.variables['basic_inc_per_cus_name'] = inc_per_df['name'].tolist()
                self.variables['basic_inc_per_indiv_brt_place'] = inc_per_df['idno'].apply(lambda x: x[:6]).tolist()
                self.variables['basic_inc_per_mobile'] = inc_per_df['phone'].tolist()
                self.variables['basic_inc_per_cert_code'] = inc_per_df['idno'].tolist()
        if dec_df.shape[0] > 0:
            self.variables['basic_decrease_obj'] = dec_df['name'].tolist()
        if intersection_df.shape[0] > 0:
            inter_com_list = intersection_df['name'].tolist()
            self.origin_com_inc_shareholder_info(inter_com_list)

    def inc_com_bus_face_info(self, inc_com_list):
        inc_com_str = '"' + '","'.join(inc_com_list) + '"'
        sql = """
            select * from info_com_bus_face where basic_id in (
                select max(id) as id from info_com_bus_basic where credit_code in (%s) and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at) group by credit_code)
        """ % inc_com_str
        df = sql_to_df(sql)
        res = []
        if df.shape[0] > 0:
            df['open_from'] = df['open_from'].apply(lambda x: pd.to_datetime(x)
                                                    if pd.notna(x) and x != '' and str(x)[:4] != '9999' else '')
            df['open_to'] = df['open_to'].apply(lambda x: pd.to_datetime(x)
                                                if pd.notna(x) and x != '' and str(x)[:4] != '9999' else '')
            df['es_date'] = df['es_date'].apply(lambda x: format(pd.to_datetime(x), '%Y-%m-%d')
                                                if pd.notna(x) and x != '' else None)
            df['appr_date'] = df['appr_date'].apply(lambda x: format(pd.to_datetime(x), '%Y-%m-%d')
                                                    if pd.notna(x) and x != '' else None)
            now = pd.datetime.now()
            df['open_year'] = df['open_from'].apply(
                lambda x: now.year - x.year if pd.notna(x) and x != '' else 0)
            self.variables['basic_inc_com_ent_name'] = df['ent_name'].tolist()
            self.variables['basic_inc_com_ent_type'] = df['ent_type'].tolist()
            self.variables['basic_inc_com_fr_name'] = df['fr_name'].tolist()
            self.variables['basic_inc_com_credit_code'] = df['credit_code'].tolist()
            self.variables['basic_inc_com_es_date'] = df['es_date'].tolist()
            self.variables['basic_inc_com_reg_cap'] = df['reg_cap'].tolist()
            self.variables['basic_inc_com_appr_date'] = df['appr_date'].tolist()
            self.variables['basic_inc_com_ent_status'] = df['ent_status'].tolist()
            self.variables['basic_inc_com_industry_phyname'] = df['industry_phyname'].tolist()
            self.variables['basic_inc_com_open_from'] = df['open_year'].tolist()
            self.variables['basic_inc_com_reg_addr'] = df['address'].tolist()
            self.variables['basic_inc_com_zs_ops_code'] = df['zs_ops_cope'].tolist()
            for com in inc_com_list:
                temp_df = df[df['credit_code'] == com]
                if temp_df.shape[0] > 0:
                    res.append(temp_df['basic_id'].tolist()[0])
                else:
                    res.append(-1)
        return res

    def inc_com_bus_shareholder_info(self, basic_id_list):
        temp_id_list = [str(x) for x in basic_id_list]
        inc_com_str = ','.join(temp_id_list)
        sql = """
            select * from info_com_bus_shareholder where basic_id in (%s) and funded_ratio >= 0.2
        """ % inc_com_str
        df = sql_to_df(sql)
        if len(basic_id_list) > 0 and df.shape[0] > 0:
            df['con_date'] = df['con_date'].apply(lambda x: format(pd.to_datetime(x), '%Y-%m-%d')
                                                  if pd.notna(x) and x != '' else None)
            for basic_id in basic_id_list:
                temp_df = df[df['basic_id'] == basic_id]
                self.variables['basic_inc_com_share_holder_name'].append(temp_df['share_holder_name'].tolist())
                self.variables['basic_inc_com_share_holder_type'].append(temp_df['share_holder_type'].tolist())
                self.variables['basic_inc_com_sub_conam'].append(temp_df['sub_conam'].tolist())
                self.variables['basic_inc_com_funded_ratio'].append(temp_df['funded_ratio'].tolist())
                self.variables['basic_inc_com_con_date'].append(temp_df['con_date'].tolist())
                self.variables['basic_inc_com_con_form'].append(temp_df['con_form'].tolist())

    def origin_com_inc_shareholder_info(self, inter_com_list):
        inter_com_str = '"' + '","'.join(inter_com_list) + '"'
        df1 = None
        if self.last_report_time is not None:
            sql1 = """
                select b.ent_name, a.* from info_com_bus_shareholder a inner join (
                    select id, ent_name from info_com_bus_basic where ent_name in (%s) and 
                    "%s" between create_time and expired_at
                ) b where a.basic_id = b.id and a.funded_ratio > 0.2
            """ % (inter_com_str, str(self.last_report_time))
            df1 = sql_to_df(sql1)
        sql2 = """
            select b.ent_name, a.* from info_com_bus_shareholder a inner join (
                select id, ent_name from info_com_bus_basic where ent_name in (%s) and 
                unix_timestamp(NOW()) < unix_timestamp(expired_at)
            ) b where a.basic_id = b.id and a.funded_ratio > 0.2
        """ % inter_com_str
        df2 = sql_to_df(sql2)
        if df1 is None:
            df1 = pd.DataFrame(columns=df2.columns)
        if df2.shape[0] > 0:
            df2['con_date'] = df2['con_date'].apply(lambda x: format(pd.to_datetime(x), '%Y-%m-%d')
                                                    if pd.notna(x) and x != '' else None)
            for row in df2.itertuples():
                temp_name = getattr(row, 'ent_name')
                temp_shareholder_name = getattr(row, 'share_holder_name')
                temp_funded_ratio = getattr(row, 'funded_ratio')
                temp_df = df1[(df1['ent_name'] == temp_name) &
                              (df1['share_holder_name'] == temp_shareholder_name) &
                              (df1['funded_ratio'] == temp_funded_ratio)]
                if temp_df.shape[0] == 0:
                    self.variables['basic_original_com_ent_name'].append(temp_name)
                    self.variables['basic_original_com_share_holder_name'].append(temp_shareholder_name)
                    self.variables['basic_original_com_share_holder_type'].append(getattr(row, 'share_holder_type'))
                    self.variables['basic_original_com_sub_conam'].append(getattr(row, 'sub_conam'))
                    self.variables['basic_original_com_funded_ratio'].append(temp_funded_ratio)
                    self.variables['basic_original_com_con_date'].append(getattr(row, 'con_date'))
                    self.variables['basic_original_com_con_form'].append(getattr(row, 'con_form'))

    def transform(self):
        LoanAfter.init_grouped_transformer(self)
        self.obj_inc_dec()
