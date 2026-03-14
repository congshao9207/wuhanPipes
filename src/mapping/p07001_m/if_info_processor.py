import pandas as pd

from mapping.module_processor import ModuleProcessor

# 和CCS数据比较相关的变量清洗
from mapping.p07001_m.calculator import marry_code_to_enum


class IfInfoProcessor(ModuleProcessor):
    def process(self):
        credit_base_df = self.cached_data["credit_base_info"]
        credit_person_df = self.cached_data["pcredit_person_info"]

        # 与ccs姓名比对
        self._if_name(credit_base_df, credit_person_df)
        # 与ccs手机号比对
        self._phone_alt(credit_base_df, credit_person_df)
        # 与ccs身份证号比对
        self._if_cert_no(credit_base_df, credit_person_df)
        # 与ccs婚姻状况比对
        self._if_marriage(credit_base_df, credit_person_df)
        # 与ccs通讯地址比对
        self._if_postal_addr(credit_base_df, credit_person_df)
        # 与ccs户籍地址比对
        self._if_residence_addr(credit_base_df, credit_person_df)
        # 与ccs居住地址比对
        self._if_live_addr(credit_base_df, credit_person_df)
        # 是否是员工
        self._if_employee(credit_base_df, credit_person_df)
        # 是否是公检法人员
        self._if_official(credit_base_df, credit_person_df)
        # 与ccs配偶姓名匹配
        self._if_spouse_name(credit_base_df, credit_person_df)
        # 与ccs配偶身份证匹配
        self._if_spouse_cert_no(credit_base_df, credit_person_df)

    def _if_name(self, credit_base_df, credit_person_df):
        # 1.从ccs.cus_indiv中选择cert_code=certificate_no的cus_name;
        # 2.入参name=1中结果,则if_name=0,否则=1
        if credit_base_df.empty:
            return
        self.variables["if_name"] = 0 if self.user_name == credit_base_df.iloc[0].name else 1

    def _phone_alt(self, credit_base_df, credit_person_df):
        # 1.从pcredit_phone_his中选取report_id=report_id且no=1的phone;
        # 2.从ccs.cus_indiv中选择cus_name=name且cert_code=certificate_no的mobile,phone,fphone;
        # 3.若1中phone=2中任意结果,则phone_alt=0,否则1
        phone_his_df = self.cached_data["pcredit_phone_his"]
        # 变更为手机总条数2020-05-25
        self.variables["phone_alt"] = phone_his_df.shape[0]

    def _if_cert_no(self, credit_base_df, credit_person_df):
        # 1.从ccs.cus_indiv中选择所有cus_name=name的cert_code;
        # 2.若certificate_no=1中任意结果,则if_cert_no=0,否则=1
        if credit_base_df.empty:
            return
        id_no = credit_base_df.iloc[0].certificate_no
        result = 0 if (pd.notna(id_no) and id_no == self.id_card_no) else 1
        self.variables["if_cert_no"] = result

    def _if_marriage(self, credit_base_df, credit_person_df):
        # 1.从pcredit_person_info中选取report_id=report_id的marriage_status;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的marital_status;
        # 3.ccs信贷系统提供的婚姻状态是否为UNMARRIED“未婚”且征信婚姻状态为“已婚”或“离异”，则返回1 ，否则返回0 。
        if credit_person_df.empty:
            return

        param_marry_state = self.cached_data["basicMarryState"]
        if not param_marry_state:
            return
        credit_marry_status = credit_person_df["marriage_status"].values[0]
        if param_marry_state == 'UNMARRIED' and credit_marry_status in (['2', '3']):
            result = 1
        else:
            result = 0
        self.variables["if_marriage"] = result

    def _if_postal_addr(self, credit_base_df, credit_person_df):
        # 1.从pcredit_person_info中选取report_id=report_id的communication_address;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的post_addr;
        # 3.若1中结果=2中结果,则变量=0,否则=1
        person_info_df = self.cached_data["pcredit_person_info"]
        if person_info_df.empty:
            return

        postal_address = self.cached_data["basicPostalAddress"]
        if not postal_address:
            return

        result = 0 if postal_address.strip() in list(person_info_df["communication_address"]) else 1
        self.variables["if_postal_addr"] = result

    def _if_residence_addr(self, credit_base_df, credit_person_df):
        # 1.从pcredit_person_info中选取report_id=report_id的residence_address;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的indiv_houh_reg_addr;
        # 3.若1中结果=2中结果,则变量=0,否则=1
        house_address = self.cached_data["basicHouseAddress"]
        if not house_address:
            return
        person_info_df = self.cached_data["pcredit_person_info"]
        if person_info_df.empty:
            return
        result = 0 if house_address.strip() in list(person_info_df["residence_address"]) else 1
        self.variables["if_residence_addr"] = result

    def _if_live_addr(self, credit_base_df, credit_person_df):
        # 1.从pcredit_live中选取report_id=report_id且no=1的live_address;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的indiv_rsd_addr;
        # 3.若1中结果=2中结果,则变量=0,否则=1
        live_address = self.cached_data["basicLiveAddress"]
        if not live_address:
            return
        credit_live_df = self.cached_data["pcredit_live"]
        if credit_live_df.empty:
            return

        result = 0 if live_address.strip() in list(credit_live_df["live_address"]) else 1
        self.variables["if_live_addr"] = result

    def _if_employee(self, credit_base_df, credit_person_df):
        # 1.从pcredit_profession中选取report_id=report_id且no=1的duty;
        # 2.若1中结果=3或9,则变量=1,否则=0
        credit_profession_df = self.cached_data["pcredit_profession"]
        if credit_profession_df.empty:
            return
        duty = credit_profession_df.loc[:, 'duty'].to_list()
        duty = [x for x in duty if x != '--' and pd.notnull(x)]
        # df = credit_profession_df.query('duty in ["3"]')

        self.variables["if_employee"] = 1 if len(duty) != 0 and duty[0] == '3' else 0

    def _if_official(self, credit_base_df, credit_person_df):
        # 1.从pcredit_profession中选取report_id=report_id且no=1的work_type;
        # 2.若1中结果=10,则变量=1,否则=0
        credit_profession_df = self.cached_data["pcredit_profession"]
        if credit_profession_df.empty:
            return
        df = credit_profession_df.query('no == 1 and work_type == "10"')
        self.variables["if_official"] = 1 if not df.empty else 0

    def _if_spouse_name(self, credit_base_df, credit_person_df):
        # 1.从pcredit_person_info中选取report_id=report_id的spouse_name;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的cus_id;
        # 3.从ccs.cus_imp_rel中选取cus_id=2中cus_id且indiv_family_flag=01的indiv_rel_cus_name;
        # 4.若1中结果=3中结果,则变量=1,否则=0
        spouse_name = self.cached_data["spouseName"]
        if not spouse_name:
            return

        if credit_person_df.empty:
            return
        result = 0 if spouse_name in list(credit_person_df["spouse_name"]) else 1
        self.variables["if_spouse_name"] = result

    def _if_spouse_cert_no(self, credit_base_df, credit_person_df):
        # 1.从pcredit_person_info中选取report_id=report_id的spouse_certificate_no;
        # 2.从ccs.cus_indiv中选取cus_name=name且cert_code=certificate_no的cus_id;
        # 3.从ccs.cus_imp_rel中选取cus_id=2中cus_id且indiv_family_flag=01的indiv_rl_cert_code;
        # 4.若1中结果=3中结果,则变量=1,否则=0
        spouse_id_no = self.cached_data["spouseIdNo"]
        if not spouse_id_no:
            return

        if credit_person_df.empty:
            return
        result = 0 if spouse_id_no in list(credit_person_df["spouse_certificate_no"]) else 1
        self.variables["if_spouse_cert_no"] = result
