import re
from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
from view.p08004_v.trans_report_util import convert_relationship
from pandas.tseries import offsets
import datetime
import pandas as pd

RELATED_LIST = ['U_PERSONAL',
                'U_PER_LG_COMPANY',
                'U_PER_SH_H_COMPANY',
                'U_PER_SH_M_COMPANY',
                'U_PER_SH_L_COMPANY',
                'U_PER_SP_PERSONAL',
                'U_PER_SP_LG_COMPANY',
                'U_PER_SP_SH_H_COMPANY',
                'U_PER_SP_SH_M_COMPANY',
                'U_PER_SP_SH_L_COMPANY',
                'U_PER_CT_COMPANY',
                'U_PER_SP_CT_COMPANY',
                'U_PER_PARENTS_PERSONAL',
                'U_PER_CHILDREN_PERSONAL',
                'U_PER_PARTNER_PERSONAL',
                'U_PER_REL_PERSONAL',
                'U_PER_REL_NA_PERSONAL',
                'U_PER_REL_COMPANY',
                'U_PER_GUARANTOR_PERSONAL',
                'U_PER_GUARANTOR_COMPANY',
                'U_PER_CHAIRMAN_COMPANY',
                'U_PER_SUPERVISOR_COMPANY',
                'U_PER_OTHER',
                'U_PER_COMPANY_SH_PERSONAL',
                'U_PER_COMPANY_FIN_PERSONAL',
                'U_COMPANY',
                'U_COMPANY_SH_PERSONAL',
                'U_COM_LEGAL_PERSONAL',
                'U_COMPANY_SH_COMPANY',
                'U_COM_CT_PERSONAL',
                'U_COM_CT_CT_COMPANY',
                'U_COM_CT_LG_COMPANY',
                'U_COM_CT_SH_H_COMPANY',
                'U_COM_CT_SH_M_COMPANY',
                'U_COM_CT_SH_L_COMPANY',
                'U_COM_CT_SP_PERSONAL',
                'U_COM_REL_PERSONAL',
                'U_COM_REL_NA_PERSONAL',
                'U_COM_REL_COMPANY',
                'U_COM_GUARANTOR_PERSONAL',
                'U_COM_GUARANTOR_COMPANY',
                'U_COM_CT_SP_CT_COMPANY',
                'U_COM_CT_SP_LG_COMPANY',
                'U_COM_CT_SP_SH_H_COMPANY',
                'U_COM_CT_SP_SH_M_COMPANY',
                'U_COM_CT_SP_SH_L_COMPANY',
                'U_COM_OTHER',
                'U_COM_FIN_PERSONAL']
RELATED_LIST_SIMPLE = [
    'U_PERSONAL',
    'U_PER_REL_PERSONAL',
    'U_PER_REL_NA_PERSONAL',
    'U_PER_REL_COMPANY',
    'U_PER_SP_PERSONAL',
    'U_PER_GUARANTOR_PERSONAL',
    'U_PER_GUARANTOR_COMPANY',
    'U_PER_OTHER',
    'U_COMPANY',
    'U_COM_REL_PERSONAL',
    'U_COM_REL_NA_PERSONAL',
    'U_COM_REL_COMPANY',
    'U_COM_GUARANTOR_PERSONAL',
    'U_COM_GUARANTOR_COMPANY',
    'U_COM_OTHER']
RELATION_RANK = dict(zip(RELATED_LIST, range(1, len(RELATED_LIST) + 1)))
RELATION_RANK_SIMPLE = dict(zip(RELATED_LIST_SIMPLE, range(1, len(RELATED_LIST_SIMPLE) + 1)))


class JsonMtrTitle(TransFlow):

    def process(self):
        self.create_u_title()

    def create_u_title(self):

        sql1 = """
            SELECT ap.related_name AS relatedName, acc.id as account_id, ap.id_type,
            ap.relationship AS relation,
            ac.bank AS bankName,ac.account_no AS bankAccount,
            acc.start_time, acc.end_time, acc.trans_flow_src_type, ap.id_card_no, acc.file_id
            FROM trans_apply ap
            left join trans_account ac
            on ap.account_id = ac.id
            left join trans_account acc
            on ac.account_no = acc.account_no and ac.bank = acc.bank and ac.id_card_no = acc.id_card_no
            where ap.report_req_no =  %(report_req_no)s
        """

        account_df = sql_to_df(sql=sql1,
                               params={"report_req_no": self.reqno})
        account_df['trans_flow_src_type'] = account_df['trans_flow_src_type'].fillna(1)
        # 20230308非实名版仅保留选择的文件
        product_code = self.origin_data['strategyInputVariables']['product_code']
        # 20240620 流水3.0 不再区分product_code，统一筛选file_id
        if self.file_ids is not None and len(self.file_ids) > 0:
            account_df = account_df[account_df['file_id'].isin(self.file_ids)]
        # 仅展示近一年的流水数据
        year_ago = pd.to_datetime((account_df['end_time'].max() - offsets.DateOffset(months=12)).date())
        account_df.loc[(account_df['start_time'] < year_ago) & (account_df['end_time'] < year_ago) &
                       pd.notna(account_df['account_id']), ['start_time', 'end_time', 'account_id']] = None
        account_df = account_df[(account_df['start_time'] >= year_ago) | (account_df['end_time'] >= year_ago) |
                                pd.isna((account_df['account_id']))]
        account_df.loc[account_df['start_time'] < year_ago, 'start_time'] = year_ago
        # 关联人信息
        relation_df = account_df.drop_duplicates(['relatedName', 'relation'])
        relation_df.rename({'relatedName': 'name'}, axis=1, inplace=True)
        # 流水基本信息
        account_df = account_df[pd.notna(account_df.account_id)]
        unique_acc = \
            account_df.drop_duplicates(subset=['relatedName', 'relation', 'bankName', 'bankAccount', 'id_card_no']
                                       )[['relatedName', 'relation', 'bankName', 'bankAccount', 'id_card_no']]
        if unique_acc.shape[0] == 0:
            unique_acc['startEndDate'] = None
        # 每个类别账户数
        each_acc_cnt = [0, 0, 0]
        start_end_date_list = []
        for row in unique_acc.itertuples():
            ind = getattr(row, 'Index')
            rel_name = getattr(row, 'relatedName')
            rel = getattr(row, 'relation')
            bank_name = getattr(row, 'bankName')
            bank_acc = getattr(row, 'bankAccount')
            temp_df = account_df[(account_df.relatedName == rel_name) & (account_df.relation == rel) &
                                 (account_df.bankName == bank_name) & (account_df.bankAccount == bank_acc)]
            temp_df.sort_values(by=['start_time', 'end_time'], inplace=True, ascending=True)
            temp_df['start_time'] = temp_df['start_time'].apply(lambda x: x.date())
            temp_df['end_time'] = temp_df['end_time'].apply(lambda x: x.date())
            temp_start = pd.to_datetime(temp_df['start_time']).tolist()
            temp_end = pd.to_datetime(temp_df['end_time']).tolist()
            start_end_date_list.append(self.union_date(temp_start, temp_end))
            unique_src_type = temp_df['trans_flow_src_type'].unique().tolist()
            if 1 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（银行流水）'
                each_acc_cnt[0] += 1
            elif 2 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（支付宝流水）'
                each_acc_cnt[1] += 1
            elif 3 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（微信流水）'
                each_acc_cnt[2] += 1
            elif 5 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（客如云/微风企）'
            elif 6 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（新疆收单）'
            elif 7 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（聚合支付）'
            elif 8 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（美团核销券）'
            elif 9 in unique_src_type:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（美团扫码枪）'
                each_acc_cnt[1] += 1
            else:
                unique_acc.loc[ind, 'bankName_src'] = f'{bank_name}（其他）'
                each_acc_cnt[3] += 1
        unique_acc['startEndDate'] = start_end_date_list
        account_df = unique_acc
        # 根据产品编号判断使用哪一种关联关系列表
        product_code = self.origin_data['strategyInputVariables']['product_code']
        # 20240620 流水3.0，均考虑任务制的关联关系
        relation_rank = RELATION_RANK_SIMPLE
        account_df['rank'] = account_df.relation.apply(lambda x: relation_rank[x])
        account_df.sort_values(by='rank', axis=0, inplace=True)
        account_df.drop('rank', axis=1, inplace=True)
        account_df['relation'] = account_df['relation'].apply(lambda x: convert_relationship(x, product_code))

        cashier = pd.DataFrame(data=None,
                               columns=['name', 'bank', 'account'])
        file_df = pd.DataFrame()
        for account in self.cached_data.get('input_param'):
            if str(account).__contains__('\'ifCashier\': \'是\''):
                cashier.loc[len(cashier)] = [account.get('name'),
                                             account.get('extraParam').get('accounts')[0]['bankName'],
                                             account.get('extraParam').get('accounts')[0]['bankAccount']]
            temp_name = account.get('name')
            temp_idno = account.get('idno')
            temp_file_df = pd.DataFrame(account.get('extraParam').get('fileInfo'))
            if temp_file_df.shape[0] > 0:
                temp_file_df['ownerName'] = temp_file_df['ownerName'].apply(
                    lambda x: "未取得" if pd.isna(x) or x == "" else f"{x}(一致)" if x == temp_name else f"{x}(不一致)")
                temp_file_df = temp_file_df.groupby(by=['bankName', 'bankAccount'], as_index=False).agg({
                    'fileName': pd.Series.tolist, 'contentId': pd.Series.tolist,
                    'uploadDate': pd.Series.tolist, 'ownerName': pd.Series.tolist})
                temp_file_df.rename({"ownerName": "userName"}, axis=1, inplace=True)
                # temp_file_df['relatedName'] = temp_name
                temp_file_df['id_card_no'] = temp_idno
                file_df = pd.concat([file_df, temp_file_df], axis=0, ignore_index=True)

        if not cashier.empty:
            cashier['account_detail'] = '(出纳)'
            account_df = pd.merge(account_df, cashier,
                                  how='left',
                                  left_on=['relatedName', 'bankName', 'bankAccount'],
                                  right_on=['name', 'bank', 'account']).fillna("")
            account_df['bankAccount'] = account_df['bankAccount'] + account_df['account_detail']
        # 删除强关联关系中担保人展示
        # 不删除担保人的展示
        # drop_list = account_df[account_df.relation == '担保人'].index.tolist()
        # account_df.drop(drop_list, 0, inplace=True)
        # account_df.reset_index(drop=True, inplace=True)
        if file_df.shape[0] == 0:
            account_df['fileName'] = [[] for _ in range(account_df.shape[0])]
            account_df['userName'] = [[] for _ in range(account_df.shape[0])]
            account_df['contentId'] = [[] for _ in range(account_df.shape[0])]
            account_df['uploadDate'] = [[] for _ in range(account_df.shape[0])]
        else:
            file_df.drop_duplicates(subset=['bankName', 'bankAccount', 'id_card_no'], inplace=True)
            account_df = pd.merge(account_df, file_df, how='left', on=['id_card_no', 'bankName', 'bankAccount'])
            account_df.drop(columns='bankName', inplace=True)
            account_df = account_df.rename(columns={'bankName_src': 'bankName'})
        account_df.reset_index(drop=True, inplace=True)
        account_df['bankAccount'] = account_df['bankAccount'].apply(lambda x: str(x).strip())
        for col in ['bankAccount', 'relatedName', 'bankName']:
            account_df[col] = account_df[col].apply(lambda x: str(x).strip())
        account_list = account_df[['relatedName', 'relation', 'bankName', 'bankAccount', 'startEndDate',
                                   'fileName', 'userName', 'contentId', 'uploadDate']].to_dict(orient='records')

        relation_df = pd.DataFrame(self.cached_data.get('input_param'))[['name', 'baseTypeDetail', 'idno', 'userType']]

        # 20220914新增，同一关联对象多个关联关系并列展示
        relation_df['rank'] = relation_df.baseTypeDetail.apply(lambda x: relation_rank[x])
        relation_df['relation'] = relation_df['baseTypeDetail'].apply(lambda x: convert_relationship(x, product_code))
        temp_df = relation_df.groupby(by='idno', as_index=False).agg({
            'rank': 'min', 'relation': lambda x: '，'.join(x)})
        temp_df.sort_values(by='rank', axis=0, inplace=True)
        relation_df.drop(['rank', 'relation'], axis=1, inplace=True)
        relation_df = relation_df.drop_duplicates(subset=['idno'], keep='last')
        relation_df = pd.merge(temp_df, relation_df, how='left', on='idno')
        now = datetime.datetime.now()
        detail_list = []
        for row in relation_df.itertuples():
            ind = getattr(row, 'Index')
            rel_name = getattr(row, 'name')
            rel_code = getattr(row, 'idno')
            id_type = getattr(row, 'userType')
            if id_type == 'PERSONAL':
                relation_df.loc[ind, 'cusType'] = 'PERSON'
                if pd.isna(rel_code) or not self.is_valid_idcard(rel_code):
                    detail_list.append({})
                    continue
                try:
                    age = now.year - int(rel_code[6:10])
                    if pd.to_datetime(rel_code[6: 14]) + offsets.DateOffset(years=age) > pd.to_datetime(now.date()):
                        age -= 1
                    detail_list.append({'basic_name': rel_name, 'basic_age': age,
                                        'basic_sex': '男' if int(rel_code[-2]) % 2 == 1 else '女',
                                        'basic_indiv_brt_place': rel_code[:6], 'basic_id': rel_code})
                except:
                    detail_list.append({})
            else:
                relation_df.loc[ind, 'cusType'] = 'COMPANY'
                if pd.isna(rel_code) or rel_code == '':
                    detail_list.append({})
                    continue
                detail_list.append(self.company_detail(rel_name, rel_code))
        relation_df['detail'] = detail_list if product_code == '08004' else None
        relation_df = relation_df[['name', 'relation', 'cusType', 'detail']]
        # json_str = "{\"cusName\":\"" + self.cusName \
        #            + "\",\"appAmt\":" + str(self.appAmt) \
        #            + ",\"流水信息\":" + account_list \
        #            + ",\"关联人\":" + relation_df.to_json(orient='records') \
        #            + "}"
        #
        # self.variables["表头"] = json.loads(json_str)
        self.variables['report_title'] = {
            'cusName': self.cusName,
            'appAmt': self.appAmt,
            '流水信息': account_list,
            '关联人': relation_df.to_dict(orient='records')
        }
        overview_sub_tips = "客户强关联关系下识别到"
        for i, acc_cnt in enumerate(each_acc_cnt):
            if acc_cnt == 0:
                continue
            overview_sub_tips += f"{acc_cnt}个"
            overview_sub_tips += "银行账户流水，" if i == 0 else "支付宝账户流水，" if i == 1 else "微信账户流水，"

    @staticmethod
    def union_date(start, end):
        res = []
        for i, v in enumerate(start):
            if i == 0:
                res.append([v, end[i]])
            else:
                if v <= res[-1][-1]:
                    if end[i] > res[-1][-1]:
                        res[-1][-1] = end[i]
                else:
                    res.append([v, end[i]])
        res = [f"{format(x[0], '%Y/%m/%d')}—{format(x[-1], '%Y/%m/%d')}" for x in res]
        return res

    @staticmethod
    def company_detail(name, idno):
        sql = "select * from %s where basic_id = (SELECT id FROM info_com_bus_basic WHERE ent_name='%s'"
        if pd.notna(idno):
            sql += " and credit_code = '%s'"
        sql += " and unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id desc limit 1)"
        face_df = sql_to_df(sql=sql % ("info_com_bus_face", name, idno))
        detail = {}
        if face_df.shape[0] == 0:
            return detail
        share_df = sql_to_df(sql=sql % ("info_com_bus_shareholder", name, idno))
        shareholder_str = ''
        if share_df.shape[0] > 0:
            shareholder_str = '，'.join(
                share_df[(pd.notna(share_df['share_holder_name'])) &
                         (pd.notna(share_df['funded_ratio']))
                         ].apply(lambda x: f"{str(x['share_holder_name'])}（{x['funded_ratio']:.0%}）", axis=1))
        detail['basic_name'] = name
        detail['basic_fr_name'] = face_df.loc[0, 'fr_name']
        detail['basic_es_date'] = "" if pd.isna(face_df.loc[0, 'es_date']) else \
            format(face_df.loc[0, 'es_date'], '%Y-%m-%d')
        detail['basic_appr_date'] = "" if pd.isna(face_df.loc[0, 'appr_date']) else \
            format(face_df.loc[0, 'appr_date'], '%Y-%m-%d')
        detail['basic_industry_phyname'] = face_df.loc[0, 'industry_phyname']
        detail['basic_address'] = face_df.loc[0, 'address']
        detail['basic_opera_range'] = face_df.loc[0, 'operate_scope']
        detail['basic_shareholder'] = shareholder_str
        detail['basic_ent_type'] = face_df.loc[0, 'ent_type']
        detail['basic_credit_code'] = idno
        detail['basic_reg_cap'] = face_df.loc[0, 'reg_cap']
        detail['basic_ent_status'] = face_df.loc[0, 'ent_status']
        open_from = "*" if pd.isna(face_df.loc[0, 'open_from']) else format(face_df.loc[0, 'open_from'], "%Y-%m-%d")
        open_to = "*" if pd.isna(face_df.loc[0, 'open_to']) else format(face_df.loc[0, 'open_to'], "%Y-%m-%d")
        detail['basic_open_date_range'] = open_from + "至" + open_to
        return detail

    @staticmethod
    def is_valid_idcard(idcard):
        # 身份证校验
        if isinstance(idcard, int):
            idcard = str(idcard)

        IDCARD_REGEX = '[1-9][0-9]{14}([0-9]{2}[0-9X])?'
        if not re.match(IDCARD_REGEX, idcard):
            return False

        factors = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        items = [int(item) for item in idcard[:-1]]
        copulas = sum([a * b for a, b in zip(factors, items)])
        ckcodes = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']

        return ckcodes[copulas % 11].upper() == idcard[-1].upper()
