import copy

from view.TransFlow import TransFlow
import pandas as pd
from pandas.tseries.offsets import *


class JsonUnionUnusualTrans(TransFlow):

    def process(self):
        self.read_u_unusual_in_u_flow()

    def read_u_unusual_in_u_flow(self):
        flow_df = self.trans_u_flow_portrait[['trans_date', 'trans_time', 'bank', 'account_no', 'opponent_name',
                                              'trans_amt', 'trans_use', 'remark', 'unusual_trans_type',
                                              'usual_trans_type', 'trans_flow_src_type']]
        # 异常交易
        unusual_bank_flow = {
            "gaming": {'form_detail': [], 'risk_tips': ''},
            "amusement": {'form_detail': [], 'risk_tips': ''},
            "case_disputes": {'form_detail': [], 'risk_tips': ''},
            "security_fines": {'form_detail': [], 'risk_tips': ''},
            "insurance_claims": {'form_detail': [], 'risk_tips': ''},
            "stock": {'form_detail': [], 'risk_tips': ''},
            "hospital": {'form_detail': [], 'risk_tips': ''},
            "loan": {'form_detail': [], 'risk_tips': ''},
            "foreign_guarantee": {'form_detail': [], 'risk_tips': ''},
            "noritomo": {'form_detail': [], 'risk_tips': ''},
            "reward": {'form_detail': [], 'risk_tips': ''},
            "coupon_clipper": {'form_detail': [], 'risk_tips': ''},
            "daily_consummption": {'form_detail': [], 'risk_tips': ''},
        }
        unusual_wxzfb_flow = copy.deepcopy(unusual_bank_flow)

        # 中性交易
        usual_bank_flow = {
            "fast_in_out": {"form_detail": [], "risk_tips": ''},
            "big_in_out": {"form_detail": [], "risk_tips": ''},
            "family_unstable": {"form_detail": [], "risk_tips": ''},
            "financing": {"form_detail": [], "risk_tips": ''},
            "house_sale": {"form_detail": [], "risk_tips": ''},
            "same_in_out": {"form_detail": [], "risk_tips": ''},
            "large_income": {"form_detail": [], "risk_tips": ''},
            "large_expense": {"form_detail": [], "risk_tips": ''},
        }
        usual_wxzfb_flow = copy.deepcopy(usual_bank_flow)
        overview_abnormal_tips = ''

        if flow_df.shape[0] > 0:
            # 获取一年前的日期
            year_ago = pd.to_datetime(flow_df['trans_time']).max() - DateOffset(months=12)
            # 筛选近一年有异常交易数据
            df = flow_df.loc[(pd.to_datetime(flow_df.trans_time) >= year_ago) &
                             (pd.notna(flow_df.unusual_trans_type))]
            if df.shape[0] > 0:
                df['trans_time'] = df['trans_time'].astype(str)
                df['trans_amt'] = df.trans_amt.apply(lambda x: '%.2f' % x)
                df['remark'] = df[['remark', 'trans_use']].fillna("").apply(
                    lambda x: ",".join([x['remark'], x['trans_use']])
                    if len(x['trans_use']) > 0 and len(x['remark']) > 0
                    else "".join([x['remark'], x['trans_use']]), axis=1)
                df.drop(columns=['trans_use', 'trans_date'], inplace=True)
                # 区分银行,微信并去除trans_flow_src_type列
                bank_df = df[df.trans_flow_src_type == 0].iloc[:, :-1]
                wxzfb_df = df[df.trans_flow_src_type == 1].iloc[:, :-1]
                if bank_df.shape[0] > 0:
                    unusual_bank_flow, overview_abnormal_tips = \
                        self.get_unusual_trans_risk(self, bank_df, unusual_bank_flow, overview_abnormal_tips, '银行：')

                if wxzfb_df.shape[0] > 0:
                    unusual_wxzfb_flow, overview_abnormal_tips = \
                        self.get_unusual_trans_risk(self, wxzfb_df, unusual_wxzfb_flow, overview_abnormal_tips, '微信支付宝：')

            # 筛选近一年中性交易数据
            usual_trans_df = flow_df.loc[
                (pd.to_datetime(flow_df.trans_date) >= year_ago) &
                (pd.notna(flow_df.usual_trans_type)) & (flow_df.usual_trans_type != '')]
            if usual_trans_df.shape[0] > 0:
                usual_trans_df['trans_time'] = usual_trans_df['trans_time'].astype(str)
                usual_trans_df['trans_amt'] = usual_trans_df.trans_amt.apply(lambda x: '%.2f' % x)
                usual_trans_df['remark'] = usual_trans_df[['remark', 'trans_use']].fillna("").apply(
                    lambda x: ",".join([x['remark'], x['trans_use']])
                    if len(x['trans_use']) > 0 and len(x['remark']) > 0
                    else "".join([x['remark'], x['trans_use']]), axis=1)
                usual_trans_df.drop(columns=['trans_use', 'trans_date'], inplace=True)
                bank_usual_df = usual_trans_df[usual_trans_df['trans_flow_src_type'] == 0].iloc[:, :-1]
                wxzfb_usual_df = usual_trans_df[usual_trans_df['trans_flow_src_type'] == 1].iloc[:, :-1]
                if bank_usual_df.shape[0] > 0:
                    usual_bank_flow = self.get_usual_trans_risk(bank_usual_df, usual_bank_flow)

                if wxzfb_usual_df.shape[0] > 0:
                    usual_wxzfb_flow = self.get_usual_trans_risk(wxzfb_usual_df, usual_wxzfb_flow)

        self.variables['abnormal_trans_risk'] = {
            "bank_flow": unusual_bank_flow,
            "wxzfb_flow": unusual_wxzfb_flow
        }

        self.variables['normal_trans_detail'] = {
            "bank_flow": usual_bank_flow,
            "wxzfb_flow": usual_wxzfb_flow
        }
        self.variables['trans_report_overview']['abnormal_trans_risk']['risk_tips'] = overview_abnormal_tips

    @staticmethod
    def get_unusual_trans_risk(self, df, flow, overview_abnormal_tips, tipes_type):
        # 博彩-投机风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('博彩')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            temp_df['temp_time'] = pd.to_datetime(temp_df.trans_time)
            temp_df['month'] = temp_df.temp_time.dt.month
            # 按月汇总交易金额
            temp_month_df = temp_df.loc[temp_df.trans_amt < 0].groupby('month').agg({'trans_amt': 'sum'})
            # 月交易金额大于1万次数
            cnt = temp_month_df.loc[temp_month_df.trans_amt < -10000].shape[0]
            trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
            total_cnt = temp_df.loc[temp_df.trans_amt < 0].shape[0]
            month_cnt = temp_df.month.unique().tolist()
            temp_df.drop(['temp_time', 'month'], axis=1, inplace=True)
            if trans_amt >= 1:
                flow['gaming']['form_detail'] = temp_df.to_dict('records')
            if (temp_df.shape[0] > 8) or (len(month_cnt) > 5) or \
                    (temp_month_df.shape[0] > 0 and (cnt / temp_month_df.shape[0]) > (2 / 3)):
                flow['gaming']['risk_tips'] = f"博彩购买总金额{trans_amt:.2f}万，购买总次数{total_cnt}次，博彩交易频次较高，警示申请人投机风险"
                overview_abnormal_tips += tipes_type + flow['gaming']['risk_tips'] + ';'

        # 娱乐-不良嗜好风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('娱乐')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            temp_df['temp_time'] = pd.to_datetime(temp_df['trans_time'])
            # 专家经验
            # 晚上9点-凌晨4点，均算夜间
            # 20220913修改：夜间定义为00:01-04:00
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
            if trans_amt >= 1:
                flow['amusement']['form_detail'] = temp_df.drop('temp_time', axis=1).to_dict('records')
            if temp_df.shape[0] > 8 and trans_amt > 5:
                flow['amusement']['risk_tips'] = f"娱乐场所消费金额{trans_amt:.2f}万，娱乐场所的消费金额较大，警示申请人不良嗜好风险"
            # 筛选含交易时间数据
            temp_df = temp_df.loc[~temp_df.temp_time.astype(str).str.contains('00:00:00')]
            # 筛选夜间
            temp_df = temp_df.loc[(temp_df.temp_time.dt.hour < 4) & (temp_df.temp_time.dt.hour > 0)]
            trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
            temp_df.drop('temp_time', axis=1, inplace=True)
            if temp_df.shape[0] > 8 and trans_amt > 5:
                # 若同时命中两条，只展示本条
                flow['amusement']['risk_tips'] = f"娱乐场所的夜间消费金额{trans_amt:.2f}万，在娱乐场所的夜间消费金额较大，警示申请人不良嗜好导致的家庭稳定性风险"
            if flow['amusement']['risk_tips'] != "":
                overview_abnormal_tips += tipes_type + flow['amusement']['risk_tips'] + ';'

        # 案件纠纷-履约风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('案件纠纷')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
            # 案件纠纷展示与黄色预警同步，即命中则展示
            flow['case_disputes']['form_detail'] = temp_df.to_dict('records')
            if trans_amt > 20:
                flow['case_disputes']['risk_tips'] = f"作为被告，案件纠纷总支出金额{trans_amt:.2f}万，涉案金额较大，警示申请人履约风险"
                overview_abnormal_tips += tipes_type + flow['case_disputes']['risk_tips'] + ';'

        # 治安罚款-治安管理风险
        # 表单
        # 20220913治安罚款修改为超过1000才展示
        temp_df = df.loc[df.unusual_trans_type.str.contains('治安罚款')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt_sum = temp_df.trans_amt.abs().sum()
            if trans_amt_sum > 1000:
                flow['security_fines']['form_detail'] = temp_df.to_dict('records')
                # 专家经验
                trans_amt = trans_amt_sum / 10000
                flow['security_fines']['risk_tips'] = f"有治安罚款记录，罚款总金额{trans_amt:.2f}万元，预警申请人治安管理风险"
                overview_abnormal_tips += tipes_type + flow['security_fines']['risk_tips'] + ';'

        # 保险理赔-理赔风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('保险理赔')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            expense_amt = temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].abs().sum() / 10000
            income_amt = temp_df.loc[temp_df.trans_amt > 0]['trans_amt'].abs().sum() / 10000
            # 保险理赔展示与黄色预警同步，黄色预警阈值为大于3即命中，命中则展示
            if temp_df.shape[0] > 3:
                flow['insurance_claims']['form_detail'] = temp_df.to_dict('records')
                if income_amt > 20:
                    flow['insurance_claims'][
                        'risk_tips'] = f"总进账理赔金额{income_amt:.2f}万，有大额理赔进账记录，关注大额理赔事件对申请人造成损失的风险;"
                if expense_amt > 10:
                    flow['insurance_claims'][
                        'risk_tips'] += f"总出账理赔金额{expense_amt:.2f}万，有大额理赔出账记录，关注大额理赔影响申请人现金流的风险，进而影响申请人经营的风险;"
                if flow['insurance_claims']['risk_tips'] != "":
                    overview_abnormal_tips += tipes_type + flow['insurance_claims']['risk_tips'] + ';'

        # 股票期货-投资风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('股票期货')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.loc[temp_df.trans_amt < 0]['trans_amt'].sum()) / 10000
            if trans_amt > 0:
                self.variables['suggestion_and_guide']['trans_general_info']['bank_trans_type'][
                    'risk_tips'] += "客户有一定的投资理财意识，可适当进行行内理财基金等产品的营销;"
            if trans_amt >= 20:
                flow['stock']['form_detail'] = temp_df.to_dict('records')
            if trans_amt > 100:
                flow['stock']['risk_tips'] = f"股票期货出账金额{trans_amt:.2f}万，有股票期货交易记录，警示申请人投资风险"
                overview_abnormal_tips += tipes_type + flow['stock']['risk_tips'] + ';'

        # 医疗-健康风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('医院')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.trans_amt.sum()) / 10000
            if trans_amt >= 1:
                flow['hospital']['form_detail'] = temp_df.to_dict('records')
            if trans_amt > 3:
                flow['hospital'][
                    'risk_tips'] = f"医疗消费总金额{trans_amt:.2f}万，有大额医院消费记录，关注申请人或家人健康风险"
                overview_abnormal_tips += tipes_type + flow['hospital']['risk_tips'] + ';'

        # 典当-隐形负债风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('典当')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['noritomo']['form_detail'] = temp_df.to_dict('records')
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.trans_amt.sum()) / 10000
            flow['noritomo'][
                'risk_tips'] = f"有典当机构交易记录，与典当机构交易总金额{trans_amt:.2f}万，警示申请人资金紧张的风险"
            overview_abnormal_tips += tipes_type + flow['noritomo']['risk_tips'] + ';'

        # 银行独有贷款异常,对外担保异常模块
        # 贷款异常-逾期风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('贷款异常')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['loan']['form_detail'] = temp_df.to_dict('records')
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.trans_amt.sum()) / 10000
            flow['loan']['risk_tips'] = f"有担保公司代偿记录，代偿总金额{trans_amt:.2f}万，警示申请人贷款逾期风险"
            overview_abnormal_tips += tipes_type + flow['loan']['risk_tips'] + ';'
        # 对外担保异常-代偿风险
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('对外担保异常')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['foreign_guarantee']['form_detail'] = temp_df.to_dict('records')
            # 专家经验
            temp_df['trans_amt'] = temp_df['trans_amt'].astype(float)
            trans_amt = abs(temp_df.trans_amt.sum()) / 10000
            flow['foreign_guarantee'][
                'risk_tips'] = f"有替他人代偿记录，代偿总金额{trans_amt:.2f}万，警示代偿影响申请人现金流的风险"
            overview_abnormal_tips += tipes_type + flow['foreign_guarantee']['risk_tips']

        # 微信支付宝流水独有直播打赏,薅羊毛,生活类模块
        # 直播打赏
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('直播打赏')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['reward']['form_detail'] = temp_df.to_dict('records')

        # 薅羊毛
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('薅羊毛')]
        if temp_df.shape[0] > 100:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['coupon_clipper']['form_detail'] = temp_df.to_dict('records')

        # 生活类
        # 表单
        temp_df = df.loc[df.unusual_trans_type.str.contains('生活类')]
        if temp_df.shape[0] > 0:
            temp_df.drop('unusual_trans_type', inplace=True, axis=1)
            flow['daily_consummption']['form_detail'] = temp_df.to_dict('records')
            # 专家经验

        return flow, overview_abnormal_tips

    @staticmethod
    def get_usual_trans_risk(usual_trans_df, usual_flow):
        usual_trans_list = ['快进快出', '整进整出', '家庭不稳定', '理财行为', '房产买卖', '同进同出',
                            '大额进账', '大额出账']
        usual_transform_list = ["fast_in_out", "big_in_out", "family_unstable", "financing", "house_sale",
                                "same_in_out", "large_income", "large_expense"]
        for ind, usual in enumerate(usual_trans_list):
            temp_df = usual_trans_df.loc[usual_trans_df.usual_trans_type.str.contains(usual)]
            if temp_df.shape[0] > 0:
                # 表单
                temp_df.drop(['usual_trans_type', 'unusual_trans_type'], inplace=True, axis=1)
                usual_flow[usual_transform_list[ind]]['form_detail'] = temp_df.to_dict('records')
                # 专家经验
        return usual_flow
