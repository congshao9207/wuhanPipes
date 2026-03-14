import datetime
import json
import pandas as pd
import scipy
from scipy.stats import ttest_rel

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df

logger = LoggerUtil().logger(__name__)


class T31001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'oth_loan_all_12m': 0,  # 12个月内非本机构查询次数
            'oth_loan_all_6m': 0,  # 6个月内非本机构查询次数
            'oth_loan_p2p_6m': 0,  # 6个月内P2P查询次数
            'oth_loan_nonbank_6m': 0,  # 6个月内非银机构查询次数
            'oth_loan_reason_else_6m': 0,  # 6个月内非贷前审查查询次数
            'oth_loan_reason_else_1_3m': 0,  # 第2,3个月非贷前审查查询次数
            'oth_loan_month_avg_24m': 0,  # 24个月内月均查询次数
            'oth_loan_month_ttest_12m': 0,  # 12个月内月申请次数偏离程度
            'oth_loan_apply_months_24m': 0,  # 24个月内查询月份数
            'oth_loan_org_cnt_9m': 0,  # 9个月内不同查询机构数
            'oth_loan_inc_6_12m': 0,  # 近6个月相对近12个月新增机构数
            'oth_loan_inc_3_12m': 0,  # 近3个月相对近6个月新增机构数
            'oth_loan_inc_12_24m': 0,  # 近12个月相对近24个月新增机构数
            'oth_loan_nonbank_9m': 0,  # 9个月内非银机构查询次数
            'oth_loan_scale3_12m': 0,  # 12个月内中型机构查询次数
            'oth_loan_org_cnt_12m': 0,  # 12个月内查询不同机构个数
            'oth_loan_month_1sigma_12m': 0,  # 12个月内查询次数偏离平均值的月份数
            'oth_loan_reason_else_6_9m': 0,  # 第7,8个月内非贷前审查查询次数
            'oth_loan_reason_else_3m': 0,  # 近3个月内非贷前审查查询次数
            'oth_loan_model_score': 0  # LR模型分
        }

    # 读取详细信息json串,解析成dataframe并去除本机构查询记录
    def _loan_other_df(self):
        sql = """
            select detail_info_data from info_other_loan_summary where user_name=%(user_name)s and 
            id_card_no=%(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  order by id  desc limit 1
        """
        df = sql_to_df(sql=sql, params={"user_name": self.user_name,
                                        "id_card_no": self.id_card_no})
        if df.empty:
            return pd.DataFrame()
        else:
            detail_data = json.loads(df.iloc[0, 0])
            if len(detail_data) == 0:
                return pd.DataFrame()
            else:
                other_loan_df = pd.DataFrame(columns=['date', 'indu_code', 'org_scale',
                                                      'reason_code', 'org_code'])
                for i in range(len(detail_data)):
                    other_loan_df.loc[i, 'date'] = detail_data[i]['dateUpdated']
                    other_loan_df.loc[i, 'indu_code'] = detail_data[i]['industryCode']
                    other_loan_df.loc[i, 'org_scale'] = detail_data[i]['orgScale']
                    other_loan_df.loc[i, 'reason_code'] = detail_data[i]['reasonCode']
                    other_loan_df.loc[i, 'org_code'] = detail_data[i]['var1']
                other_loan_df['date'] = other_loan_df['date'].fillna('2010-01-01')
                other_loan_df['date'] = pd.to_datetime(other_loan_df['date'])
                other_loan_df.sort_values(by='date', inplace=True)
                now = datetime.datetime.now()
                before_2y = datetime.datetime(now.year - 2, now.month, now.day)
                other_loan_df = other_loan_df[(~other_loan_df['org_code'].str.contains('806460')) &
                                              (other_loan_df['date'] >= before_2y)]
                other_loan_df['month_from_now'] = other_loan_df['date'].apply(lambda x:
                                                                              (
                                                                                          now.year - x.year) * 12 + now.month - x.month +
                                                                              (now.day - x.day) // 100)
        return other_loan_df

    # 基本数据统计
    def _basic_query_data(self, df):
        if df.empty:
            return
        self.variables['oth_loan_all_12m'] = df.loc[df['month_from_now'] <= 11].shape[0]
        self.variables['oth_loan_all_6m'] = df.loc[df['month_from_now'] <= 5].shape[0]
        self.variables['oth_loan_p2p_6m'] = df.loc[(df['month_from_now'] <= 5) &
                                                   (df['indu_code'] == 'P2P')].shape[0]
        self.variables['oth_loan_nonbank_6m'] = df.loc[(df['month_from_now'] <= 5) &
                                                       (df['indu_code'] != 'BAK')].shape[0]
        self.variables['oth_loan_reason_else_6m'] = df.loc[(df['month_from_now'] <= 5) &
                                                           (df['reason_code'] != '01')].shape[0]
        self.variables['oth_loan_reason_else_1_3m'] = df.loc[(df['month_from_now'] >= 1) &
                                                             (df['month_from_now'] <= 2) &
                                                             (df['reason_code'] != '01')].shape[0]
        self.variables['oth_loan_nonbank_9m'] = df.loc[(df['month_from_now'] <= 8) &
                                                       (df['indu_code'] != 'BAK')].shape[0]
        self.variables['oth_loan_scale3_12m'] = df.loc[(df['month_from_now'] <= 11) &
                                                       (df['org_scale'] == '3')].shape[0]
        self.variables['oth_loan_reason_else_6_9m'] = df.loc[(df['month_from_now'] >= 6) &
                                                             (df['month_from_now'] <= 8) &
                                                             (df['reason_code'] != '01')].shape[0]
        self.variables['oth_loan_reason_else_3m'] = df.loc[(df['month_from_now'] <= 2) &
                                                           (df['reason_code'] != '01')].shape[0]
        return

    # 去重机构数据统计
    def _duplicate_org_data(self, df):
        if df.empty:
            return
        self.variables['oth_loan_org_cnt_9m'] = len(set(df.loc[df['month_from_now'] <= 8]['org_code'].to_list()))
        self.variables['oth_loan_org_cnt_12m'] = len(set(df.loc[df['month_from_now'] <= 11]['org_code'].to_list()))
        self.variables['oth_loan_inc_6_12m'] = len(set(df.loc[df['month_from_now'] <= 5]['org_code'].to_list()).
                                                   difference(set(df.loc[(df['month_from_now'] >= 6) &
                                                                         (df['month_from_now'] <= 11)]['org_code'].
                                                                  to_list())))
        self.variables['oth_loan_inc_3_12m'] = len(set(df.loc[df['month_from_now'] <= 2]['org_code'].to_list()).
                                                   difference(set(df.loc[(df['month_from_now'] >= 3) &
                                                                         (df['month_from_now'] <= 11)]['org_code'].
                                                                  to_list())))
        self.variables['oth_loan_inc_12_24m'] = len(set(df.loc[df['month_from_now'] <= 11]['org_code'].to_list()).
                                                    difference(set(df.loc[(df['month_from_now'] >= 12) &
                                                                          (df['month_from_now'] <= 23)]['org_code'].
                                                                   to_list())))
        return

    # 统计型数据统计
    def _statistic_data(self, df):
        if df.empty:
            return
        self.variables['oth_loan_apply_months_24m'] = len(set(df.loc[df['month_from_now'] <= 23]['month_from_now'].
                                                              to_list()))
        self.variables['oth_loan_month_avg_24m'] = round(df.loc[df['month_from_now'] <= 23].shape[0] / \
                                                   self.variables['oth_loan_apply_months_24m'], 2) if self.variables[
                                                                                                      'oth_loan_apply_months_24m'] > 0 else 0
        month_list = list()
        # total_avg_list为所有已放款主体的近12月月均查询次数总体平均值
        total_avg_list = [0.3334148130041555,
                          0.23955023221706184,
                          0.22757272060620876,
                          0.21583964800782204,
                          0.21339525788315816,
                          0.19995111219750672,
                          0.17599608897580055,
                          0.17819604008799805,
                          0.16304082131508188,
                          0.164996333414813,
                          0.15057443167929602,
                          0.15375213884135908]
        for i in range(12):
            cnt = df.loc[df['month_from_now'] == i].shape[0]
            month_list.append(cnt)
        avg = sum(month_list) / len(month_list)
        std = scipy.std(month_list)
        ttest, pval = ttest_rel(month_list, total_avg_list)
        self.variables['oth_loan_month_1sigma_12m'] = len([x for x in month_list if x > avg + std])
        self.variables['oth_loan_month_ttest_12m'] = round(ttest + pval - pval, 4)
        return

    # 模型公式概率计算
    def _model_score(self, df):
        if df.empty:
            return
        model_df = pd.DataFrame(columns=['intercept', 'org_cnt_12m', 'P2P_3m', 'HTL_9m', 'reason_else_6m',
                                         'dec_1_12m', 'NONBANK_3m'])
        model_df.loc[0, 'intercept'] = 1
        model_df.loc[0, 'org_cnt_12m'] = len(set(df.loc[df['month_from_now'] <= 11]['org_code'].to_list()))
        model_df.loc[0, 'P2P_3m'] = df.loc[(df['month_from_now'] <= 2) &
                                           (df['indu_code'] == 'P2P')].shape[0]
        model_df.loc[0, 'HTL_9m'] = df.loc[(df['month_from_now'] <= 8) &
                                           (df['indu_code'] == 'HTL')].shape[0]
        model_df.loc[0, 'reason_else_6m'] = df.loc[(df['month_from_now'] <= 5) &
                                                   (df['reason_code'] != '01')].shape[0]
        model_df.loc[0, 'dec_1_12m'] = len(set(df.loc[(df['month_from_now'] >= 1) &
                                                      (df['month_from_now'] <= 11)]['org_code'].to_list()).
                                           difference(set(df.loc[df['month_from_now'] == 0]['org_code'].
                                                          to_list())))
        model_df.loc[0, 'NONBANK_3m'] = df.loc[(df['month_from_now'] <= 2) &
                                               (df['indu_code'] != 'BAK')].shape[0]
        # 执行woe转换
        # Continuous Recoding: org_cnt_12m ###
        model_df.loc[:, 'r_org_cnt_12m'] = model_df['org_cnt_12m'].fillna(1.0)
        model_df.loc[model_df['r_org_cnt_12m'] > 15.5, 'r_org_cnt_12m'] = 15.5
        model_df.loc[model_df['r_org_cnt_12m'] < -1.0, 'r_org_cnt_12m'] = -1.0
        model_df.loc[:, 'r_woe_org_cnt_12m'] = 0
        model_df.loc[(-1.001 <= model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= -0.5),
                     'r_woe_org_cnt_12m'] = -1.1648465748979033
        model_df.loc[(-0.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 0.5),
                     'r_woe_org_cnt_12m'] = -0.9516095437864857
        model_df.loc[(0.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 1.5),
                     'r_woe_org_cnt_12m'] = -0.48401265457131504
        model_df.loc[(1.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 2.5),
                     'r_woe_org_cnt_12m'] = -0.24909203606608535
        model_df.loc[(2.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 3.5),
                     'r_woe_org_cnt_12m'] = 0.42336475123641376
        model_df.loc[(3.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 4.5),
                     'r_woe_org_cnt_12m'] = 0.5069689699044293
        model_df.loc[(4.5 < model_df['r_org_cnt_12m']) & (model_df['r_org_cnt_12m'] <= 35.0),
                     'r_woe_org_cnt_12m'] = 1.5820507611623447
        model_df.loc[pd.isnull(model_df['org_cnt_12m']), 'r_woe_org_cnt_12m'] = 0
        # Continuous Recoding: P2P_3m ###
        model_df.loc[:, 'r_p2p_3m'] = model_df['P2P_3m'].fillna(0.0)
        model_df.loc[model_df['r_p2p_3m'] > 4.5, 'r_p2p_3m'] = 4.5
        model_df.loc[model_df['r_p2p_3m'] < -1.0, 'r_p2p_3m'] = -1.0
        model_df.loc[:, 'r_woe_p2p_3m'] = 0
        model_df.loc[(-1.001 <= model_df['r_p2p_3m']) & (model_df['r_p2p_3m'] <= -0.5),
                     'r_woe_p2p_3m'] = -1.1648465748979033
        model_df.loc[(-0.5 < model_df['r_p2p_3m']) & (model_df['r_p2p_3m'] <= 0.5),
                     'r_woe_p2p_3m'] = -0.3166654608988103
        model_df.loc[(0.5 < model_df['r_p2p_3m']) & (model_df['r_p2p_3m'] <= 1.5),
                     'r_woe_p2p_3m'] = 0.5318345479886161
        model_df.loc[(1.5 < model_df['r_p2p_3m']) & (model_df['r_p2p_3m'] <= 10.0),
                     'r_woe_p2p_3m'] = 1.8092576671481833
        model_df.loc[pd.isnull(model_df['P2P_3m']), 'r_woe_p2p_3m'] = 0
        # Continuous Recoding: HTL_9m ###
        model_df.loc[:, 'r_htl_9m'] = model_df['HTL_9m'].fillna(0.0)
        model_df.loc[model_df['r_htl_9m'] > 6.0, 'r_htl_9m'] = 6.0
        model_df.loc[model_df['r_htl_9m'] < -1.0, 'r_htl_9m'] = -1.0
        model_df.loc[:, 'r_woe_htl_9m'] = 0
        model_df.loc[(-1.001 <= model_df['r_htl_9m']) & (model_df['r_htl_9m'] <= -0.5),
                     'r_woe_htl_9m'] = -1.1648465748979033
        model_df.loc[(-0.5 < model_df['r_htl_9m']) & (model_df['r_htl_9m'] <= 0.5),
                     'r_woe_htl_9m'] = -0.2564281533325186
        model_df.loc[(0.5 < model_df['r_htl_9m']) & (model_df['r_htl_9m'] <= 13.0),
                     'r_woe_htl_9m'] = 1.1693274709555992
        model_df.loc[pd.isnull(model_df['HTL_9m']), 'r_woe_htl_9m'] = 0
        # Continuous Recoding: reason_else_6m ###
        model_df.loc[:, 'r_reason_else_6m'] = model_df['reason_else_6m'].fillna(0.0)
        model_df.loc[model_df['r_reason_else_6m'] > 11.5, 'r_reason_else_6m'] = 11.5
        model_df.loc[model_df['r_reason_else_6m'] < -3.0, 'r_reason_else_6m'] = -3.0
        model_df.loc[:, 'r_woe_reason_else_6m'] = 0
        model_df.loc[(-3.001 <= model_df['r_reason_else_6m']) & (
                model_df['r_reason_else_6m'] <= -1.5), 'r_woe_reason_else_6m'] = -1.1648465748979033
        model_df.loc[(-1.5 < model_df['r_reason_else_6m']) & (
                model_df['r_reason_else_6m'] <= 0.5), 'r_woe_reason_else_6m'] = -0.50547489891745
        model_df.loc[(0.5 < model_df['r_reason_else_6m']) & (
                model_df['r_reason_else_6m'] <= 2.5), 'r_woe_reason_else_6m'] = 0.11176036196345397
        model_df.loc[(2.5 < model_df['r_reason_else_6m']) & (
                model_df['r_reason_else_6m'] <= 23.0), 'r_woe_reason_else_6m'] = 1.5184678459723926
        model_df.loc[pd.isnull(model_df['reason_else_6m']), 'r_woe_reason_else_6m'] = 0
        # Continuous Recoding: dec_1_12m ###
        model_df.loc[:, 'r_dec_1_12m'] = model_df['dec_1_12m'].fillna(1.0)
        model_df.loc[model_df['r_dec_1_12m'] > 12.649999999999864, 'r_dec_1_12m'] = 12.649999999999864
        model_df.loc[model_df['r_dec_1_12m'] < -1.0, 'r_dec_1_12m'] = -1.0
        model_df.loc[:, 'r_woe_dec_1_12m'] = 0
        model_df.loc[(-1.001 <= model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= -0.5),
                     'r_woe_dec_1_12m'] = -1.1648465748979033
        model_df.loc[(-0.5 < model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= 0.5),
                     'r_woe_dec_1_12m'] = -0.7285703003893413
        model_df.loc[(0.5 < model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= 1.5),
                     'r_woe_dec_1_12m'] = -0.265697495129455
        model_df.loc[(1.5 < model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= 2.5),
                     'r_woe_dec_1_12m'] = -0.1338102251382001
        model_df.loc[(2.5 < model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= 4.5),
                     'r_woe_dec_1_12m'] = 0.5808696237201016
        model_df.loc[(4.5 < model_df['r_dec_1_12m']) & (model_df['r_dec_1_12m'] <= 32.0),
                     'r_woe_dec_1_12m'] = 1.7163516427695722
        model_df.loc[pd.isnull(model_df['dec_1_12m']), 'r_woe_dec_1_12m'] = 0
        # Continuous Recoding: NONBANK_3m ###
        model_df.loc[:, 'r_nonbank_3m'] = model_df['NONBANK_3m'].fillna(0.0)
        model_df.loc[model_df['r_nonbank_3m'] > 16.0, 'r_nonbank_3m'] = 16.0
        model_df.loc[model_df['r_nonbank_3m'] < -13.0, 'r_nonbank_3m'] = -13.0
        model_df.loc[:, 'r_woe_nonbank_3m'] = 0
        model_df.loc[(-13.001 <= model_df['r_nonbank_3m']) & (model_df['r_nonbank_3m'] <= 0.5),
                     'r_woe_nonbank_3m'] = -0.6934745202648234
        model_df.loc[(0.5 < model_df['r_nonbank_3m']) & (model_df['r_nonbank_3m'] <= 1.5),
                     'r_woe_nonbank_3m'] = 0.2734999019435337
        model_df.loc[(1.5 < model_df['r_nonbank_3m']) & (model_df['r_nonbank_3m'] <= 2.5),
                     'r_woe_nonbank_3m'] = 0.6649881265096078
        model_df.loc[(2.5 < model_df['r_nonbank_3m']) & (model_df['r_nonbank_3m'] <= 16.0),
                     'r_woe_nonbank_3m'] = 1.443088162526502
        model_df.loc[pd.isnull(model_df['NONBANK_3m']), 'r_woe_nonbank_3m'] = 0
        # 模型公式计算
        logit1 = -2.593758 * model_df.loc[0, 'intercept'] + \
                 0.622697 * model_df.loc[0, 'r_woe_org_cnt_12m'] + \
                 0.459437 * model_df.loc[0, 'r_woe_p2p_3m'] + \
                 0.406185 * model_df.loc[0, 'r_woe_htl_9m']
        logit2 = -2.603892 * model_df.loc[0, 'intercept'] + \
                 0.337653 * model_df.loc[0, 'r_woe_reason_else_6m'] + \
                 0.532143 * model_df.loc[0, 'r_woe_dec_1_12m'] + \
                 0.47668 * model_df.loc[0, 'r_woe_nonbank_3m']
        score1 = 1 / (1 + scipy.exp(-logit1))
        score2 = 1 / (1 + scipy.exp(-logit2))
        self.variables['oth_loan_model_score'] = round(score1 + score2, 4)
        return

    # 执行变量转换
    def transform(self):
        logger.info("t31001 transform.")
        other_loan_df = self._loan_other_df()
        self._basic_query_data(other_loan_df)
        self._duplicate_org_data(other_loan_df)
        self._statistic_data(other_loan_df)
        self._model_score(other_loan_df)
