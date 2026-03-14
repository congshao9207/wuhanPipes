import pandas as pd
from datetime import datetime
from util.mysql_reader import sql_to_df
from mapping.trans_module_processor import TransModuleProcessor


class ApplyAmtPrediction(TransModuleProcessor):

    def process(self):
        self.variables["cus_apply_amt_pred"] = self._calculate_output(
            self.variables["q_2_balance_amt"],
            self.variables["balance_max_0_to_5"],
            self.variables["mean_sigma_left"],
            self.variables["mean_sigma_right"],
            self.variables["q_3_balance_amt"],
            self.variables["income_max_weight"],
            self.variables["mean_2sigma_left"],
            self.variables["balance_max"],
            self.variables["balance_min_weight"],
            self.variables["balance_max_weight"]
        )

    @staticmethod
    def _calculate_output(q_2_balance_amt,
                         balance_max_0_to_5,
                         mean_sigma_left,
                         mean_sigma_right,
                         q_3_balance_amt,
                         income_max_weight,
                         mean_2sigma_left,
                         balance_max,
                         balance_min_weight,
                         balance_max_weight
                         ):
        q_2_balance_amt = max(min(q_2_balance_amt, 195859.5), -107808.77)
        balance_max_0_to_5 = max(min(balance_max_0_to_5, 54224.48), 41950.83)
        mean_sigma_left = max(min(mean_sigma_left, 62770.26), -125197.66)
        mean_sigma_right = max(min(mean_sigma_right, 390882.77), -159093.06)
        q_3_balance_amt = max(min(q_3_balance_amt, 165827.93), -93053.75)
        income_max_weight = max(min(income_max_weight, 173475.71), -18316.07)
        mean_2sigma_left = max(min(mean_2sigma_left, 178849.4), -397759.54)
        balance_max = max(min(balance_max, 2363684.4), -1074526.9)
        balance_min_weight = max(min(balance_min_weight, 184786.17), -98508.38)
        balance_max_weight = max(min(balance_max_weight, 351823.87), -121134.98)

        cus_apply_amt_pred = q_2_balance_amt * 0.4093 + \
                             balance_max_0_to_5 * 8.5719 + \
                             mean_sigma_left * 1.4890 + \
                             mean_sigma_right * 2.7299 + \
                             q_3_balance_amt * 0.3622 + \
                             income_max_weight * (-2.6788) + \
                             mean_2sigma_left * 0.8685 + \
                             balance_max * 0.0902 + \
                             balance_min_weight * 2.3637 + \
                             balance_max_weight * (-1.2959)

        if cus_apply_amt_pred < 0:
            return 0
        elif cus_apply_amt_pred > 300e4:
            return 300e4
        else:
            return int(cus_apply_amt_pred/1e4) * 1e4
