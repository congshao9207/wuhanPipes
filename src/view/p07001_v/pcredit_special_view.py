from mapping.module_processor import ModuleProcessor
import pandas as pd


class PcreditSpecialView(ModuleProcessor):

    def process(self):
        self._get_special_info()

    def _get_special_info(self):
        loan_df = self.cached_data.get("pcredit_loan")
        special_df = self.cached_data.get("pcredit_special")
        if loan_df is None or loan_df.empty or special_df is None or special_df.empty:
            return
        special_df = special_df[special_df['special_type'] == '1']
        special_df = special_df.drop(['id'], axis=1)
        loan_df_temp = loan_df[loan_df['account_type'].isin(['01', '02', '03'])]
        if not loan_df_temp.empty:
            df = pd.merge(loan_df_temp, special_df, left_on='id', right_on='record_id')
            if not df.empty:
                self.variables["extension_number"] = df['id'].unique().size
