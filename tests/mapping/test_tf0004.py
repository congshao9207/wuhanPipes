from mapping.tf0004 import Tf0004
import pandas as pd


def test_ps_court_info():
    ps = Tf0004()
    ps.run(user_name='罗淑英')
    ent_on_status = ['在营（开业）', '存续（在营、开业、在册）', '在营', '在业', '存续', '开业']
    legal_df = ps._com_bus_frinv_df(status=ent_on_status)
    shareholder_df = ps._com_bus_entinvitem_df(status=ent_on_status)
    concat_df = pd.concat([shareholder_df, legal_df])
    court_merge_df = ps._court_info_df(df=concat_df)
    judicative_df = ps._court_judicative_pape_df(df=court_merge_df)
    trial_df = ps._court_trial_process_df(df=court_merge_df)
    court_merge_df = ps._court_info_df(df=concat_df)
    dispute_df = pd.concat([judicative_df, trial_df])


    print(ps.variables['com_bus_court_open_docu_status'])

