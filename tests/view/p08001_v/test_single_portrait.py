from portrait.transflow.single_account_portrait.trans_z02_single_portrait import SingleProtrait
from portrait.transflow.single_account_portrait.trans_flow import TransFlow
from portrait.transflow.single_account_portrait.trans_z03_single_summary_portrait import SingleSummaryPortrait
from portrait.transflow.single_account_portrait.trans_z04_single_remark_portrait import SingleRemarkPortrait
from portrait.transflow.single_account_portrait.trans_z05_single_counterparty_portrait import SingleCounterpartyPortrait
from portrait.transflow.single_account_portrait.trans_z07_single_loan_portrait import SingleLoanPortrait
from portrait.transflow.single_account_portrait.trans_z06_single_related_portrait import SingleRelatedPortrait
from sqlalchemy import create_engine
import pandas as pd
import random


def fake_df():
    df = pd.DataFrame(columns=['trans_date', 'trans_amt', 'account_balance', 'relationship', 'is_sensitive',
                               'is_interest', 'cost_type'])
    df['trans_date'] = pd.date_range('2019-12-01', '2020-12-31')
    length = len(df)
    account_balance = 10000
    for i in range(length):
        trans_amt = round((random.random() - 0.5) * 10000, 2)
        trans_amt = max(trans_amt, -account_balance)
        account_balance = round(account_balance + trans_amt, 2)

        df.loc[i, 'trans_amt'] = trans_amt
        df.loc[i, 'account_balance'] = account_balance
    df.loc[df.trans_date.isin(['2020-03-21', '2020-06-21', '2020-09-21', '2020-12-21']), 'is_interest'] = 1
    return df


def real_df():
    engine = create_engine('mysql+pymysql://financial_products_db:xdgkm7pj@192.168.1.9:3360/financial_products_tempdb',
                           encoding='utf8')
    sql = """select transaction_date as trans_date,transaction_amount as trans_amt,account_balance,remark as 
        remark_type,opponent_name,relation as relationship,sensetive as is_sensitive,costs as cost_type,
        is_loan as is_interest from transaction_flow where account_id=257"""
    df = pd.read_sql(sql, engine)
    return df


base = TransFlow()
r_df = real_df()
# r_df = pd.read_excel(r'E:\二级流水\TEST_CASE.xls')
r_df['loan_type'] = ''
base.trans_flow_portrait_df = r_df
base.trans_flow_portrait_df_2_year = r_df


def test_single_portrait():
    cls = SingleProtrait(base)
    cls.process()


def test_single_summary_portrait():
    cls = SingleSummaryPortrait(base)
    cls.process()


def test_single_remark_portrait():
    cls = SingleRemarkPortrait(base)
    cls.process()


def test_single_counterparty_portrait():
    cls = SingleCounterpartyPortrait(base)
    cls.process()


def test_single_related_portrait():
    cls = SingleRelatedPortrait(base)
    cls.process()


def test_single_loan_portrait():
    cls = SingleLoanPortrait(base)
    cls.process()
