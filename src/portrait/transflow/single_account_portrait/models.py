# coding: utf-8
from sqlalchemy import Column, DECIMAL, Date, DateTime, ForeignKey, Index, JSON, String, TIMESTAMP, Table, Text, Time
from sqlalchemy.dialects.mysql import BIGINT, BIT, INTEGER, LONGTEXT, MEDIUMTEXT, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CreditBaseInfo(Base):
    __tablename__ = 'credit_base_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    report_type = Column(String(32))
    credit_type = Column(String(16))
    name = Column(String(20))
    certificate_type = Column(String(16))
    certificate_no = Column(String(32))
    queryer = Column(String(50))
    query_reason = Column(String(50))
    query_time = Column(DateTime)
    report_time = Column(DateTime)
    create_user = Column(String(10))
    create_time = Column(DateTime)
    create_dep = Column(String(10))
    update_user = Column(String(10))
    update_time = Column(DateTime)
    update_dep = Column(String(10))
    file_path = Column(String(200))
    json_id = Column(String(32))
    report_no = Column(String(32))


class JhiPersistentAuditEvent(Base):
    __tablename__ = 'jhi_persistent_audit_event'
    __table_args__ = (
        Index('idx_persistent_audit_event', 'principal', 'event_date'),
    )

    event_id = Column(BIGINT(20), primary_key=True)
    principal = Column(String(50), nullable=False)
    event_date = Column(TIMESTAMP)
    event_type = Column(String(255))


class PcreditAccSpeculate(Base):
    __tablename__ = 'pcredit_acc_speculate'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    speculate_record_id = Column(BIGINT(20))
    report_time = Column(DateTime)
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    repay_month = Column(String(32))
    repay_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    account_status = Column(String(20))
    settled = Column(BIT(1))
    create_time = Column(DateTime)


class PcreditAccountCollection(Base):
    __tablename__ = 'pcredit_account_collection'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    account_type = Column(INTEGER(11))
    total_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    overdue_count = Column(INTEGER(11))
    overdue_count_day_90 = Column(INTEGER(11))
    assure_count = Column(INTEGER(11))


class PcreditAssetsManage(Base):
    __tablename__ = 'pcredit_assets_manage'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    asset_manage_company = Column(String(20))
    debt_date = Column(Date)
    debt_amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))
    lately_repay_date = Column(Date)
    amount = Column(DECIMAL(16, 4))


class PcreditBizInfo(Base):
    __tablename__ = 'pcredit_biz_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    biz_type = Column(String(16))
    biz_sub_type = Column(String(32))
    biz_counts = Column(INTEGER(11))
    biz_first_month = Column(String(16))


class PcreditCarTradeRecord(Base):
    __tablename__ = 'pcredit_car_trade_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    car_no = Column(String(32))
    engine_no = Column(String(128))
    car_brand = Column(String(64))
    car_type = Column(String(32))
    use_type = Column(String(32))
    car_status = Column(String(32))
    mortgage_tag = Column(String(32))
    info_update_date = Column(DateTime)


class PcreditCardInstitution(Base):
    __tablename__ = 'pcredit_card_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)


class PcreditCertInfo(Base):
    __tablename__ = 'pcredit_cert_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    cert_type = Column(String(32))
    cert_no = Column(String(32))


class PcreditCivilJudgmentsRecord(Base):
    __tablename__ = 'pcredit_civil_judgments_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    result = Column(String(256))
    effective_date = Column(DateTime)
    litigious = Column(String(64))
    litigious_amt = Column(DECIMAL(16, 4))


class PcreditCreditCard(Base):
    __tablename__ = 'pcredit_credit_card'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    card_type = Column(INTEGER(11))
    account_type = Column(INTEGER(11))
    credit_line = Column(DECIMAL(16, 4))
    used_line = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    overdraft_month_year_5 = Column(INTEGER(11))
    overdraft_month_day_90 = Column(INTEGER(11))
    overdraft_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditCreditGuarantee(Base):
    __tablename__ = 'pcredit_credit_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    credit_limit = Column(DECIMAL(16, 4))
    card_grant_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    used_limit = Column(DECIMAL(16, 4))
    bill_date = Column(Date)


class PcreditCreditTaxRecord(Base):
    __tablename__ = 'pcredit_credit_tax_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    org_name = Column(String(64))
    amount = Column(DECIMAL(16, 4))
    stats_date = Column(DateTime)


class PcreditDebit(Base):
    __tablename__ = 'pcredit_debit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    used_limit = Column(DECIMAL(16, 4))
    avg_limit_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    paln_repay_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    share_amt = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))


class PcreditDebitInfo(Base):
    __tablename__ = 'pcredit_debit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_times = Column(INTEGER(11))
    overdue_1_less_2 = Column(INTEGER(11))
    overdue_2_less_2 = Column(INTEGER(11))
    overdue_1_greater_2 = Column(INTEGER(11))
    overdue_2_greater_2 = Column(INTEGER(11))
    overdue_3_greater_2 = Column(INTEGER(11))
    overdue_4_greater_2 = Column(INTEGER(11))
    lowest_repayment_count = Column(INTEGER(11))


class PcreditDefaultInfo(Base):
    __tablename__ = 'pcredit_default_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    default_type = Column(String(32))
    default_subtype = Column(String(32))
    default_count = Column(INTEGER(11))
    default_month = Column(INTEGER(11))
    default_balance = Column(INTEGER(11))
    max_overdue_sum = Column(DECIMAL(16, 4))
    max_overdue_month = Column(INTEGER(11))


class PcreditForceExecutionRecord(Base):
    __tablename__ = 'pcredit_force_execution_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    court_name = Column(String(128))
    cause = Column(LONGTEXT)
    register_date = Column(DateTime)
    result_type = Column(String(32))
    case_status = Column(String(32))
    case_end_date = Column(DateTime)
    apply_execution_object = Column(String(128))
    apply_execution_object_amt = Column(DECIMAL(16, 4))
    executed_object = Column(String(128))
    executed_object_amt = Column(DECIMAL(16, 4))


class PcreditFundParticipationRecord(Base):
    __tablename__ = 'pcredit_fund_participation_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    pay_date = Column(DateTime)
    total_months = Column(String(32))
    begin_work_date = Column(String(32))
    pay_status = Column(String(32))
    personal_pay_rate = Column(DECIMAL(16, 4))
    monthly_amt = Column(DECIMAL(16, 4))
    pay_company = Column(String(128))
    info_update_date = Column(DateTime)
    reason = Column(String(128))


class PcreditGuaranteeOther(Base):
    __tablename__ = 'pcredit_guarantee_others'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    info = Column(String(200))
    start_date = Column(DateTime)
    bank_name = Column(String(100))
    money_type = Column(INTEGER(11))
    name = Column(String(32))
    contract_amount = Column(DECIMAL(16, 4))
    guarantee_amount = Column(DECIMAL(16, 4))
    loan_balance = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditGuaranteePay(Base):
    __tablename__ = 'pcredit_guarantee_pay'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    biz_type = Column(String(16))
    debt_date = Column(DateTime)
    debt_amount = Column(DECIMAL(16, 4))
    debt_status = Column(String(32))
    expiry_date = Column(DateTime)
    account_status = Column(String(32))
    account_closedate = Column(DateTime)
    repay_org = Column(String(20))
    lately_repay_date = Column(Date)
    repay_amount = Column(DECIMAL(16, 4))
    lately_repayment_date = Column(Date)
    amount = Column(DECIMAL(16, 4))


class PcreditGuaranteePayDetail(Base):
    __tablename__ = 'pcredit_guarantee_pay_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    guapay_id = Column(BIGINT(20))
    special_type = Column(String(32))
    occur_date = Column(DateTime)
    change_month = Column(INTEGER(11))
    occur_amont = Column(DECIMAL(16, 4))
    record = Column(String(128))


class PcreditHouseFundRecord(Base):
    __tablename__ = 'pcredit_house_fund_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    pay_date = Column(DateTime)
    pay_month = Column(String(32))
    pay_duration_date = Column(String(32))
    pay_status = Column(String(32))
    month_fee_amt = Column(DECIMAL(16, 4))
    personal_pay_rate = Column(DECIMAL(16, 4))
    company_pay_rate = Column(DECIMAL(16, 4))
    pay_company_name = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditHouseLoan(Base):
    __tablename__ = 'pcredit_house_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    remark = Column(String(200))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    overdue_amout = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditInfo(Base):
    __tablename__ = 'pcredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    housing_loan_count = Column(INTEGER(11))
    biz_housing_loan_count = Column(INTEGER(11))
    other_loan_count = Column(INTEGER(11))
    loan_1st_date = Column(String(7))
    debit_card_count = Column(INTEGER(11))
    debit_card_1st_date = Column(String(7))
    semi_credit_card_count = Column(INTEGER(11))
    semi_credit_card_1st_date = Column(INTEGER(11))
    declare_count = Column(INTEGER(11))
    dissent_count = Column(INTEGER(11))
    score = Column(INTEGER(11))
    score_date = Column(String(7))
    bad_debts_count = Column(INTEGER(11))
    bad_debts_balance = Column(DECIMAL(16, 4))
    asset_deal_count = Column(INTEGER(11))
    asset_deal_balance = Column(DECIMAL(16, 4))
    replace_repay_count = Column(INTEGER(11))
    replace_repay_balance = Column(DECIMAL(16, 4))
    loan_overdue_count = Column(INTEGER(11))
    loan_overdue_month = Column(INTEGER(11))
    loan_overdue_month_max_total = Column(DECIMAL(16, 4))
    loan_overdue_max_month = Column(INTEGER(11))
    debit_card_overdue_count = Column(INTEGER(11))
    debit_card_month_count = Column(INTEGER(11))
    debit_card_month_max_total = Column(DECIMAL(16, 4))
    debit_card_max_month = Column(INTEGER(11))
    semi_credit_card_overdraft_acount_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_month_60 = Column(INTEGER(11))
    semi_credit_card_overdraft_balance_60 = Column(DECIMAL(16, 4))
    semi_credit_card_overdraft_max_month_60 = Column(INTEGER(11))
    uncleared_legal_count = Column(INTEGER(11))
    uncleared_org_count = Column(INTEGER(11))
    uncleared_count = Column(INTEGER(11))
    uncleared_contract_count = Column(DECIMAL(16, 4))
    uncleared_balance = Column(DECIMAL(16, 4))
    uncleared_avg_repaly_6 = Column(DECIMAL(16, 4))
    undestroy_legal_count = Column(INTEGER(11))
    undestroy_org_count = Column(INTEGER(11))
    undestroy_count = Column(INTEGER(11))
    undestroy_limit = Column(DECIMAL(16, 4))
    undestory_max_limit = Column(DECIMAL(16, 4))
    undestory_min_limt = Column(DECIMAL(16, 4))
    undestory_used_limit = Column(DECIMAL(16, 4))
    undestory_avg_use = Column(DECIMAL(16, 4))
    undestory_semi_legal_count = Column(INTEGER(11))
    undestory_semi_org_count = Column(INTEGER(11))
    undestory_semi_count = Column(INTEGER(11))
    undestory_semi_limit = Column(DECIMAL(16, 4))
    undestory_semi_max_limit = Column(DECIMAL(16, 4))
    undestory_semi_min_limt = Column(DECIMAL(16, 4))
    undestory_semi_overdraft = Column(DECIMAL(16, 4))
    undestory_semi_avg_overdraft = Column(DECIMAL(16, 4))
    guarantee_count = Column(INTEGER(11))
    guarantee_amont = Column(DECIMAL(16, 4))
    guarantee_principal = Column(DECIMAL(16, 4))
    guarantee_catagory = Column(String(2))
    non_revolloan_org_count = Column(INTEGER(11))
    non_revolloan_accountno = Column(INTEGER(11))
    non_revolloan_totalcredit = Column(DECIMAL(16, 4))
    non_revolloan_balance = Column(DECIMAL(16, 4))
    non_revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    revolcredit_org_count = Column(INTEGER(11))
    revolcredit_account = Column(INTEGER(11))
    revolcredit_totalcredit = Column(DECIMAL(16, 4))
    revolcredit_balance = Column(DECIMAL(16, 4))
    revolcredit_repayin_6_m = Column(DECIMAL(16, 4))
    revolloan_org_count = Column(INTEGER(11))
    revolloan_account_no = Column(INTEGER(11))
    revolloan_totalcredit = Column(DECIMAL(16, 4))
    revolloan_balance = Column(DECIMAL(16, 4))
    revolloan_repayin_6_m = Column(DECIMAL(16, 4))
    ind_guarantee_count = Column(INTEGER(11))
    ind_guarantee_sum = Column(DECIMAL(16, 4))
    ind_guarantee_balance = Column(DECIMAL(16, 4))
    ind_repay_count = Column(INTEGER(11))
    ind_repay_sum = Column(DECIMAL(16, 4))
    ind_repay_balance = Column(DECIMAL(16, 4))
    ent_guarantee_count = Column(INTEGER(11))
    ent_guarantee_sum = Column(DECIMAL(16, 4))
    ent_guarantee_balance = Column(DECIMAL(16, 4))
    ent_repay_count = Column(INTEGER(11))
    ent_repay_sum = Column(DECIMAL(16, 4))
    ent_repay_balance = Column(DECIMAL(16, 4))


class PcreditInsuranceExtractRecord(Base):
    __tablename__ = 'pcredit_insurance_extract_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    address = Column(String(128))
    retirement_category = Column(String(64))
    retirement_date = Column(DateTime)
    begin_work_date = Column(DateTime)
    actual_amt = Column(DECIMAL(16, 4))
    stop_reason = Column(String(128))
    origin_company = Column(String(128))
    info_update_date = Column(DateTime)


class PcreditLargeScale(Base):
    __tablename__ = 'pcredit_large_scale'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    large_scale_quota = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    usedsum = Column(DECIMAL(16, 4))


class PcreditLive(Base):
    __tablename__ = 'pcredit_live'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    live_address = Column(String(50))
    phone = Column(String(50))
    live_address_type = Column(String(2))
    update_time = Column(DateTime)


class PcreditLoan(Base):
    __tablename__ = 'pcredit_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    account_type = Column(String(8))
    account_org = Column(String(32))
    account_mark = Column(String(32))
    loan_date = Column(Date)
    end_date = Column(DateTime)
    credit_purpose = Column(String(8))
    respon_object = Column(String(16))
    respon_type = Column(String(16))
    guarantee_no = Column(String(64))
    credit_limit = Column(DECIMAL(16, 4))
    credit_limit_no = Column(String(16))
    credit_share_amt = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    repay_period = Column(INTEGER(11))
    repay_frequency = Column(String(8))
    describe_text = Column(String(200))
    account_status = Column(String(16))
    category = Column(String(8))
    principal_amount = Column(DECIMAL(16, 4))
    surplus_repay_period = Column(INTEGER(11))
    repay_amount = Column(DECIMAL(16, 4))
    plan_repay_date = Column(Date)
    amout_replay_amount = Column(DECIMAL(16, 4))
    lately_replay_date = Column(Date)
    overdue_period = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))
    overdue_31_principal = Column(DECIMAL(16, 4))
    overdue_61_principal = Column(DECIMAL(16, 4))
    overdue_91_principal = Column(DECIMAL(16, 4))
    overdue_180_principal = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdue_start_year = Column(INTEGER(11))
    overdue_start_month = Column(INTEGER(11))
    overdue_end_year = Column(INTEGER(11))
    overdue_end_month = Column(INTEGER(11))
    loan_creditor = Column(String(50))
    loan_amount = Column(DECIMAL(16, 4))
    loan_type = Column(String(30))
    loan_guarantee_type = Column(String(8))
    loan_repay_type = Column(String(20))
    loan_repay_status = Column(String(16))
    joint_loan_mark = Column(String(32))
    expiry_date = Column(DateTime)
    loan_status_time = Column(DateTime)
    quota_used = Column(DECIMAL(16, 4))
    large_scale_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_limit = Column(DECIMAL(16, 4))
    bill_date = Column(DateTime)
    latest_category = Column(String(8))
    latest_loan_balance = Column(DECIMAL(16, 4))
    latest_replay_date = Column(DateTime)
    latest_replay_amount = Column(DECIMAL(16, 4))
    latest_repay_status = Column(String(16))
    loan_expire_date = Column(Date)
    loan_end_date = Column(Date)
    loan_status = Column(String(8))
    loan_balance = Column(DECIMAL(16, 4))


class PcreditLoanGuarantee(Base):
    __tablename__ = 'pcredit_loan_guarantee'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    org = Column(String(20))
    contract_amount = Column(DECIMAL(16, 4))
    loan_grant_date = Column(Date)
    loan_expire_date = Column(Date)
    amount = Column(DECIMAL(16, 4))
    principal_amount = Column(DECIMAL(16, 4))
    category = Column(String(8))
    plan_repay_date = Column(Date)


class PcreditLoanInstitution(Base):
    __tablename__ = 'pcredit_loan_institution'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    org_desc = Column(String(128))
    org_desc_add_date = Column(DateTime)
    jhi_declare = Column(String(128))
    declare_add_date = Column(DateTime)
    remark = Column(String(128))
    remark_add_date = Column(DateTime)
    special_remark = Column(String(128))
    special_remark_date = Column(DateTime)


class PcreditNoncreditDetail(Base):
    __tablename__ = 'pcredit_noncredit_details'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    pay_years = Column(INTEGER(11))
    pay_month = Column(INTEGER(11))
    pay_status = Column(String(16))


class PcreditNoncreditInfo(Base):
    __tablename__ = 'pcredit_noncredit_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    noncredit_type = Column(String(10))
    noncredit_count = Column(INTEGER(11))
    noncredit_sum = Column(DECIMAL(16, 4))


class PcreditNoncreditList(Base):
    __tablename__ = 'pcredit_noncredit_list'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    biz_org = Column(String(32))
    biz_type = Column(String(16))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(16))
    current_arrears_amt = Column(DECIMAL(16, 4))
    record_date = Column(DateTime)


class PcreditObjectionMark(Base):
    __tablename__ = 'pcredit_objection_mark'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    jhi_type = Column(String(32))
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditOtherLoan(Base):
    __tablename__ = 'pcredit_other_loan'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_type = Column(INTEGER(11))
    info = Column(String(200))
    start_date = Column(Date)
    bank_name = Column(String(32))
    money_type = Column(INTEGER(11))
    loan_type = Column(INTEGER(11))
    end_date = Column(Date)
    balance = Column(DECIMAL(16, 4))
    over_line = Column(DECIMAL(16, 4))
    overdue_month_year_5 = Column(INTEGER(11))
    overdue_month_day_90 = Column(INTEGER(11))
    loan_amount = Column(DECIMAL(16, 4))
    now_date = Column(Date)


class PcreditOverdraft(Base):
    __tablename__ = 'pcredit_overdraft'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditOverdue(Base):
    __tablename__ = 'pcredit_overdue'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    month_amount = Column(INTEGER(11))
    overdue_amount = Column(DECIMAL(16, 4))


class PcreditPersonInfo(Base):
    __tablename__ = 'pcredit_person_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    verifi_result = Column(String(64))
    authority = Column(String(64))
    sex = Column(String(8))
    birthday = Column(Date)
    marriage_status = Column(String(8))
    mobile_no = Column(String(20))
    work_tel = Column(String(20))
    home_tel = Column(String(20))
    education = Column(String(8))
    jhi_degree = Column(String(8))
    employment = Column(String(16))
    nationality = Column(String(64))
    email = Column(String(32))
    communication_address = Column(String(50))
    residence_address = Column(String(50))
    spouse_name = Column(String(20))
    spouse_certificate_type = Column(String(16))
    spouse_certificate_no = Column(String(32))
    spouse_work_unit = Column(String(50))
    spouse_mobile_no = Column(String(20))


class PcreditPersonalStatement(Base):
    __tablename__ = 'pcredit_personal_statement'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    content = Column(String(128))
    add_date = Column(DateTime)


class PcreditPhoneHi(Base):
    __tablename__ = 'pcredit_phone_his'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    no = Column(INTEGER(11))
    phone = Column(String(16))
    update_time = Column(DateTime)


class PcreditPortraitsMain(Base):
    __tablename__ = 'pcredit_portraits_main'

    id = Column(BIGINT(20), primary_key=True)
    reprot_id = Column(String(32), nullable=False)
    report_time = Column(DateTime)
    marital_status = Column(INTEGER(11))
    is_owened = Column(INTEGER(11))
    is_mortgage = Column(INTEGER(11))
    is_have_credit = Column(INTEGER(11))


class PcreditPortraitsQuery(Base):
    __tablename__ = 'pcredit_portraits_query'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_approval_inquiry_month_1 = Column(INTEGER(11))
    credit_approval_inquiry_month_1 = Column(INTEGER(11))
    self_inquiry_month_1 = Column(INTEGER(11))
    qualification_examination_year_2 = Column(INTEGER(11))
    loan_approval_month_2 = Column(INTEGER(11))
    credit_approval_month_2 = Column(INTEGER(11))
    loan_approval_month_3 = Column(INTEGER(11))
    credit_approval_month_3 = Column(INTEGER(11))
    loan_approval_month_6 = Column(INTEGER(11))
    credit_approval_month_6 = Column(INTEGER(11))
    loan_approval_year_1 = Column(INTEGER(11))
    credit_approval_year_1 = Column(INTEGER(11))
    qualification_examination_year_1 = Column(INTEGER(11))
    approvals_month_3 = Column(INTEGER(11))


class PcreditPortraitsSummary(Base):
    __tablename__ = 'pcredit_portraits_summary'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    house_loan_count = Column(INTEGER(11))
    first_loan_month = Column(String(10))
    first_credit_month = Column(String(10))
    sum_count = Column(INTEGER(11))
    loan_overdue_count = Column(INTEGER(11))
    loan_max_overdue_money = Column(DECIMAL(16, 4))
    loan_max_overdue_month = Column(INTEGER(11))
    credit_overdue_account_count = Column(INTEGER(11))
    credit_overdue_month_count = Column(INTEGER(11))
    credit_max_overdue_money = Column(DECIMAL(16, 4))
    credit_max_overdue_month = Column(INTEGER(11))
    unsettled_loan_organization_number = Column(INTEGER(11))
    unsettled_loan_number = Column(INTEGER(11))
    unsettled_loan_contract_total = Column(DECIMAL(16, 4))
    unsettled_loan_total_balance = Column(DECIMAL(16, 4))
    unsettled_loan_ave_month_6 = Column(DECIMAL(16, 4))
    uncancelled_credit_organization_number = Column(INTEGER(11))
    uncancelled_credit_total_money = Column(DECIMAL(16, 4))
    uncancelled_credit_max_money = Column(DECIMAL(16, 4))
    uncancelled_credit_used_money = Column(DECIMAL(16, 4))
    uncancellation_credit_average_month_6 = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_account_number = Column(INTEGER(11))
    uncancelled_quasicredit_total_money = Column(DECIMAL(16, 4))
    uncancelled_quasicredit_average_month_6 = Column(DECIMAL(16, 4))
    foreign_guaranty_number = Column(INTEGER(11))
    foreign_guaranty_principal_balance = Column(DECIMAL(16, 4))
    credit_used_rate = Column(DECIMAL(16, 4))


class PcreditPortraitsTransaction(Base):
    __tablename__ = 'pcredit_portraits_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    loan_account_abnormality = Column(INTEGER(11))
    loan_fivelevel_abnormality = Column(INTEGER(11))
    loan_overdue_month_6 = Column(INTEGER(11))
    loan_overdue_year_2 = Column(INTEGER(11))
    loan_overdue_year_5 = Column(INTEGER(11))
    loan_max_overdue_month = Column(INTEGER(11))
    loan_max_overdue_number = Column(INTEGER(11))
    unsettled_loan_max_overdue_year_2 = Column(INTEGER(11))
    large_loan_overdue_twice = Column(INTEGER(11))
    small_loan_overdue_twice = Column(INTEGER(11))
    business_loan_overdue_twice = Column(INTEGER(11))
    unsettled_loan_unbank_number = Column(INTEGER(11))
    unsettled_busloan_agency_number = Column(INTEGER(11))
    loan_new_total_month_6 = Column(DECIMAL(16, 4))
    loan_new_total_year_1 = Column(DECIMAL(16, 4))
    loan_new_agency_month_3 = Column(INTEGER(11))
    loan_new_agency_year_1 = Column(INTEGER(11))
    guaranteed_loan_total_month_6 = Column(DECIMAL(16, 4))
    guaranteed_loan_total_year_1 = Column(DECIMAL(16, 4))
    guaranteed_loan_agency_year_1 = Column(INTEGER(11))
    loan_expiration_total_month_3 = Column(DECIMAL(16, 4))
    loan_expiration_total_month_6 = Column(DECIMAL(16, 4))
    loan_expiration_total_year_1 = Column(DECIMAL(16, 4))
    business_loan_agency_year_2 = Column(INTEGER(11))
    unsettled_house_loan_number = Column(INTEGER(11))
    unsettled_car_loan_number = Column(INTEGER(11))
    unsettled_house_loan_payed = Column(DECIMAL(16, 4))
    unsettled_car_loan_payed = Column(DECIMAL(16, 4))
    unsettled_loan_overdue_money = Column(DECIMAL(16, 4))
    unsettled_loan_bank_number = Column(INTEGER(11))
    loan_gdz_year_2 = Column(INTEGER(11))
    extension_number = Column(INTEGER(11))
    loan_new_number_month_6 = Column(INTEGER(11))
    loan_new_number_year_1 = Column(INTEGER(11))
    loan_max_overdue_month_6 = Column(INTEGER(11))
    loan_max_overdue_year_1 = Column(INTEGER(11))
    loan_max_overdue_year_2 = Column(INTEGER(11))
    business_loan_corpus_overdue = Column(INTEGER(11))
    loan_doubtful = Column(INTEGER(11))
    credit_account_abnormality = Column(INTEGER(11))
    credit_overdue_month_6 = Column(INTEGER(11))
    credit_overdue_year_2 = Column(INTEGER(11))
    credit_overdue_year_5 = Column(INTEGER(11))
    credit_new_total_year_1 = Column(DECIMAL(16, 4))
    credit_new_total_month_6 = Column(DECIMAL(16, 4))
    credit_gdz_year_2 = Column(INTEGER(11))
    credit_max_overdue_number = Column(INTEGER(11))
    credit_max_overdue_year_2 = Column(INTEGER(11))
    credit_activated_number = Column(INTEGER(11))
    credit_new_number_month_6 = Column(INTEGER(11))
    credit_new_number_year_1 = Column(INTEGER(11))
    credit_now_overdue_money = Column(DECIMAL(16, 4))
    credit_min_payed_number = Column(INTEGER(11))
    credit_quasi_abnormality = Column(INTEGER(11))
    loan_scured_five_abnormality = Column(INTEGER(11))
    credit_financial_tension = Column(DECIMAL(16, 4))


class PcreditProfession(Base):
    __tablename__ = 'pcredit_profession'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    work_unit = Column(String(50))
    work_type = Column(String(64))
    work_phone = Column(String(16))
    work_address = Column(String(50))
    profession = Column(String(20))
    industry = Column(String(20))
    duty = Column(String(10))
    duty_title = Column(String(10))
    enter_date = Column(Date)
    update_time = Column(Date)


class PcreditPubInfo(Base):
    __tablename__ = 'pcredit_pub_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    pub_type = Column(String(32))
    pub_count = Column(INTEGER(11))
    pub_sum = Column(DECIMAL(16, 4))


class PcreditPublicContent(Base):
    __tablename__ = 'pcredit_public_content'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    jhi_comment = Column(String(200))
    detail_id = Column(String(32))


class PcreditPunishmentRecord(Base):
    __tablename__ = 'pcredit_punishment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    org_name = Column(String(128))
    content = Column(LONGTEXT)
    amount = Column(DECIMAL(16, 4))
    effective_date = Column(DateTime)
    end_date = Column(DateTime)
    reconsideration_result = Column(String(128))


class PcreditQualificationRecord(Base):
    __tablename__ = 'pcredit_qualification_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    qualification_name = Column(String(128))
    grade = Column(String(32))
    award_date = Column(DateTime)
    expired_date = Column(DateTime)
    revoked_date = Column(DateTime)
    award_org = Column(String(128))
    org_address = Column(String(128))


class PcreditQueryRecord(Base):
    __tablename__ = 'pcredit_query_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    no = Column(INTEGER(11))
    jhi_time = Column(Date)
    operator = Column(String(50))
    reason = Column(String(20))


class PcreditQueryTime(Base):
    __tablename__ = 'pcredit_query_times'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    last_query_time = Column(DateTime)
    last_query_org = Column(String(32))
    last_query_type = Column(String(32))
    loan_org_1 = Column(INTEGER(11))
    credit_org_1 = Column(INTEGER(11))
    loan_times_1 = Column(INTEGER(11))
    credit_times_1 = Column(INTEGER(11))
    self_times_1 = Column(INTEGER(11))
    loan_times_2 = Column(INTEGER(11))
    guarantee_times_2 = Column(INTEGER(11))
    agreement_times_2 = Column(INTEGER(11))


class PcreditRepayment(Base):
    __tablename__ = 'pcredit_repayment'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    jhi_year = Column(INTEGER(11))
    month = Column(INTEGER(11))
    repayment_amt = Column(DECIMAL(16, 4))
    status = Column(String(8))


class PcreditRewardRecord(Base):
    __tablename__ = 'pcredit_reward_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    reward_org = Column(String(128))
    reward_content = Column(String(128))
    effective_date = Column(DateTime)
    expired_date = Column(DateTime)


class PcreditScoreInfo(Base):
    __tablename__ = 'pcredit_score_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    score = Column(INTEGER(11))
    position = Column(String(32))
    desc_content = Column(String(64))


class PcreditSemiCredit(Base):
    __tablename__ = 'pcredit_semi_credit'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    describe_text = Column(String(200))
    account_status = Column(String(16))
    overdraft_balance = Column(DECIMAL(16, 4))
    avg_overdraft_balance_6 = Column(DECIMAL(16, 4))
    max_overdraft_amount = Column(DECIMAL(16, 4))
    bill_date = Column(Date)
    actual_repay_amount = Column(DECIMAL(16, 4))
    lately_repay_date = Column(Date)
    overdraft_amount_180 = Column(DECIMAL(16, 4))
    share_amt = Column(DECIMAL(16, 4))
    remarks = Column(String(200))
    repayment_start_year = Column(INTEGER(11))
    repayment_start_month = Column(INTEGER(11))
    repayment_end_year = Column(INTEGER(11))
    repayment_end_month = Column(INTEGER(11))
    overdraft_start_year = Column(INTEGER(11))
    overdraft_start_month = Column(INTEGER(11))
    overdraft_end_year = Column(INTEGER(11))
    overdraft_end_month = Column(INTEGER(11))


class PcreditSimplePortrait(Base):
    __tablename__ = 'pcredit_simple_portraits'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    house_count = Column(INTEGER(11))
    un_house_count = Column(INTEGER(11))
    house_amout = Column(DECIMAL(16, 4))
    un_house_amount = Column(DECIMAL(16, 4))
    un_house_balance = Column(DECIMAL(16, 4))
    un_other_count = Column(INTEGER(11))
    un_other_amount = Column(DECIMAL(16, 4))
    un_other_balance = Column(DECIMAL(16, 4))
    other_max_amout = Column(DECIMAL(16, 4))
    loan_car_count = Column(INTEGER(11))
    un_credit_amount = Column(DECIMAL(16, 4))
    un_credit_bank_amount = Column(DECIMAL(16, 4))
    un_credit_used = Column(DECIMAL(16, 4))
    foreign_guaranty_count = Column(INTEGER(11))
    foreign_guaranty_amount = Column(DECIMAL(16, 4))
    overdue_house_count = Column(INTEGER(11))
    overdue_other_count = Column(INTEGER(11))
    overdue_credit_count = Column(INTEGER(11))
    loan_current_amount = Column(DECIMAL(16, 4))
    credit_current_amount = Column(DECIMAL(16, 4))
    loan_overdue_day_90 = Column(INTEGER(11))
    credit_overdue_day_90 = Column(INTEGER(11))
    un_loan_year_5 = Column(INTEGER(11))
    un_credit_overdue_year_5 = Column(INTEGER(11))
    overdue_house_year_5 = Column(INTEGER(11))
    overdue_other_year_5 = Column(INTEGER(11))
    overdue_credit_year_5 = Column(INTEGER(11))
    credit_amount_month_6 = Column(DECIMAL(16, 4))
    credit_count_month_6 = Column(INTEGER(11))
    credit_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_month_6 = Column(DECIMAL(16, 4))
    loan_count_month_6 = Column(INTEGER(11))
    loan_amount_month_3 = Column(DECIMAL(16, 4))
    loan_amount_future_6 = Column(DECIMAL(16, 4))
    loan_count_query_6 = Column(INTEGER(11))
    loan_count_query_3 = Column(INTEGER(11))
    credit_count_query_6 = Column(INTEGER(11))
    credit_count_query_3 = Column(INTEGER(11))
    self_count_query_6 = Column(INTEGER(11))
    self_count_query_3 = Column(INTEGER(11))
    report_time = Column(DateTime)
    self_house_count = Column(INTEGER(11))
    self_fund_count = Column(INTEGER(11))
    un_house_month_pay = Column(DECIMAL(16, 4))
    clear_house_loan_amout = Column(DECIMAL(16, 4))
    un_house_loan_rate = Column(DECIMAL(16, 4))
    un_car_month_pay = Column(DECIMAL(16, 4))
    clear_car_amount = Column(DECIMAL(16, 4))
    clear_other_count = Column(INTEGER(11))
    un_other_loan_count = Column(INTEGER(11))
    un_other_limit_min = Column(DECIMAL(16, 4))
    un_other_limit_max = Column(DECIMAL(16, 4))
    clear_other_limit_min = Column(DECIMAL(16, 4))
    clear_other_limit_max = Column(DECIMAL(16, 4))
    other_month_amount = Column(DECIMAL(16, 4))
    credit_use_rate = Column(DECIMAL(16, 4))
    pb_loan_max_amount = Column(DECIMAL(16, 4))
    credit_report_month = Column(INTEGER(11))
    credit_yuan_count = Column(INTEGER(11))
    overdue_count_year_5 = Column(INTEGER(11))
    un_pb_overdue_count = Column(INTEGER(11))
    loan_amount_future_2 = Column(DECIMAL(16, 4))
    approve_count_month_3 = Column(INTEGER(11))
    approve_count_month_6 = Column(INTEGER(11))
    approve_count_year_2 = Column(INTEGER(11))
    credit_is_normal = Column(INTEGER(11))
    loan_is_normal = Column(INTEGER(11))
    inter_buzy_bank_limit_6_m = Column(DECIMAL(16, 4))
    inter_buzy_bank_limit_max = Column(DECIMAL(16, 4))
    inter_buzy_bank_count_6_m = Column(INTEGER(11))
    other_mont_fixed_payment = Column(DECIMAL(16, 4))
    other_mont_insert_first = Column(DECIMAL(16, 4))
    clear_other_amount_year_1 = Column(DECIMAL(16, 4))
    clear_other_amount_year_2 = Column(DECIMAL(16, 4))
    clear_other_amount_year_3 = Column(DECIMAL(16, 4))
    clear_other_amount_year_4 = Column(DECIMAL(16, 4))
    clear_other_amount_year_1_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_2_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_3_max = Column(DECIMAL(16, 4))
    clear_other_amount_year_4_max = Column(DECIMAL(16, 4))
    credit_report_loan_month = Column(INTEGER(11))
    pb_loan_amount_future_12 = Column(DECIMAL(16, 4))
    loan_amount_future_12 = Column(DECIMAL(16, 4))
    overdue_un_mont_fixed = Column(INTEGER(11))
    un_loan_continued = Column(INTEGER(11))


class PcreditSpecial(Base):
    __tablename__ = 'pcredit_special'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    record_id = Column(BIGINT(20))
    record_type = Column(String(8))
    special_type = Column(String(32))
    special_date = Column(DateTime)
    special_month = Column(INTEGER(11))
    special_money = Column(DECIMAL(16, 4))
    special_comment = Column(String(80))


class PcreditSpeculateRecord(Base):
    __tablename__ = 'pcredit_speculate_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    record_id = Column(BIGINT(20))
    pre_start_date = Column(DateTime)
    pre_end_date = Column(DateTime)
    pre_acc_org = Column(INTEGER(11))
    pre_amt = Column(DECIMAL(16, 4))
    pre_balance = Column(DECIMAL(16, 4))
    pre_freq = Column(INTEGER(11))
    pre_terms = Column(INTEGER(11))
    pre_should_amt = Column(DECIMAL(16, 4))
    pre_real_amt = Column(DECIMAL(16, 4))
    pre_res_terms = Column(INTEGER(11))
    pre_should_date = Column(DateTime)
    pre_month_days = Column(INTEGER(11))
    pre_quar_days = Column(INTEGER(11))
    pre_real_date = Column(DateTime)
    pre_month_period = Column(INTEGER(11))
    pre_quar_period = Column(INTEGER(11))
    loan_repay_type = Column(String(32))
    nominal_interest_rate = Column(DECIMAL(16, 4))
    real_interest_rate = Column(DECIMAL(16, 4))
    settled = Column(BIT(1))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class PcreditTelecomPaymentRecord(Base):
    __tablename__ = 'pcredit_telecom_payment_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    carrier = Column(String(32))
    biz_type = Column(String(128))
    biz_start_date = Column(DateTime)
    current_payment_status = Column(String(32))
    current_arrears_amt = Column(DECIMAL(16, 4))
    current_arrears_months = Column(String(32))
    record_date = Column(DateTime)
    last_24_month_payment_history = Column(String(512))


class PcreditThresholdRecord(Base):
    __tablename__ = 'pcredit_threshold_record'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    seq = Column(String(32))
    personal_category = Column(String(32))
    address = Column(String(128))
    work_place = Column(String(128))
    home_monthly_income = Column(DECIMAL(16, 4))
    apply_date = Column(DateTime)
    approval_date = Column(DateTime)
    info_update_date = Column(DateTime)


class PcreditTransaction(Base):
    __tablename__ = 'pcredit_transaction'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32), nullable=False)
    remarks = Column(String(200))


class PcreditWarnInfo(Base):
    __tablename__ = 'pcredit_warn_info'

    id = Column(BIGINT(20), primary_key=True)
    report_id = Column(String(32))
    warn_content = Column(String(256))
    effective_date = Column(DateTime)
    expiry_date = Column(DateTime)


class RawDatum(Base):
    __tablename__ = 'raw_data'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20))
    api_no = Column(String(20))
    raw_data_desc = Column(String(64))
    risk_subject_id = Column(BIGINT(20))
    expired_at = Column(DateTime)
    req_message = Column(Text)
    req_msg_check_sum = Column(String(512))
    data_type = Column(String(50))
    raw_data = Column(MEDIUMTEXT)
    cause = Column(String(64))
    create_time = Column(DateTime)


class RequestRecord(Base):
    __tablename__ = 'request_record'

    id = Column(BIGINT(20), primary_key=True)
    biz_type = Column(String(50))
    api_no = Column(String(20))
    request_id = Column(String(50))
    user_name = Column(String(50))
    risk_subject_id = Column(BIGINT(20))
    user_seq = Column(String(64))
    merchant_no = Column(String(64))
    request_param = Column(Text)
    request_status = Column(String(50))
    data_fetch_mode = Column(String(50))
    create_time = Column(DateTime)


class RiskSubject(Base):
    __tablename__ = 'risk_subject'

    id = Column(BIGINT(20), primary_key=True)
    user_type = Column(String(255))
    name = Column(String(64))
    phone = Column(String(15))
    unique_no = Column(String(64))
    reg_code = Column(String(64))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)


class TransAccount(Base):
    __tablename__ = 'trans_account'

    id = Column(BIGINT(20), primary_key=True)
    out_req_no = Column(String(32))
    account_name = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    bank = Column(String(32))
    account_no = Column(String(64))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    trans_flow_type = Column(INTEGER(11))
    update_time = Column(DateTime)
    account_state = Column(INTEGER(11))
    create_time = Column(DateTime)


class TransApply(Base):
    __tablename__ = 'trans_apply'

    id = Column(BIGINT(20), primary_key=True)
    out_req_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_no = Column(String(32))
    cus_name = Column(String(32))
    related_name = Column(String(32))
    relationship = Column(String(32))
    account_id = Column(BIGINT(20))
    industry = Column(String(32))
    id_card_no = Column(String(32))
    id_type = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlow(Base):
    __tablename__ = 'trans_flow'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    trans_time = Column(DateTime)
    opponent_name = Column(String(255))
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(16))
    trans_channel = Column(String(16))
    trans_type = Column(String(16))
    trans_use = Column(String(16))
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlowException(Base):
    __tablename__ = 'trans_flow_exception'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    out_req_no = Column(String(32))
    trans_time = Column(DateTime)
    opponent_name = Column(String(255))
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    currency = Column(String(16))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(16))
    trans_channel = Column(String(16))
    trans_type = Column(String(16))
    trans_use = Column(String(16))
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransFlowPortrait(Base):
    __tablename__ = 'trans_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    account_id = Column(BIGINT(20))
    trans_date = Column(Date)
    trans_time = Column(Time)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    opponent_name = Column(String(64))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(32))
    opponent_account_bank = Column(String(32))
    trans_channel = Column(String(64))
    trans_type = Column(String(64))
    trans_use = Column(String(64))
    remark = Column(String(255))
    currency = Column(String(16))
    phone = Column(String(32))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    loan_type = Column(String(32))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(255))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    trans_flow_src_type = Column(INTEGER(11))
    usual_trans_type = Column(String(256))


class TransSingleAbnormalRecovery(Base):
    __tablename__ = 'trans_single_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(32))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleCounterpartyPortrait(Base):
    __tablename__ = 'trans_single_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(32))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleLoanPortrait(Base):
    __tablename__ = 'trans_single_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    loan_type = Column(String(16))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSinglePortrait(Base):
    __tablename__ = 'trans_single_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(INTEGER(11))
    normal_income_d_mean = Column(INTEGER(11))
    normal_income_m_mean = Column(INTEGER(11))
    normal_income_m_std = Column(INTEGER(11))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    income_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleRelatedPortrait(Base):
    __tablename__ = 'trans_single_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(32))
    relationship = Column(String(32))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    opponent_account_no = Column(String(64))


class TransSingleRemarkPortrait(Base):
    __tablename__ = 'trans_single_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    remark_type = Column(String(32))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransSingleSummaryPortrait(Base):
    __tablename__ = 'trans_single_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    variable_cost_amt = Column(DECIMAL(16, 4))


class TransUAbnormalRecovery(Base):
    __tablename__ = 'trans_u_abnormal_recovery'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    account_id = Column(BIGINT(20))
    flow_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    account_no = Column(String(64))
    abnormal_recovery_id = Column(BIGINT(20))
    abnormal_recovery_label = Column(String(32))
    trans_amt = Column(DECIMAL(16, 4))
    trans_datetime = Column(DateTime)
    remark = Column(String(64))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUCounterpartyPortrait(Base):
    __tablename__ = 'trans_u_counterparty_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    month = Column(String(16))
    opponent_name = Column(String(64))
    income_amt_order = Column(String(16))
    expense_amt_order = Column(String(16))
    trans_amt = Column(DECIMAL(16, 4))
    trans_month_cnt = Column(INTEGER(11))
    trans_cnt = Column(INTEGER(11))
    trans_mean = Column(DECIMAL(16, 4))
    trans_amt_proportion = Column(DECIMAL(16, 4))
    trans_gap_avg = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUFlowPortrait(Base):
    __tablename__ = 'trans_u_flow_portrait'

    id = Column(BIGINT(20), primary_key=True)
    flow_id = Column(BIGINT(20))
    apply_no = Column(String(32))
    account_id = Column(BIGINT(20))
    report_req_no = Column(String(32))
    trans_date = Column(DateTime)
    trans_time = Column(DateTime)
    trans_amt = Column(DECIMAL(16, 4))
    account_balance = Column(DECIMAL(16, 4))
    bank = Column(String(64))
    account_no = Column(String(64))
    opponent_name = Column(String(64))
    opponent_type = Column(INTEGER(11))
    opponent_account_no = Column(String(64))
    opponent_account_bank = Column(String(64))
    trans_channel = Column(String(64))
    trans_type = Column(String(32))
    trans_use = Column(String(64))
    remark = Column(String(64))
    currency = Column(String(16))
    phone = Column(String(16))
    relationship = Column(String(32))
    is_financing = Column(INTEGER(11))
    is_interest = Column(INTEGER(11))
    is_repay = Column(INTEGER(11))
    is_before_interest_repay = Column(INTEGER(11))
    loan_type = Column(String(16))
    unusual_trans_type = Column(String(16))
    is_sensitive = Column(INTEGER(11))
    cost_type = Column(String(16))
    remark_type = Column(String(64))
    income_cnt_order = Column(INTEGER(11))
    expense_cnt_order = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    trans_flow_src_type = Column(INTEGER(11))
    usual_trans_type = Column(String(256))


class TransULoanPortrait(Base):
    __tablename__ = 'trans_u_loan_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    loan_type = Column(String(32))
    month = Column(String(16))
    loan_amt = Column(DECIMAL(16, 4))
    loan_cnt = Column(INTEGER(11))
    loan_mean = Column(DECIMAL(16, 4))
    repay_amt = Column(DECIMAL(16, 4))
    repay_cnt = Column(INTEGER(11))
    repay_mean = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUModelling(Base):
    __tablename__ = 'trans_u_modelling'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    apply_amt = Column(DECIMAL(16, 4))
    pawn_cnt = Column(INTEGER(11))
    medical_cnt = Column(INTEGER(11))
    court_cnt = Column(INTEGER(11))
    insure_cnt = Column(INTEGER(11))
    night_trans_cnt = Column(INTEGER(11))
    fam_unstab_cnt = Column(INTEGER(11))
    balance_mean = Column(DECIMAL(16, 4))
    balance_max = Column(DECIMAL(16, 4))
    balance_max_0_to_5 = Column(DECIMAL(16, 4))
    balance_0_to_5_prop = Column(DECIMAL(16, 4))
    income_0_to_5_prop = Column(DECIMAL(16, 4))
    balance_min_weight = Column(DECIMAL(16, 4))
    balance_max_weight = Column(DECIMAL(16, 4))
    income_max_weight = Column(DECIMAL(16, 4))
    half_year_interest_amt = Column(DECIMAL(16, 4))
    half_year_balance_amt = Column(DECIMAL(16, 4))
    year_interest_amt = Column(DECIMAL(16, 4))
    q_2_balance_amt = Column(DECIMAL(16, 4))
    q_3_balance_amt = Column(DECIMAL(16, 4))
    year_interest_balance_prop = Column(DECIMAL(16, 4))
    q_4_interest_balance_prop = Column(DECIMAL(16, 4))
    income_mean = Column(DECIMAL(16, 4))
    mean_sigma_left = Column(DECIMAL(16, 4))
    mean_sigma_right = Column(DECIMAL(16, 4))
    mean_2_sigma_left = Column(DECIMAL(16, 4))
    mean_2_sigma_right = Column(DECIMAL(16, 4))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_amt_d_mean = Column(DECIMAL(16, 4))
    normal_income_amt_m_mean = Column(DECIMAL(16, 4))
    normal_expense_amt_m_std = Column(DECIMAL(16, 4))
    opponent_cnt = Column(INTEGER(11))
    income_rank_1_amt = Column(DECIMAL(16, 4))
    income_rank_2_amt = Column(DECIMAL(16, 4))
    income_rank_3_amt = Column(DECIMAL(16, 4))
    income_rank_4_amt = Column(DECIMAL(16, 4))
    income_rank_2_cnt_prop = Column(DECIMAL(16, 4))
    expense_rank_6_avg_gap = Column(DECIMAL(16, 4))
    income_rank_9_avg_gap = Column(DECIMAL(16, 4))
    expense_rank_10_avg_gap = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    enterprise_3_income_amt = Column(DECIMAL(16, 4))
    enterprise_3_expense_cnt_prop = Column(DECIMAL(16, 4))
    all_relations_expense_cnt_prop = Column(DECIMAL(16, 4))
    hit_loan_type_cnt_6_cm = Column(INTEGER(11))
    private_income_amt_12_cm = Column(DECIMAL(16, 4))
    private_income_mean_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_amt_12_cm = Column(DECIMAL(16, 4))
    pettyloan_income_mean_12_cm = Column(DECIMAL(16, 4))
    finlease_expense_cnt_6_cm = Column(INTEGER(11))
    otherfin_income_mean_3_cm = Column(DECIMAL(16, 4))
    all_loan_expense_cnt_3_cm = Column(DECIMAL(16, 4))
    income_net_rate_compare_2 = Column(DECIMAL(16, 4))
    cus_apply_amt_pred = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUPortrait(Base):
    __tablename__ = 'trans_u_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    analyse_start_time = Column(DateTime)
    analyse_end_time = Column(DateTime)
    not_full_month = Column(String(16))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_income_cnt = Column(INTEGER(11))
    normal_income_mean = Column(DECIMAL(16, 4))
    normal_income_d_mean = Column(DECIMAL(16, 4))
    normal_income_m_mean = Column(DECIMAL(16, 4))
    normal_income_m_std = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    normal_expense_cnt = Column(INTEGER(11))
    income_amt_y_pred = Column(DECIMAL(16, 4))
    relationship_risk = Column(INTEGER(11))
    income_0_to_5_cnt = Column(INTEGER(11))
    income_5_to_10_cnt = Column(INTEGER(11))
    income_10_to_30_cnt = Column(INTEGER(11))
    income_30_to_50_cnt = Column(INTEGER(11))
    income_50_to_100_cnt = Column(INTEGER(11))
    income_100_to_200_cnt = Column(INTEGER(11))
    income_above_200_cnt = Column(INTEGER(11))
    balance_0_to_5_day = Column(INTEGER(11))
    balance_5_to_10_day = Column(INTEGER(11))
    balance_10_to_30_day = Column(INTEGER(11))
    balance_30_to_50_day = Column(INTEGER(11))
    balance_50_to_100_day = Column(INTEGER(11))
    balance_100_to_200_day = Column(INTEGER(11))
    balance_above_200_day = Column(INTEGER(11))
    income_weight_max = Column(DECIMAL(16, 4))
    income_weight_min = Column(DECIMAL(16, 4))
    balance_weight_max = Column(DECIMAL(16, 4))
    balance_weight_min = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransURelatedPortrait(Base):
    __tablename__ = 'trans_u_related_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    opponent_name = Column(String(64))
    relationship = Column(String(16))
    income_cnt_order = Column(INTEGER(11))
    income_cnt = Column(INTEGER(11))
    income_amt_order = Column(INTEGER(11))
    income_amt = Column(DECIMAL(16, 4))
    income_amt_proportion = Column(DECIMAL(16, 4))
    expense_cnt_order = Column(INTEGER(11))
    expense_cnt = Column(INTEGER(11))
    expense_amt_order = Column(INTEGER(11))
    expense_amt = Column(DECIMAL(16, 4))
    expense_amt_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    opponent_account_no = Column(String(64))


class TransURemarkPortrait(Base):
    __tablename__ = 'trans_u_remark_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(32))
    report_req_no = Column(String(32))
    remark_type = Column(String(64))
    remark_income_amt_order = Column(INTEGER(11))
    remark_expense_amt_order = Column(INTEGER(11))
    remark_trans_cnt = Column(INTEGER(11))
    remark_trans_amt = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)


class TransUSummaryPortrait(Base):
    __tablename__ = 'trans_u_summary_portrait'

    id = Column(BIGINT(20), primary_key=True)
    apply_no = Column(String(255))
    report_req_no = Column(String(32))
    month = Column(String(16))
    q_1_year = Column(INTEGER(11))
    q_2_year = Column(INTEGER(11))
    q_3_year = Column(INTEGER(11))
    q_4_year = Column(INTEGER(11))
    normal_income_amt = Column(DECIMAL(16, 4))
    normal_expense_amt = Column(DECIMAL(16, 4))
    net_income_amt = Column(DECIMAL(16, 4))
    salary_cost_amt = Column(DECIMAL(16, 4))
    living_cost_amt = Column(DECIMAL(16, 4))
    tax_cost_amt = Column(DECIMAL(16, 4))
    rent_cost_amt = Column(DECIMAL(16, 4))
    insurance_cost_amt = Column(DECIMAL(16, 4))
    loan_cost_amt = Column(DECIMAL(16, 4))
    interest_amt = Column(DECIMAL(16, 4))
    balance_amt = Column(DECIMAL(16, 4))
    interest_balance_proportion = Column(DECIMAL(16, 4))
    create_time = Column(DateTime)
    update_time = Column(DateTime)
    variable_cost_amt = Column(DECIMAL(16, 4))


class TransactionBill(Base):
    __tablename__ = 'transaction_bill'

    id = Column(BIGINT(20), primary_key=True)
    channel_no = Column(String(20))
    channel_name = Column(String(64))
    channel_api_no = Column(String(20))
    expired_at = Column(DateTime)
    user_name = Column(String(64))
    phone = Column(String(15))
    id_card_no = Column(String(64))
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    bank_card_no = Column(String(25))
    jhi_type = Column(String(50))
    file_path = Column(String(255))
    card_type = Column(String(50))


class TransactionBillLine(Base):
    __tablename__ = 'transaction_bill_line'

    id = Column(BIGINT(20), primary_key=True)
    raw_data_id = Column(BIGINT(20))
    risk_subject_id = Column(BIGINT(20))
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    transaction_bill_id = Column(BIGINT(20))
    serial_no = Column(String(64))
    band_card_no = Column(String(32))
    id_card_no = Column(String(64))
    user_name = Column(String(64))
    phone = Column(String(15))
    gen_card_bank = Column(String(64))
    master_type = Column(String(50))
    currency = Column(String(50))
    transaction_time = Column(DateTime)
    payee_user_name = Column(String(32))
    payee_bank_info = Column(String(64))
    payee_bank_card = Column(String(32))
    purpose = Column(String(32))
    transaction_mode = Column(String(50))
    summary = Column(String(32))
    channel = Column(String(50))
    available_amount = Column(DECIMAL(16, 4))
    on_line_audit = Column(INTEGER(11))
    verification = Column(BIT(1))
    batch_no = Column(String(64))
    import_mode = Column(INTEGER(11))


class JhiPersistentAuditEvtDatum(Base):
    __tablename__ = 'jhi_persistent_audit_evt_data'

    event_id = Column(ForeignKey('jhi_persistent_audit_event.event_id'), primary_key=True, nullable=False, index=True)
    name = Column(String(150), primary_key=True, nullable=False)
    value = Column(String(255))

    event = relationship('JhiPersistentAuditEvent')
