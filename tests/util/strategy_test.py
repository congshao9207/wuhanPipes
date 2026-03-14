import requests
from jsonpath import jsonpath

from strategy_config import obtain_strategy_url


def test_to_strategy():
    # 调用决策引擎
    strategy_response = requests.post(obtain_strategy_url(), json={'StrategyOneRequest': {'Header': {'InquiryCode': 'Q360763137919713280', 'ProcessCode': 'Level1_m'}, 'Body': {'Application': {'Variables': {'out_strategyBranch': '16002,24001,f0004,f0005', 'court_ent_admi_vio': 0, 'court_ent_judge': 16, 'court_ent_trial_proc': 33, 'court_ent_tax_pay': 0, 'court_ent_owed_owe': 0, 'court_ent_tax_arrears': 0, 'court_ent_dishonesty': 0, 'court_ent_limit_entry': 0, 'court_ent_high_cons': 0, 'court_ent_pub_info': 0, 'court_ent_cri_sus': 0, 'court_ent_tax_arrears_amt_3y': 0, 'court_ent_pub_info_amt_3y': 0, 'court_ent_admi_vio_amt_3y': 0, 'court_ent_judge_amt_3y': 3978444.65, 'court_ent_docu_status': 2, 'court_ent_proc_status': 2, 'court_ent_fin_loan_con': 0, 'court_ent_loan_con': 0, 'court_ent_pop_loan': 0, 'court_ent_pub_info_max': 0, 'court_ent_judge_max': 1538321.0, 'court_ent_tax_arrears_max': 0, 'court_ent_admi_violation_max': 0, 'com_bus_status': 1, 'com_bus_endtime': '2045-09-22', 'com_bus_relent_revoke': 0, 'com_bus_case_info': 0, 'com_bus_shares_frost': 0, 'com_bus_shares_frost_his': 0, 'com_bus_shares_impawn': 0, 'com_bus_shares_impawn_his': 0, 'com_bus_mor_detail': 0, 'com_bus_mor_detail_his': 0, 'com_bus_liquidation': 0, 'com_bus_exception': 0, 'com_bus_exception_his': 0, 'com_bus_illegal_list': 0, 'com_bus_illegal_list_his': 0, 'com_bus_registered_capital': 12985.14, 'com_bus_openfrom': '2015-09-23', 'com_bus_ent_type': '有限责任公司（自然人投资或控股）', 'com_bus_esdate': '2015-09-23', 'com_bus_industryphycode': 'J', 'com_bus_areacode': '310115', 'com_bus_industrycode': '6940', 'com_bus_saicChanLegal_5y': 1, 'com_bus_saicChanInvestor_5y': 4, 'com_bus_saicChanRegister_5y': 3, 'com_bus_saicAffiliated': 3, 'com_bus_province': '上海', 'com_bus_city': '上海市', 'com_bus_leg_not_shh': 0, 'com_bus_exception_result': 0, 'com_bus_saicChanRunscope': 0, 'com_bus_legper_relent_revoke': 0, 'com_bus_legper_outwardCount1': 7, 'com_bus_industryphyname': '金融业', 'com_bus_court_open_admi_violation': 2, 'com_bus_court_open_judge_docu': 33, 'com_bus_court_open_judge_proc': 35, 'com_bus_court_open_tax_pay': 0, 'com_bus_court_open_owed_owe': 0, 'com_bus_court_open_tax_arrears': 0, 'com_bus_court_open_court_dishonesty': 0, 'com_bus_court_open_rest_entry': 0, 'com_bus_court_open_high_cons': 0, 'com_bus_court_open_pub_info': 0, 'com_bus_court_open_cri_sus': 0, 'com_bus_court_open_fin_loan_con': 0, 'com_bus_court_open_loan_con': 0, 'com_bus_court_open_pop_loan': 0, 'com_bus_court_open_pub_info_max': 0, 'com_bus_court_open_judge_max': 650259.0, 'com_bus_court_open_tax_arrears_max': 0, 'com_bus_court_open_admi_violation_max': 0.0, 'com_bus_court_open_docu_status': 2, 'com_bus_court_open_proc_status': 2, 'com_bus_face_outwardindusCode1': 'I,O', 'com_bus_face_outwardindusCount1': 2, 'base_date': '2019-08-19', 'base_idno': '91310115358479298M', 'base_gender': 0, 'base_age': 0, 'base_black': 0, 'base_type': 'COMPANY'}}}}})
    strategy_resp = strategy_response.json()
    error = jsonpath(strategy_resp, '$..Error')
    print(strategy_resp)
    print(error)


def test_to_strategy_second():
    # 调用决策引擎
    strategy_response = requests.post(obtain_strategy_url(), json={
    "StrategyOneRequest": {
        "Header": {
            "InquiryCode": "Q368759021009797120",
            "ProcessCode": "Level1_m"
        },
        "Body": {
            "Application": {
                "Variables": {
                    "score_black_a1": 100,
                    "score_credit_a1": 0,
                    "score_debit_a1": 0,
                    "score_fraud_a1": 0,
                    "score_a1": 100,
                    "score_black_a2": 0,
                    "score_credit_a2": 0,
                    "score_debit_a2": 0,
                    "score_fraud_a2": 0,
                    "score_a2": 18,
                    "score_black_c1": 0,
                    "score_business_c1": 10,
                    "score_c1": 56,
                    "u_industryphycode": 1,
                    "base_type": "UNION"
                }
            }
        }
    }
})
    strategy_resp = strategy_response.json()
    error = jsonpath(strategy_resp, '$..Error')
    print(strategy_resp)
    print(error)
