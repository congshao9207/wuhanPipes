
from fileparser.trans_flow.trans_z01_profile import TransProfile
from fileparser.trans_flow.trans_z02_data_standardization import TransDataStandardization
from fileparser.trans_flow.trans_z03_title_distribute import TransBasic
from fileparser.trans_flow.trans_z04_time_standardization import TransactionTime
from fileparser.trans_flow.trans_z05_amount_standardization import TransactionAmt
from fileparser.trans_flow.trans_z06_balance_standardization import TransactionBalance
from fileparser.trans_flow.trans_z07_opponent_info_standardization import OpponentInfo
from fileparser.trans_flow.trans_z08_other_info_standardization import TransactionOtherInfo
from fileparser.trans_flow.trans_z09_flow_raw_data import TransFlowRawData


file = r"..\resource\trans_flow\农商行流水.xlsx"
param = {
    'bankAccount': '32438708080074424',
    'cusName': '上海陶胜建筑材料有限公司',
    'bankName': ''
}


def test_z01():
    trans_profile = TransProfile(file, param)
    trans_profile.trans_title_check()
    # print(trans_profile.resp)
    # print(trans_profile.trans_data.shape[0])

    trans_z02 = TransDataStandardization(trans_profile.trans_data)
    trans_z02.standard()
    # print(trans_z02.trans_data.shape[0])
    trans_z03 = TransBasic(trans_z02.trans_data)
    trans_z03.match_title()
    # print(trans_z03.col_mapping)
    trans_z04 = TransactionTime(trans_z03.df, trans_z03.col_mapping, trans_profile.title_params, trans_profile.title)
    trans_z04.time_match()
    if not trans_z04.basic_status:
        print(trans_z04.resp)
        return
    trans_z04.time_interval_check()
    trans_z04.time_sequence_check()
    # print(trans_z04.resp)
    trans_z05 = TransactionAmt(trans_z04.df, trans_z03.col_mapping)
    trans_z05.amt_match()
    # print(trans_z05.resp)
    trans_z06 = TransactionBalance(trans_z05.df, trans_z03.col_mapping, trans_profile.title)
    trans_z06.balance_match()
    trans_z06.balance_sequence_check()
    # print(trans_z06.resp)
    trans_z07 = OpponentInfo(trans_z06.df, trans_z03.col_mapping)
    trans_z07.opponent_info_match()

    trans_z08 = TransactionOtherInfo(trans_z07.df, trans_z03.col_mapping)
    trans_z08.trans_info_match()
    print(trans_z08.df.columns)
    trans_z09 = TransFlowRawData(trans_profile.param, trans_profile.title_params)
    trans_z09.df = trans_z09.remove_duplicate_data(trans_z08.df)
    trans_z09.save_raw_data()
