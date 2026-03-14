from mapping.t08001 import T08001


def test_risk_anti_fraud():
    ps = T08001()
    # mock_df = pd.DataFrame({
    #     'match_blacklist': [False],
    #     'match_crank_call': [False],
    #     'match_fraud': [False],
    #     'match_empty_number': [False],
    #     'match_verification_mobile': [False],
    #     'match_small_no': [False],
    #     'match_sz_no': [False]
    # })
    # ps._info_risk_anti_fraud(df=mock_df)
    #
    # assert ps.variables['fraudinfo_isMachdBlMakt'] == 0
    # assert ps.variables['fraudinfo_isMachCraCall'] == 0
    # assert ps.variables['fraudinfo_isMachFraud'] == 0
    # assert ps.variables['fraudinfo_isMachEmpty'] == 0
    # assert ps.variables['fraudinfo_isMachYZmobile'] == 0
    # assert ps.variables['fraudinfo_isMachSmallNo'] == 0
    # assert ps.variables['fraudinfo_isMachSZNo'] == 0
    ps.run('张思诗', '420023199212152245', '15965153255')
    print(ps.variables)
