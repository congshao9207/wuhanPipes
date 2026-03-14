from src.mapping.t17001 import T17001


def test_t17001():
    ps1 = T17001()
    ps1.run(user_name='农秀英', id_card_no='610823195505053226', phone='13430955819')
    # assert ps1.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
    #                          'net_tel_risk_l': 1, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
    #                          'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 1, 'net_idno_veh': 0,
    #                          'net_idno_risk_m': 0, 'net_idno_risk_l': 1, 'net_idno_debt': 0, 'net_idno_tax': 0,
    #                          'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
    #                          'net_risk_tel_hit_high_att': 1, 'net_risk_idc_hit_ovdu': 0,
    #                          'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
    #                          'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
    #                          'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
    #                          'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
    #                          'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
    #                          'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
    #                          'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
    #                          'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
    #                          'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
    #                          'net_apply_7d': 2, 'net_apply_1m': 3, 'net_apply_3m': 5, 'net_apply_6m': 5,
    #                          'net_apply_12m': 6, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
    #                          'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
    #                          'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 97}
    #
    # ps2 = T17001()
    # ps2.run(user_name='何衣一', id_card_no='420281198612291050', phone='18953608106')
    # assert ps2.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
    #                          'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
    #                          'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
    #                          'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
    #                          'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
    #                          'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
    #                          'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
    #                          'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
    #                          'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
    #                          'net_bah_7d_tel_app': 2, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
    #                          'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
    #                          'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 5, 'net_bah_1m_idc_rel_dev': 0,
    #                          'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
    #                          'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
    #                          'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 5,
    #                          'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
    #                          'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
    #                          'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
    #                          'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 25}
    #
    # ps3 = T17001()
    # ps3.run(user_name='余叶伟', id_card_no='341122198212210210', phone='18321289099')
    # assert ps3.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
    #                          'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
    #                          'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
    #                          'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
    #                          'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
    #                          'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
    #                          'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
    #                          'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
    #                          'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
    #                          'net_bah_7d_tel_app': 40, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
    #                          'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
    #                          'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
    #                          'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
    #                          'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
    #                          'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 4,
    #                          'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
    #                          'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
    #                          'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
    #                          'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 17}
