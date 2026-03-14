from data.data_multi_to_multi_deposit_v2 import unit_multi_deposit_v2


def test_t_f16001_case():
    _assert_union_multi_to_multi_df('f16001')


def test_t_f16002_case():
    _assert_union_multi_to_multi_df('f16002')


def test_t_f24001_case():
    _assert_union_multi_to_multi_df('f24001')


def _assert_union_multi_to_multi_df(code):
    ps = unit_multi_deposit_v2()
    ps.origin_data = {"preBizDate": "2019-10-01 13:01:01"}
    ps.product_code = "06001"
    _assert_df(ps, code)


def _assert_df(ps, code):
    df = ps.run(read_file_name="贷后报告-" + code + ".xlsx", method='t')
    result_df = df[df['是否通过'] == 'false']
    if len(result_df) > 0:
        print("测试错了：" + code)
    assert result_df.shape[0] == 0
