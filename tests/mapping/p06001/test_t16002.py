import numpy as np
import pandas as pd

from mapping.p06001.t16002 import T16002
from mapping.p06001.t24001 import T24001
from mapping.p06001.tf0004 import Tf0004
from mapping.utils.df_comparator_util import df_compare

params = {'user_name': '宿州市徽香源食品有限公司',
          'id_card_no': '91341302760810639A',
          'apply_date': '2019-10-01'}


def test_df_compare():
    old_df = pd.DataFrame(np.random.randint(30, 100, 5000), columns=['id'])
    new_df = old_df.copy()

    variables = {'test': 0}
    df_compare(variables, old_df, new_df, 'test')
    print(variables)


def test_set_compare():
    old_df = pd.DataFrame(np.random.randint(30, 100, 5000), columns=['id'])
    new_df = old_df.copy()

    diff = set(list(new_df['id'])).difference(list(old_df['id']))
    print(len(diff))


def test_t16002():
    ps = T16002()
    ps.run(user_name=params['user_name'], id_card_no=params['id_card_no'],
           origin_data={'preBizDate': params['apply_date']})
    print(ps.variables)


def test_t24001():
    ps = T24001()
    ps.run(user_name=params['user_name'], id_card_no=params['id_card_no'],
           origin_data={'preBizDate': params['apply_date']})
    print(ps.variables)


def test_tf0004():
    ps = Tf0004()
    ps.run(user_name=params['user_name'], id_card_no=params['id_card_no'],
           origin_data={'preBizDate': params['apply_date']})
    print(ps.variables)
