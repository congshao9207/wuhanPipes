from util import mysql_reader


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)
