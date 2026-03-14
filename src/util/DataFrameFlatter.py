# @Author : lixiaobo
# @File : DataFrameRotator.py 
# @Software: PyCharm
from pandas import DataFrame


class DataFrameFlatter(object):
    """
    g_name 组名
    k_name 属性字段名
    v_name 属性值字段名
    """
    def __init__(self, origin_df, g_name, k_name, v_name):
        self.origin_df = origin_df
        self.g_name = g_name
        self.k_name = k_name
        self.v_name = v_name
        self.result_df = None

    def flat_df(self):
        if not self.result_df:
            gdf = self.origin_df.groupby(by=self.g_name)
            data_lines = []
            labels = None
            for g in gdf:
                labels, item_line = self.flat_item(g[1], labels)
                data_lines.append(item_line)

            self.result_df = DataFrame(data=data_lines)
        return self.result_df

    def flat_item(self, df, fields):
        if df.empty:
            return
        row_index = -1
        data_line = {}
        if not df.empty:
            for row in df.itertuples():
                if row is None:
                    continue
                row_index = row_index + 1
                if not fields:
                    fields = row.__getattribute__("_fields") if row else {}
                else:
                    for field in fields:
                        if field and field == self.v_name:
                            continue
                        elif field == self.k_name:
                            data_line[row.__getattribute__(field)] = row.__getattribute__(self.v_name) if row else None
                        elif not field.startswith("_"):
                            data_line[field] = row.__getattribute__(field) if row else {}
        return fields, data_line

