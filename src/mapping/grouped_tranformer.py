# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod

from mapping.tranformer import Transformer

invoke_each = 1
invoke_union = 2


class GroupedTransformer(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        super().__init__()
        self.full_msg = None

    @abstractmethod
    def invoke_style(self) -> int:
        return 0

    @abstractmethod
    def group_name(self):
        pass

    def each_invoke(self):
        return self.invoke_style() & invoke_each > 0

    def union_invoke(self):
        return self.invoke_style() & invoke_union > 0
