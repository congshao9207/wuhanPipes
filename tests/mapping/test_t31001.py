import pandas as pd
import json
from mapping.t31001 import T31001


def test_t31001():
    ps1 = T31001()
    ps1.run("李四", "428376812384781")
    print(ps1.variables)
