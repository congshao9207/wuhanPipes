import os

DEBUG = True

version_info = "v1.3"
app_env = os.getenv("ENV", 'dev').lower()

# 配置使用的决策引擎
# 获取环境变量，如果没有返回'http://192.168.1.20:8091/rest/S1Public'
STRATEGY_URL = os.getenv('STRATEGY_URL',
                         'http://192.168.1.20:8091/rest/S1Public')

OPEN_STRATEGY_URL = os.getenv('OPEN_STRATEGY_URL', "http://192.168.1.36:8088/rest/S1Public")

EUREKA_SERVER = os.getenv('EUREKA_SERVER', 'http://192.168.1.27:8030/eureka/')


"""
通过环境变量配置不同部署环境的是数据库
"""

"""
GEARS_DB = {
    'user': os.getenv('DB_USER', 'gears_test'),
    'pw': os.getenv('DB_PW', 'bd3by68u'),
    'host': os.getenv('DB_HOST', '192.168.1.9'),
    'port': os.getenv('DB_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_test'),
}
"""

"""
GEARS_DB = {
    'user': os.getenv('DB_USER', 'gears_test'),
    'pw': os.getenv('DB_PW', 'bd3by68u'),
    'host': os.getenv('DB_HOST', '192.168.1.9'),
    'port': os.getenv('DB_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_test'),
}
"""

"""
GEARS_DB = {
    'user': os.getenv('DB_USER', 'gears_dev'),
    'pw': os.getenv('DB_PW', 'x2cqpau4'),
    'host': os.getenv('DB_HOST', '192.168.1.9'),
    'port': os.getenv('DB_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_dev'),
}
"""

"""
GEARS_DB = {
    'user': os.getenv('DB_USER', 'gears_test'),
    'pw': os.getenv('DB_PW', 'bd3by68u'),
    'host': os.getenv('DB_HOST', '192.168.1.9'),
    'port': os.getenv('DB_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_test'),
}
"""

"""
GEARS_DB = {
 'user': os.getenv('DB_USER', 'gears_external'),
 'pw': os.getenv('DB_PW', 'j83eckas'),
 'host': os.getenv('DB_HOST', '192.168.1.9'),
 'port': os.getenv('DB_PORT', 3360),
 'db': os.getenv('DB_NAME', 'gears_external'),
}
"""



#
# GEARS_DB = {
#     'user': os.getenv('DB_USER', 'pd_query'),
#     'pw': os.getenv('DB_PW', '8dytjn3s'),
#     'host': os.getenv('DB_HOST', 'trans.magfin.cn'),
#     'port': os.getenv('DB_PORT', 6690),
#     'db': os.getenv('DB_NAME', 'gears'),
# }

# GEARS_DB = {
#     'user': os.getenv('DB_USER', 'root'),
#     'pw': os.getenv('DB_PW', ''),
#     'host': os.getenv('DB_HOST', '192.168.2.26'),
#     'port': os.getenv('DB_PORT', 3306),
#     'db': os.getenv('DB_NAME', 'wuhan_taia'),
# }


GEARS_DB = {
    'user': os.getenv('DB_USER', 'SYSDBA'),
    'pw': os.getenv('DB_PW', 'SYSDBA001'),
    'host': os.getenv('DB_HOST', '192.168.2.29'),
    'port': os.getenv('DB_PORT', 30237),
    'db': os.getenv('DB_NAME', 'wuhan_taia'),
}

