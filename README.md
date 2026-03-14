## 决策引擎数据映射服务
使用python来解决数据转换到决策引擎需要的数据结构。 本项目基于flask，pandas等框架实现。
[flask大概介绍](https://www.cnblogs.com/franknihao/p/7118469.html)
[pandas exercises](https://github.com/guipsamora/pandas_exercises)


### 决策前置的输入及输出
1. 输入请求：
```json
{
  "productCode": "产品编码",
  "reqNo": "xxxx",
  "queryData": {
    "name": "",
    "idno": "",
    "phone": ""
  }
   
}

```

### Usage

1. 本地安装好git
2. 使用ssh-keygen生成好id_rsa.pub, [把里面的内容放到gitlab上的ssh key配置里](https://www.jianshu.com/p/4f5cb637eff7)
3. 运行`git clone git@192.168.1.8:transformer/pipes.git` 把项目clone到你本地
4. [本地环境安装好python3.x的环境](https://www.anaconda.com/distribution/)请选择3.x版本
5. 到你的项目目录下运行 `pip install -r requirements.txt` 安装项目依赖的包
6. python代码写到app目录下
```
.
├── Jenkinsfile             # 生产测试环境的Jenkins Job file
├── Jenkinsfile-test
├── README.md
├── build.sh
├── docker-compose.yaml      # 使用docker-compose部署
├── docs            # 放置项目相关文档
├── src             # 源代码目录
│   ├── Dockerfile
│   ├── __pycache__
│   ├── app.py         # 应用的入口
│   ├── config.py      # 应用配置文件
│   ├── exceptions.py
│   ├── logger         # 应用日志配置
│   ├── mapping       # 需要给决策系统的数据转换
│   ├── requirements.txt
│   ├── util
│   └── view          # 报告详情需要转换的代码
└── tests         # 测试目录
    ├── data
    ├── mapping
    ├── metabase
    ├── strategyone.json
    ├── td_json.py
    ├── test_05002_06001.py
    ├── test_app.py
    ├── test_logger.py
    └── view

```
   
```bash
$ docker-compose up -d --no-recreate --scale app=5
```

### 开发流程
使用gitflow分支管理策略

推荐使用pycharm进行开发


### 单元测试
1. [如何使用pycharm运行单元测试](https://blog.csdn.net/chenmozhe22/article/details/81700504)

* 使用faker和[faker2db](https://github.com/emirozer/fake2db)来构建测试数据

2. 安装python test插件

`pip3 install pytest`

### 日志管理
日志会打到logstash里去， 使用 [python-logstash-async](https://python-logstash-async.readthedocs.io/en/stable/config.html)
到src/logger目录下找到对应logging-xxx.conf进行不同环境的日志配置

日志会根据系统的环境变量'ENV'设置的值来选择对应环境的配置。
ENV可设置为 DEV，FAT，UAT，PROD

### 如何发布上线
1. 在gitlab上创建merge request，把develop分支合并到release分支。
2. 到jenkins上运行mf014-pipes-external-build， 查看console里的Successfully tagged registry.cn-shanghai.aliyuncs.com/transformer/pipes
3. 编辑docker-compose.yml文件，修改image: registry.cn-shanghai.aliyuncs.com/transformer/pipes:<docker image tag>， 替换这里的docker image tag。
4. 到docker-compose.yml所在目录下运行docker-compose up -d
5. 在成功发布上线后，把release分支合并到master分支，并且在master分支上打上对应的tag。