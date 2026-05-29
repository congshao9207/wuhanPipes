#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2026/5/28 15:43
# @Author  : genc
# @File    : to_json.py

import json
res = {'strategyParam': {'reqNo': 'Q2059893119272517632', 'stepReqNo': 'S2059893119314460672', 'productCode': '08001', 'queryData': [{'id': 323, 'parentId': 0, 'name': '蒙商流水测试2', 'idno': '123313', 'userType': 'PERSONAL', 'relation': 'MAIN', 'extraParam': {'accounts': [{'bankAccount': 'wxid_k4jsg6kbycvm22', 'bankName': '微信支付'}], 'fileIds': [1121], 'industryName': '', 'isMain': '0', 'phantomRelation': False, 'priority': 2147483647.0, 'fileInfo': [{'bankAccount': 'wxid_k4jsg6kbycvm22', 'bankName': '微信支付', 'compareResult': 0, 'contentId': 'trans_file/SER2059197905314676736_微信支付交易明细证明(20250312-20260312)——【解压密码可在微信支付公众号查看】.pdf', 'fileName': '微信支付交易明细证明(20250312-20260312)——【解压密码可在微信支付公众号查看】.pdf', 'ownerName': '迟守飞', 'uploadDate': ''}]}, 'authorStatus': 'AUTHORIZED', 'fundratio': 0, 'applyAmo': 0}], 'preReportReqNo': 'PR2059893119100551168', 'versionNo': '1.0'}, 'strategyResult': None}

req = {'reqNo': 'Q2059945176469766144', 'stepReqNo': 'S2059945176578818048', 'productCode': '08001', 'queryData': [{'id': 326, 'parentId': 0, 'name': '蒙商流水测试2', 'idno': '123313', 'userType': 'PERSONAL', 'relation': 'MAIN', 'extraParam': {'accounts': [{'bankAccount': 'wxid_k4jsg6kbycvm22', 'bankName': '微信支付'}], 'fileIds': [1121], 'industryName': '', 'isMain': '0', 'phantomRelation': False, 'priority': 2147483647.0, 'fileInfo': [{'bankName': '微信支付', 'bankAccount': 'wxid_k4jsg6kbycvm22', 'fileName': '微信支付交易明细证明(20250312-20260312)——【解压密码可在微信支付公众号查看】.pdf', 'contentId': 'trans_file/SER2059197905314676736_微信支付交易明细证明(20250312-20260312)——【解压密码可在微信支付公众号查看】.pdf', 'uploadDate': '', 'ownerName': '迟守飞', 'fileUrl': None, 'compareResult': 0}]}, 'authorStatus': 'AUTHORIZED', 'fundratio': 0, 'applyAmo': 0}], 'preReportReqNo': 'PR2059945175945478144', 'versionNo': '1.0'}

print(json.dumps(req, indent=4, sort_keys=True))
