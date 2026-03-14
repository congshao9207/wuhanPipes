# @Time : 1/25/21 9:11 AM 
# @Author : lixiaobo
# @File : async_01.py 
# @Software: PyCharm
import asyncio
import time

import aiohttp
import requests


async def main(url):
    print(f"url:{url}, begin...")
    async with aiohttp.request('GET', url) as resp:
        info = await resp.text()
        print(f"url:{url} len:{len(info)}")


def main1(url):
    print(f"url:{url}, begin...")
    response = requests.get(url)
    info = response.text
    print(f"url:{url} len:{len(info)}")


def test_page():
    urls = ["http://www.baidu.com/s?wd=" + str(x) for x in range(10)]

    start = time.time()
    tasks = [asyncio.ensure_future(main(url)) for url in urls]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))

    print("cost time:", (time.time() - start))


def test_page1():
    urls = ["http://www.baidu.com/s?wd=" + str(x) for x in range(10)]

    start = time.time()
    for url in urls:
        main1(url)

    print("cost time:", (time.time() - start))
