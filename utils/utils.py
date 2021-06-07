#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: utils.py
@Ide: PyCharm
@Time: 2021-05-30 12:00:19
@Desc:
"""
import hashlib
import sys
from _md5 import md5
from concurrent import futures
from datetime import datetime
from datetime import timezone, timedelta

import tqdm


def get_md5_text(text):
    hashlib.md5(text.encode('utf8')).hexdigest()


def get_md5(data):
    md5_ = md5()
    md5_.update(data)
    return md5_.hexdigest()


def order_set_list(data: list):
    """
    列表去重 且顺序
    :param data:
    :return:
    """
    return list({}.fromkeys(data).keys())


def twitter_conversion(times: list or str):
    """
    Input ['Wed May 12 05:54:51 +0000 2021'] or 'Wed May 12 05:54:51 +0000 2021'
    Output ['2021-05-12 13:54:51'] or '2021-05-12 13:54:51'
    :param times:
    :return:
    """

    utc = timezone.utc  # 获取 UTC 的时区对象
    beijing = timezone(timedelta(hours=8))  # 北京时区

    def _twitter_conversion(ctime):
        time_obj = datetime.strptime(ctime, '%a %b %d %H:%M:%S %z %Y')
        utc_time = time_obj.replace(tzinfo=utc)  # 强制转换加上 UTC 时区
        time_beijing = utc_time.astimezone(beijing)  # 转为北京时间
        time_beijing = time_beijing.strftime('%Y-%m-%d %H:%M:%S')
        return time_beijing

    time_list = list()
    if isinstance(times, list):
        for ctime in times:
            time_beijing = _twitter_conversion(ctime)
            time_list.append(time_beijing)
    elif isinstance(times, str):
        return _twitter_conversion(times)
    else:
        return None
        # raise Exception("参数类型错误！")

    return time_list


def thread_pool(method, data, **kwargs):
    """
    多线程任务
    :param method: 函数
    :param data: 可遍历列表数据
    :param kwargs:
    :return:
    """
    if len(kwargs) == 1:
        kwargs = kwargs.get('kwargs')
    if isinstance(kwargs, dict):
        thread_num = kwargs.get('thread_num', 1)
        prompt = kwargs.get('prompt', '多线程任务->[线程数:%s]' % thread_num)
    else:
        sys.exit('kwargs is not dict')

    prompt = '多线程任务->%s->[线程数:%s]' % (prompt, thread_num)
    with futures.ThreadPoolExecutor(max_workers=thread_num) as executor:
        res = tqdm.tqdm(executor.map(method, data), total=len(data))
        res.set_description(prompt)
        return len(list(res))


def process_pool(method, data, **kwargs):
    """
    多进程任务
    :param method: 函数
    :param data: 可遍历列表数据
    :param kwargs: :return:
    """
    if len(kwargs) == 1:
        kwargs = kwargs.get('kwargs')
    if isinstance(kwargs, dict):
        thread_num = kwargs.get('thread_num', 1)
        prompt = kwargs.get('prompt', '多进程任务->[线程数:%s]' % thread_num)
    else:
        sys.exit('kwargs is not dict')

    prompt = '多进程任务->[%s]->[线程数:%s]' % (prompt, thread_num)
    with futures.ProcessPoolExecutor(max_workers=thread_num) as executor:
        res = tqdm.tqdm(executor.map(method, data), total=len(data))
        res.set_description(prompt)
        return len(list(res))
