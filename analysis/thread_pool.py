#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: thread_pool.py
@Ide: PyCharm
@Time: 2021-06-06 19:26:04
@Desc: 
"""
from analysis.thread import DataExtractOneThread
from utils.utils import thread_pool


class DataExtractPool(DataExtractOneThread):

    def load_data(self, name, new_name):
        redis_data = [[rest_id, item, new_name, {'over': True}] for rest_id, item in list(self.iter_get_all(name))]
        return redis_data

    def _run(self, name, new_name, prompt, method, **kwargs):
        redis_data = self.load_data(name, new_name)
        thread_num = kwargs.get('thread_num', 4)
        thread_pool(method=method, data=redis_data, prompt=prompt, thread_num=thread_num)

    def get_UserInfoPool(self, name, new_name, **kwargs):
        prompt = f'提取推特用户信息->[from:{name}]->[save::{new_name}]'
        method = self._UserInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method, **kwargs)

    def get_UserDataInfoPool(self, name, new_name, **kwargs):
        prompt = f'提取用户数据->[from:{name}]->[save::{new_name}]'
        method = self._dataInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method, **kwargs)


    def get_DownloadInfoPool(self, name, new_name, **kwargs):
        prompt = f'初始化下载信息->[from:{name}]->[save::{new_name}]'
        method = self._downloadInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method, **kwargs)




