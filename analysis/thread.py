#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: thread.py
@Ide: PyCharm
@Time: 2021-06-06 19:25:31
@Desc: 
"""
import tqdm

from analysis.data_analysis import DataExtract


class DataExtractOneThread(DataExtract):

    def get_UserInfo(self, name, new_name, **kwargs):
        """
        提取推特用户信息
        :param name: 读取的redis库
        :param new_name: 写入的redis库
        :return:
        """
        prompt = f'提取推特用户信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._UserInfo(data=[rest_id, item, new_name, kwargs])

    def get_UserLikesInfo(self, name, new_name, **kwargs):
        """
        提取用户喜欢信息
        """
        prompt = f'提取用户喜欢信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._dataInfo(data=[rest_id, item, new_name, kwargs])

    def get_UserMediaInfo(self, name, new_name, **kwargs):
        """
        提取用户媒体信息
        :param name:
        :param new_name:
        :param kwargs over:
        :return:
        """
        prompt = f'提取用户媒体信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._dataInfo([rest_id, item, new_name, kwargs])

    def get_DownloadInfo(self, name, new_name, **kwargs):
        prompt = f'初始化下载信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._downloadInfo([rest_id, item, new_name, kwargs])