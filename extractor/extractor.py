#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: extractor.py
@Ide: PyCharm
@Time: 2021-06-01 18:38:01
@Desc: 
"""
from extractor.json_path_finder import JsonDataFinder


class ExtractorApi(object):
    """
    数据提取器接口
    """

    @staticmethod
    def find_exists(resp, target: str) -> bool:
        """
        检查是否存在
        :return:
        """
        finder = JsonDataFinder(resp)
        exists = finder.find_first(target)
        if exists:
            return True
        return False

    @staticmethod
    def find_all_data(resp, target: str):
        """
        提取target下所有数据
        :param resp:
        :param target:
        :return:
        """
        finder = JsonDataFinder(resp)
        target_list = finder.find_all_data(target)
        if not target_list:
            return None
        if not isinstance(target_list, list):
            target_list = [target_list]
        return target_list

    @staticmethod
    def find_all_last_data(resp, target: str):
        """
        提取target下 的父级target值 或叫做 所有单个列表中最后一个target值
        :param resp:
        :param target:
        :return:
        """
        finder = JsonDataFinder(resp)
        target_list = finder.find_all_last_data(target)
        return target_list

    @staticmethod
    def find_first_data(resp, target: str):
        """
        提取target下第一个数据
        :param resp:
        :param target:
        :return:
        """
        finder = JsonDataFinder(resp)
        first_target = finder.find_first_data(target)
        return first_target

    @staticmethod
    def find_last_data(resp, target: str):
        """
        提取target下第最后一个数据
        :param resp:
        :param target:
        :return:
        """
        finder = JsonDataFinder(resp)
        first_target = finder.find_last_data(target)
        return first_target

    @staticmethod
    def find_assign_data(resp, target: str, index: int):
        """
        提取target下第index个数据
        :param resp:
        :param target:
        :param index:
        :return:
        """
        finder = JsonDataFinder(resp)
        assign_target = finder.find_assign_data(target, index)
        return assign_target

    @staticmethod
    def find_all_same_level_data(resp, target: str):
        finder = JsonDataFinder(resp)
        target_list = finder.find_all_same_level_data(target)
        return target_list

    @staticmethod
    def find_effective_data(resp, target: str):
        """
        获取有效数据
        推特有效数据默认排除最后两个
        :param resp:
        :param target:
        :return:
        """
        finder = JsonDataFinder(resp)
        target_list = finder.find_all_data(target)
        return target_list[:-2]
