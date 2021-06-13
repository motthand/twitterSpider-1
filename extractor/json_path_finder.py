#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: json_path_finder.py
@Ide: PyCharm
@Time: 2021-05-29 22:18:43
@Desc: 
"""
import json
from typing import List


class JsonPathFinder:

    def __init__(self, json_str, mode='key'):
        if isinstance(json_str, dict) or isinstance(json_str, list):
            self.data = json_str
        elif isinstance(json_str, str):
            self.data = json.loads(json_str)
        else:
            raise Exception(f"数据类型错误, The type is {type(json_str)}")
        self.mode = mode

    def iter_node(self, rows, road_step, target):
        if isinstance(rows, dict):
            key_value_iter = (x for x in rows.items())
        elif isinstance(rows, list):
            key_value_iter = (x for x in enumerate(rows))
        else:
            return
        for key, value in key_value_iter:
            current_path = road_step.copy()
            current_path.append(key)
            if self.mode == 'key':
                check = key
            else:
                check = value
            if check == target:
                yield current_path
            if isinstance(value, (dict, list)):
                yield from self.iter_node(value, current_path, target)

    def find_first(self, target: str) -> list:
        """
        获取第一个路径
        :param target:
        :return:
        """
        path_iter = self.iter_node(self.data, [], target)
        for path in path_iter:
            return path
        return []

    def find_last(self, target: str) -> list:
        """
        获取最后一个路径
        :param target:
        :return:
        """
        path_iter = self.iter_node(self.data, [], target)
        if path_iter:
            new_path_iter = list(path_iter)
            if new_path_iter:
                return new_path_iter[-1]
        return []

    def find_all(self, target) -> List[list]:
        """
        获取所有路径
        :param target:
        :return:
        """
        path_iter = self.iter_node(self.data, [], target)
        return list(path_iter)


class JsonDataFinder(JsonPathFinder):

    def join_path(self, target_path):
        """
        通过路径查找数据
        :param target_path:
        :return:
        """
        if not target_path:
            return False
        if isinstance(target_path[0], list):
            new_data = list()
            for targets in target_path:
                data = self.data
                for target in targets:
                    data = data[target]
                new_data.append(data)
            if len(new_data) == 1:
                return new_data[0]
            return new_data

        data = self.data
        for target in target_path:
            data = data[target]
        return data

    def find_first_data(self, target: str):
        """
        获得第一条数据
        :param target:
        :return:
        """
        target_path = self.find_first(target)
        return self.join_path(target_path)

    def find_last_data(self, target: str):
        """
        获得第一条数据
        :param target:
        :return:
        """
        target_path = self.find_last(target)
        return self.join_path(target_path)

    def find_assign_data(self, target: str, index: int):
        """
        获取指定索引的数据
        :param target:
        :param index:
        :return:
        """
        target_path = self.find_first(target)
        target_path.append(index)
        return self.join_path(target_path)

    def find_all_data(self, target: str):
        """
        获得所有数据
        :param target:
        :return:
        """
        target_path = self.find_all(target)
        return self.join_path(target_path)

    def find_all_last_data(self, target: str):
        """
        提取target下 的父级target值 或叫做 所有单个列表中最后一个target值
        :param target:
        :return:
        """
        target_path = self.find_all(target)
        _target_path = [i for i in target_path if len(i) == 2]
        return self.join_path(_target_path)

    def find_all_same_level_data(self, target: str):
        """
        获得所有同级数据
        :param target:
        :return:
        """
        target_path = self.find_all(target)
        new_target_path = [path[:-1] for path in target_path]
        return self.join_path(new_target_path)

