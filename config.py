#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: config.py
@Ide: PyCharm
@Time: 2021-05-29 12:04:54
@Desc: 
"""
import os
import configparser
from typing import List


class Config(object):
    def __init__(self, config_file='config.ini'):
        self._path = os.path.join(os.path.dirname(__file__), config_file)
        if not os.path.exists(self._path):
            raise FileNotFoundError("No such file: config.ini")

        self._config = configparser.ConfigParser()
        self._config.read(self._path, encoding='utf-8-sig')

        self._configRaw = configparser.RawConfigParser()
        self._configRaw.read(self._path, encoding='utf-8-sig')

    def get(self, section, name):
        return self._config.get(section, name)

    def getRaw(self, section, name):
        return self._configRaw.get(section, name)

    def getDict(self, section_names: List[tuple]):
        item = dict()
        for section_name in section_names:
            section = section_name[0]
            name = section_name[1]
            value = self.getRaw(section, name)
            item[name] = value
        return item


global_config = Config()
