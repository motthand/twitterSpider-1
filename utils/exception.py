#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: exception.py
@Ide: PyCharm
@Time: 2021-06-05 15:27:49
@Desc: 
"""


class TSException(Exception):

    def __init__(self, message):
        super().__init__(message)
