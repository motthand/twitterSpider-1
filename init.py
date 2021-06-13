#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: init.py
@Ide: PyCharm
@Time: 2021-06-08 17:55:36
@Desc: 加载并读取配置文件数据
"""
from datetime import datetime

from config import global_config
from utils.exception import TSException


class Init(object):
    # 项目启动时间
    obj_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class ConfigAnalysis(Init):

    def __init__(self):
        self.rest_id_list = list()

        self.username, self.password = self.load_login()
        self.download_path = self.load_download()
        self.m3u8_ts_path = self.load_m3u8()

        _fetch_info = self.load_fetch()
        self.retry_max_number = _fetch_info.get('retry_max_number')
        self.retry_min_random_wait = _fetch_info.get('retry_min_random_wait')
        self.retry_max_random_wait = _fetch_info.get('retry_max_random_wait')
        self.fetch_timeout = _fetch_info.get('fetch_timeout')
        self.proxies = _fetch_info.get('proxies')
        self.User_Agent = _fetch_info.get('User_Agent')

        _redis_info = self.load_redis()
        self.redis_pass = _redis_info.get('redis_pass')
        self.redis_host = _redis_info.get('redis_host')
        self.redis_port = _redis_info.get('redis_port')
        self.db = _redis_info.get('db')

        _spider_info = self.load_spider()
        self.level = _spider_info.get('level')
        self.thread = _spider_info.get('thread')
        self.over = _spider_info.get('over')
        self.count = _spider_info.get('count')
        self.thread = _spider_info.get('thread')
        self.spider_wait = _spider_info.get('spider_wait')

        _email_info = self.load_email()
        self.host_server = _email_info.get('host_server')
        self.sender_qq = _email_info.get('sender_qq')
        self.pwd = _email_info.get('pwd')
        self.receiver = _email_info.get('receiver')

    def isdigit(self, parameter_name, parameter):
        if not parameter.isdigit():
            raise TSException(f"Error: [{parameter_name}] 配置信息参数错误，请输入数字，你输入的是：{parameter}")
        return int(parameter)

    def istitle(self, parameter_name, parameter):
        if not parameter.istitle():
            raise TSException(f"Error: [{parameter_name}] 配置信息参数错误，请输入True或False，你输入的是：{parameter}")
        return eval(parameter)

    @staticmethod
    def load_login():
        username = global_config.getRaw('config', 'username')
        password = global_config.getRaw('config', 'password')
        return username, password

    @staticmethod
    def load_download():
        download_path = global_config.getRaw('download', 'download_path')
        return download_path

    @staticmethod
    def load_m3u8():
        m3u8_ts_path = global_config.getRaw('download', 'm3u8_ts_path')
        return m3u8_ts_path

    @staticmethod
    def load_fetch():
        # retrying
        retry_max_number = global_config.getRaw('request', 'retry_max_number')
        retry_min_random_wait = global_config.getRaw('request', 'retry_min_random_wait')
        retry_max_random_wait = global_config.getRaw('request', 'retry_max_random_wait')
        # http
        fetch_timeout = global_config.getRaw('request', 'fetch_timeout')
        # VPN代理
        proxies = global_config.getDict([('request', 'http'), ('request', 'https')])
        User_Agent = global_config.getDict([('request', 'User-Agent')])
        return_info = {
            "retry_max_number": retry_max_number,
            "retry_min_random_wait": retry_min_random_wait,
            "retry_max_random_wait": retry_max_random_wait,
            "fetch_timeout": fetch_timeout,
            "proxies": proxies,
            "User_Agent": User_Agent,
        }
        return return_info

    @staticmethod
    def load_redis():
        redis_pass = global_config.getRaw('redis', 'redis_pass')
        if redis_pass == 'None':
            redis_pass = eval(redis_pass)
        redis_host = global_config.getRaw('redis', 'redis_host')
        redis_port = global_config.getRaw('redis', 'redis_port')
        db = global_config.getRaw('redis', 'DB')
        return_info = {
            "redis_pass": redis_pass,
            "redis_host": redis_host,
            "redis_port": redis_port,
            "db": db,
        }
        return return_info

    def load_spider(self):
        from multiprocessing import cpu_count
        level = global_config.getRaw('spider', 'level').strip()
        thread = global_config.getRaw('spider', 'thread').strip()
        over = global_config.getRaw('spider', 'over').strip()
        count = global_config.getRaw('spider', 'count').strip()
        spider_wait = global_config.getRaw('spider', 'spider_wait').strip()

        spider_wait = self.isdigit('spider_wait', spider_wait) if level else 1
        level = self.isdigit('level', level) if level else 1
        thread = self.isdigit('thread', thread) if thread else 2
        thread = cpu_count() if thread > cpu_count() else thread
        over = self.istitle('over', over) if over else True
        count = self.isdigit('count', count) if count else 20
        return_info = {"level": level, "thread": thread, "over": over, "count": count, "spider_wait": spider_wait}
        return return_info

    def load_email(self):
        host_server = global_config.getRaw('email', 'host_server').strip()
        sender_qq = global_config.getRaw('email', 'sender_qq').strip()
        pwd = global_config.getRaw('email', 'pwd').strip()
        receiver = global_config.getRaw('email', 'receiver').strip()
        return_info = {"host_server": host_server, "sender_qq": sender_qq, "pwd": pwd, "receiver": receiver}
        return return_info
