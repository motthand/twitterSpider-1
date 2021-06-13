#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: login.py
@Ide: PyCharm
@Time: 2021-05-29 15:07:36
@Desc: twitterSpider login
"""
import functools
import os
import pickle

import requests
from DecryptLogin import login

from init import ConfigAnalysis
from utils.logger import logger


class TwitterLogin(ConfigAnalysis):
    is_login = False
    # 固定值
    authorization = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

    def __init__(self):
        super(TwitterLogin, self).__init__()

        self.session = requests.session()
        self.cookies_dir_path = os.path.join(os.path.dirname(__file__), "cookie")
        if not os.path.exists(self.cookies_dir_path):
            os.makedirs(self.cookies_dir_path)
        self.user_cookie_path = os.path.join(self.cookies_dir_path, f"{self.username}.cookies")

        self._login()

    def check_login(func):
        """
        用户登陆态校验装饰器。若用户未登陆，则登陆
        """

        @functools.wraps(func)
        def new_func(self, *args, **kwargs):
            if not self.is_login:
                logger.info(f"{func.__name__} 需登陆后调用，开始登陆")
                self._login()
            return func(self, *args, **kwargs)

        return new_func

    def check_cookie_file(self):

        if not os.path.exists(self.user_cookie_path):
            return False
        return True

    def save_cookies_to_local(self):
        """
        保存Cookie到本地
        :return:
        """
        with open(self.user_cookie_path, 'wb') as f:
            pickle.dump(self.session.cookies, f)

    def load_cookies_from_local(self):
        """
        从本地加载Cookie
        :return:
        """
        with open(self.user_cookie_path, 'rb') as f:
            local_cookies = pickle.load(f)
        self.session.cookies.update(local_cookies)

    def login_by_account(self):
        """
        账号登录
        :return:
        """
        lg = login.Login()
        infos_return, session, response_text = lg.twitter(self.username, self.password)
        return infos_return, session, response_text

    def login_by_cookie(self):
        """
        cookie登录
        :return:
        """
        self.load_cookies_from_local()

    def OAuth2Session(self):
        headers = {
            "authorization": self.authorization,
            "x-csrf-token": self.session.cookies.get("ct0"),
            "x-twitterSpider-active-user": 'yes',
            "x-twitterSpider-auth-type": 'OAuth2Session',
            "x-twitterSpider-client-language": 'zh-cn',
            "x-twitterSpider-polling": 'true',
        }
        self.session.headers = headers

    def _login(self):
        """
        登录
        :return:
        """
        user_cookie_path = self.check_cookie_file()
        if not user_cookie_path:
            infos_return, self.session, response_text = self.login_by_account()
            self.save_cookies_to_local()

        else:
            self.login_by_cookie()

        self.OAuth2Session()
        self.is_login = True
