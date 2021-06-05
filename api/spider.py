#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: spider.py
@Ide: PyCharm
@Time: 2021-06-01 16:56:01
@Desc:
"""
import inspect

from api.twitter import TwitterApi
from extractor.extractor import ExtractorApi
from utils.exception import TSException
from utils.logger import logger
from utils.twitter_redis import SpiderRedis


class SpiderAPI(TwitterApi, ExtractorApi):
    """
    抓取接口所有数据
    """

    def __init__(self):
        super(SpiderAPI, self).__init__()
        self.spider_redis = SpiderRedis()
        self.redis = self.spider_redis.redis
        self.num = 1

    @staticmethod
    def doc_info(method):
        """
        获取方法的注释信息 前两行
        :param method:
        :return:
        """
        doc = method.__doc__.strip().split('\n')
        start_prompt = doc[0]
        end_prompt = doc[1].strip()
        return start_prompt, end_prompt

    def init_params(self, **kwargs):
        count = kwargs.get('count', 20)
        over = kwargs.get('over', True)
        return_dict = kwargs.get('return_dict', True)
        fetch = kwargs.get('fetch', True)
        if not isinstance(over, bool):
            raise TSException(f"类型错误应该为bool:{type(over)}")
        if not isinstance(return_dict, bool):
            raise TSException(f"类型错误应该为bool:{type(return_dict)}")
        if not isinstance(fetch, bool):
            raise TSException(f"类型错误应该为bool:{type(fetch)}")
        return count, over, return_dict, fetch

    def get_function(self):
        """
        获取被指定调用的方法
        """
        current = inspect.currentframe()
        get_outer = inspect.getouterframes(current, 2)
        function = getattr(self, get_outer[2][3])
        return function

    def follow_judge(self, rest_id, cursor, redis_name, api, **kwargs):
        """
        获取推特用户信息的方法
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :param redis_name: redis数据名
        :param api: 需要使用的API接口
        :return:
        """
        rest_id = self.user_id if rest_id is None else rest_id
        self.user_id = rest_id
        count, over, return_dict, fetch = self.init_params(**kwargs)

        function = self.get_function()
        start_prompt, end_prompt = self.doc_info(function)

        # 获取数据
        resp = api(rest_id=rest_id, cursor=cursor, count=count, return_dict=return_dict, fetch=fetch)

        # 提取信息
        itemContents = self.find_all_data(resp, 'itemContent')

        # 检查是否提取完毕
        if not itemContents:
            self.num = 1
            return

        for n, itemContent in enumerate(itemContents):
            rest_id = self.find_first_data(itemContent, 'rest_id')
            name = self.find_first_data(itemContent, 'name')

            # 保存数据
            self.spider_redis.execute(redis_name=redis_name, redis_key=rest_id, data=itemContent, over=over)

            logger.info(
                f"Success {start_prompt} [{self.user_id}]: [{self.num}][{n + 1}/{len(itemContents)}] {rest_id}->{name}")
            self.num += 1

        cursor = self.find_first_data(resp, 'value')
        function(cursor=cursor)

    def user_judge(self, rest_id, cursor, redis_name, api, **kwargs):
        """
        获取推主动态的方法
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :param redis_name: redis数据名
        :param api: 需要使用的API接口
        :return:
        """
        rest_id = self.user_id if rest_id is None else rest_id
        self.user_id = rest_id
        count, over, return_dict, fetch = self.init_params(**kwargs)

        function = self.get_function()
        start_prompt, end_prompt = self.doc_info(function)

        resp = api(rest_id=rest_id, cursor=cursor, count=count, return_dict=return_dict, fetch=fetch)

        # 检查是否提取完毕
        if not self.find_exists(resp, 'tweet'):
            return

        # 获取数据来源
        entries_list = self.find_effective_data(resp, target='entries')

        # 筛选数据  可能出现无效数据
        tweet_list = self.find_all_data(entries_list, target='tweet')

        # 检查是否提取完毕
        if not tweet_list:
            return

        user_list = self.find_all_data(tweet_list, 'user')
        if not user_list:
            self.num += 1
            return

        user_rest_ids = self.find_all_data(user_list, 'rest_id')  # 推特信息原创者的rest_id
        twitter_rest_ids = self.find_all_last_data(tweet_list, 'rest_id')  # 推特rest_id
        for n, (user_rest_id, twitter_rest_id, tweet) in enumerate(zip(user_rest_ids, twitter_rest_ids, tweet_list)):
            # 保存数据
            only_key = f'{user_rest_id}_{twitter_rest_id}'
            self.spider_redis.execute(redis_name=redis_name, redis_key=only_key, data=tweet, over=over)

            logger.info(f"Success {end_prompt} [{self.user_id}]: [{self.num}][{n + 1}/{len(user_rest_ids)}] {only_key}")
            self.num += 1

        cursor = self.find_last_data(resp, 'value')
        function(cursor=cursor)

    def getOne_Following(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单个用户所有正在关注信息
        默认获取自己
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param kwargs count: 数量
        :param kwargs over: 是否覆盖保存的数据
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        redis_name = self.spider_redis.redis_Following
        api = self.API_Following
        self.follow_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def getOne_Followers(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单个人所有关注者信息
        默认获取自己
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param kwargs count: 数量
        :param kwargs over: 是否覆盖保存的数据
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        :return:
        """
        redis_name = self.spider_redis.redis_Follower
        api = self.API_Followers
        self.follow_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def getOne_FollowersYouKnow(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单个人认识的推特主
        默认获取自己
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """
        redis_name = self.spider_redis.redis_FollowersYouKnow
        api = self.API_FollowersYouKnow
        self.follow_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def getOne_UserMedia(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单人所有媒体
        获取该用户的媒体信息
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """
        redis_name = self.spider_redis.redis_UserMedia
        api = self.API_UserMedia
        self.user_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def getOne_Likes(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单个推特主所有喜欢
        获取该用户的喜欢信息
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param kwargs count: 数量
        :param kwargs over: 是否覆盖保存的数据
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        redis_name = self.spider_redis.redis_UserLikes
        api = self.API_UserLikes
        self.user_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def getOne_UserTweets(self, rest_id=None, cursor=None, **kwargs):
        """
        获取单个用户所有推文信息
        获取该用户的推文信息
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param kwargs count: 数量
        :param kwargs over: 是否覆盖保存的数据
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        redis_name = self.spider_redis.redis_UserTweets
        api = self.API_UserTweets
        self.user_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)


if __name__ == '__main__':
    t = SpiderAPI()
    t.getOne_Following()
    # t.getOne_Followers()
    # t.getOne_FollowersYouKnow()
    # t.getOne_UserMedia()
    # t.getOne_Likes()
    # t.getOne_UserTweets()
