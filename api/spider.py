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

    def doc_info(self, method):
        """
        获取方法的注释信息 前两行
        :param method:
        :return:
        """
        doc = method.__doc__.strip().split('\n')
        start_prompt = doc[0]
        end_prompt = doc[1].strip()
        return start_prompt, end_prompt

    def follow_judge(self, rest_id, cursor, count, over, return_dict, fetch, redis_name, api):
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
        current = inspect.currentframe()
        getouter = inspect.getouterframes(current, 2)
        callback = getattr(self, getouter[1][3])
        start_prompt, end_prompt = self.doc_info(callback)

        rest_id = self.user_id if rest_id is None else rest_id
        self.user_id = rest_id

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
        callback(cursor=cursor)

    def user_judge(self, rest_id, cursor, count, over, return_dict, fetch, redis_name, api):
        """获取推主动态的方法"""
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        callback = getattr(self, calframe[1][3])
        start_prompt, end_prompt = self.doc_info(callback)

        rest_id = self.user_id if rest_id is None else rest_id
        self.user_id = rest_id

        resp = api(rest_id, cursor, count, return_dict, fetch)

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
        callback(cursor=cursor)

    def getOne_Following(self, rest_id=None, cursor=None, count=20, over=True, return_dict=True, fetch=True):
        """
        获取单个用户所有正在关注信息
        默认获取自己
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """

        self.follow_judge(rest_id=rest_id, cursor=cursor, count=count, over=over, return_dict=return_dict,
                          fetch=fetch, redis_name=self.spider_redis.redis_Following, api=self.API_Following)

    def getOne_Followers(self, rest_id=None, cursor=None, count=20, over=True, return_dict=True, fetch=True):
        """
        获取单个人所有关注者信息
        默认获取自己
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        :return:
        """
        self.follow_judge(rest_id=rest_id, cursor=cursor, count=count, over=over, return_dict=return_dict,
                          fetch=fetch, redis_name=self.spider_redis.redis_Follower, api=self.API_Followers)

    def getOne_FollowersYouKnow(self, rest_id=None, cursor=None, count=20, over=True, return_dict=True, fetch=True):
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
        self.follow_judge(rest_id=rest_id, cursor=cursor, count=count, over=over, return_dict=return_dict,
                          fetch=fetch, redis_name=self.spider_redis.redis_FollowersYouKnow,
                          api=self.API_FollowersYouKnow)

    def getOne_UserMedia(self, rest_id=None, cursor=None, count=20, return_dict=True, fetch=True, over=True):
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
        self.user_judge(rest_id, cursor, count, over, return_dict, fetch, redis_name=self.spider_redis.redis_UserMedia,
                        api=self.API_UserMedia)

    def getOne_Likes(self, rest_id=None, cursor=None, count=20, return_dict=True, fetch=True, over=True):
        """
        获取单个推特主所有喜欢
        获取该用户的喜欢信息
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """
        self.user_judge(rest_id, cursor, count, over, return_dict, fetch, redis_name=self.spider_redis.redis_UserLikes,
                        api=self.API_UserLikes)

    def getOne_UserTweets(self, rest_id=None, cursor=None, count=20, return_dict=True, fetch=True, over=True):
        """
        获取单个用户所有推文信息
        获取该用户的推文信息
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param over: 是否覆盖保存的数据
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """
        self.user_judge(rest_id, cursor, count, over, return_dict, fetch, redis_name=self.spider_redis.redis_UserTweets,
                        api=self.API_UserTweets)


if __name__ == '__main__':
    t = SpiderAPI()
    # t.getOne_Following()
    # t.getOne_Followers()
    # t.getOne_FollowersYouKnow()

    # t.getOne_UserMedia()
    # t.getOne_Likes()
    # t.getOne_UserTweets()
