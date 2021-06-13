#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: twitter_redis.py
@Ide: PyCharm
@Time: 2021-05-30 16:07:58
@Desc: 
"""

import json

from init import ConfigAnalysis
from utils.exception import TSException


class ConnectionRedisPool(ConfigAnalysis):

    def connection(self):
        import redis
        if self.redis_pass:
            pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, password=self.redis_pass,
                                        decode_responses=True, db=self.db)
        else:
            pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, decode_responses=True, db=self.db)
        return redis.Redis(connection_pool=pool)


class BaseRedis(ConnectionRedisPool):

    def __init__(self):
        super(BaseRedis, self).__init__()
        redis_connection = ConnectionRedisPool()
        self.redis = redis_connection.connection()

    def redis_hset(self, redis_name, redis_key, data):
        value = json.dumps(data, ensure_ascii=False)
        self.redis.hset(name=redis_name, key=redis_key, value=value)

    def execute(self, redis_name, redis_key, data: dict, **kwargs):
        """
        执行
        :param redis_name:
        :param redis_key:
        :param data:
        :return:
        """
        over = kwargs.get('over', True)
        if not isinstance(over, bool):
            raise TSException(f"类型错误应该为bool:{type(over)}")
        if over:
            self.redis_hset(redis_name, redis_key, data)
        else:
            if not self.redis.hexists(name=redis_name, key=redis_key):
                self.redis_hset(redis_name, redis_key, data)


class TwitterRedis(BaseRedis):
    # rest_id 为用户id
    spider_Following = "Following"
    spider_Follower = "Follower"
    spider_FollowersYouKnow = "FollowersYouKnow"
    spider_Home = "Home"

    # rest_id 为用户id_内容id
    spider_UserMedia = "UserMedia"
    spider_UserLikes = "UserLikes"
    spider_UserTweets = "UserTweets"

    _analysis_userInfo = "analysis_userInfo"
    _analysis_Info = "analysis_dataInfo"
    _analysis_downloadInfo = "analysis_downloadInfo"

    redis_name_item = {
        "spider_analysis_info": {
            spider_Following: _analysis_userInfo,
            spider_Follower: _analysis_userInfo,
            spider_FollowersYouKnow: _analysis_userInfo,

        },
        "spider_analysis": {
            spider_UserMedia: _analysis_Info,
            spider_UserLikes: _analysis_Info,
            spider_UserTweets: _analysis_Info,
        }
    }

    # 已经爬取的信息存储位置
    spider_have = f'CacheInfo'
