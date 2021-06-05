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

from config import global_config
from utils.exception import TSException


class ConnectionRedisPool(object):

    def __init__(self):
        self.redis_pass = global_config.getRaw('redis', 'redis_pass')
        if self.redis_pass == 'None':
            self.redis_pass = eval(self.redis_pass)
        self.redis_host = global_config.getRaw('redis', 'redis_host')
        self.redis_port = global_config.getRaw('redis', 'redis_port')
        self.db = global_config.getRaw('redis', 'DB')

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


class SpiderRedis(BaseRedis):

    def __init__(self):
        super(SpiderRedis, self).__init__()
        self._spider_Following = "Following"
        self._spider_Follower = "Follower"
        self._spider_FollowersYouKnow = "FollowersYouKnow"
        self._spider_UserMedia = "UserMedia"
        self._spider_UserLikes = "UserLikes"
        self._spider_UserTweets = "UserTweets"

        _analysis = 'analysis_'
        _analysis_userInfo = f"{_analysis}userInfo"

        self.redis_name_item = {
            "spider_analysis_info": {
                self._spider_Following: _analysis_userInfo,
                self._spider_Follower: _analysis_userInfo,
                self._spider_FollowersYouKnow: _analysis_userInfo,

            },
            "spider_analysis": {
                self._spider_UserMedia: _analysis + self._spider_UserMedia,
                self._spider_UserLikes: _analysis + self._spider_UserLikes,
                self._spider_UserTweets: _analysis + self._spider_UserTweets,
            }
        }


if __name__ == '__main__':
    s = SpiderRedis()
