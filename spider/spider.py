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
from api.spider import SpiderAPI
from utils.logger import logger


class TwitterSpider(SpiderAPI):

    def iter_get_all(self, name):
        """
        分批获取redis所有数据
        :param name:
        :return:
        """
        for item in self.redis.hscan_iter(name=name):
            yield item

    def getMyFollowing(self, rest_id=None):
        """获取某人的 正在关注"""

        # 获取关注的人
        # self.getOne_Following()

        # 读取关注的人
        for user_id, item in self.iter_get_all(name=self.spider_redis.redis_Following):
            name = self.find_first_data(item, 'name')
            screen_name = self.find_first_data(item, 'screen_name')
            logger.info(f"开始获取【{user_id}——{screen_name}——{name}】的信息")
            self.getOne_UserMedia(user_id)

    def getAll_Following(self):
        "获取所有用户 正在关注"
        pass

    def getAll_Followers(self):
        """获取所有用户 关注"""
        pass

    def getAll_FollowersYouKnow(self):
        """获取所有用户 可能认识的人"""
        pass

    def getAll_UserMedia(self):
        """获取所有用户 媒体"""
        pass

    def getAll_Likes(self):
        """获取所有用户 喜欢"""
        pass

    def getAll_UserTweets(self):
        """获取所有用户 推文"""
        pass


if __name__ == '__main__':
    t = TwitterSpider()
    # t.getOne_Following()
    # t.getOne_Followers()
    # t.getOne_FollowersYouKnow()

    # t.getOne_UserMedia('1126756908193308672')
    t.getOne_Likes('1126756908193308672')
    t.getOne_UserTweets('1126756908193308672')
    # t.getMyFollowing()
