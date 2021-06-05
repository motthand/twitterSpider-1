#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: data_analysis.py
@Ide: PyCharm
@Time: 2021-06-01 16:57:37
@Desc: redis数据解析
"""
import json

from extractor.extractor import ExtractorApi
from extractor.json_path_finder import JsonDataFinder
from utils.logger import logger
from utils.twitter_redis import SpiderRedis
from utils.utils import twitter_conversion


class BaseAnalysis(SpiderRedis, ExtractorApi):
    def __init__(self):
        super(BaseAnalysis, self).__init__()

    def iter_get_all(self, name):
        """
        分批获取redis所有数据
        :param name:
        :return:
        """
        for item in self.redis.hscan_iter(name=name):
            yield item

    @staticmethod
    def data_to_list(data):
        """
        :param data:
        :return:
        """
        if not isinstance(data, list):
            data = [data]
        return data

    @staticmethod
    def _find_hashtags(data):
        """标签"""
        finder = JsonDataFinder(data)
        hashtags = finder.find_first_data('hashtags')
        if hashtags:
            finder = JsonDataFinder(hashtags)
            hashtags_text = finder.find_all_data('text')
            return hashtags_text

    def _get_media(self, data):
        """媒体"""
        media = self.find_first_data(data, 'media')
        if media:
            id_strs = self.data_to_list(self.find_all_data(media, 'id_str'))
            expanded_urls = self.data_to_list(self.find_all_data(media, 'expanded_url'))
            media_urls = self.data_to_list(self.find_all_data(media, 'media_url_https'))
            types = self.data_to_list(self.find_all_data(media, 'type'))
            urls = self.data_to_list(self.find_all_data(media, 'url'))
            media_keys = self.data_to_list(self.find_all_data(media, 'media_key'))
            video_infos = self.data_to_list(self.find_all_data(media, 'video_info'))

            zip_info = zip(id_strs, types, media_urls, urls, expanded_urls, media_keys, video_infos)

            info = [{
                "id_str": id_str,
                "type": _type,
                "media_url_https": media_url,
                "url": url,
                "expanded_url": expanded_url,
                "media_key": media_key,
                "video_info": video_info,
            } for id_str, _type, media_url, url, expanded_url, media_key, video_info in zip_info]

            return info

    @staticmethod
    def _get_urls(data):
        finder = JsonDataFinder(data)
        urls = finder.find_all_data('urls')
        if urls:
            finder = JsonDataFinder(urls)
            expanded_url = finder.find_all_data('expanded_url')
            if not isinstance(expanded_url, list):
                expanded_url = [expanded_url]
            return expanded_url


class BaseExtract(BaseAnalysis):

    def entities(self, data):
        entities = self.find_first_data(data, 'entities')
        if entities:
            # 标签
            hashtags_text = self._find_hashtags(entities)
            # 媒体
            media = self._get_media(entities)
            # 符号
            # symbols_list = finder.find_first_data('symbols')
            # 网址
            urls_list = self._get_urls(entities)
            # 用户提及
            # user_mentions_list = finder.find_first_data('user_mentions')

            info = {
                "hashtags_list": hashtags_text,
                "media": media,
                "urls_list": urls_list,
                # "symbols_list": symbols_list,
                # "user_mentions_list": user_mentions_list,
            }
            return info

    def extended_entities(self, data):
        """
        扩展实体
        :param data:
        :return:
        """
        extended_entities = self.find_first_data(data, 'extended_entities')
        if extended_entities:
            # 媒体
            media = self._get_media(extended_entities)
            return media

    def extract_userInfo(self, data):
        """
        提取用户信息
        数据在user下
        :return:
        """
        user_info = {
            "昵称": self.find_first_data(data, 'name'),
            "账号": self.find_first_data(data, 'screen_name'),
            "地址": self.find_first_data(data, 'location'),
            "推特注册时间": twitter_conversion(self.find_first_data(data, 'created_at')),
            "正在关注": self.find_first_data(data, 'friends_count'),
            "关注者": self.find_first_data(data, 'followers_count'),
            "媒体数": self.find_first_data(data, 'media_count'),
            "普通关注者": self.find_first_data(data, 'normal_followers_count'),
            "描述": self.find_first_data(data, 'description'),
            "个人资料横幅网址": self.find_first_data(data, 'profile_banner_url'),
            "头像": self.find_first_data(data, 'profile_image_url_https'),
            "entities": self.entities(data),
        }
        return user_info

    def extract_userMediaInfo(self, data):
        """
        提取用户媒体信息
        数据在legacy下
        :param finder:
        :return:
        """

        info = {
            "user_id_str": self.find_first_data(data, 'user_id_str'),
            "conversation_id_str": self.find_first_data(data, 'conversation_id_str'),
            "created_at": twitter_conversion(self.find_first_data(data, 'created_at')),
            "full_text": self.find_first_data(data, 'full_text'),
            "favorite_count": self.find_first_data(data, 'favorite_count'),
            "reply_count": self.find_first_data(data, 'reply_count'),
            "retweet_count": self.find_first_data(data, 'retweet_count'),
            "source": self.find_first_data(data, 'source'),
            "entities": self.entities(data),
        }
        return info

    def extract_userLikesInfo(self, data):
        """
        提取用户喜欢信息
        :return:
        """
        info = {
            "user_id_str": self.find_first_data(data, 'user_id_str'),
            "conversation_id_str": self.find_first_data(data, 'conversation_id_str'),
            "created_at": twitter_conversion(self.find_first_data(data, 'created_at')),
            "full_text": self.find_first_data(data, 'full_text'),
            "favorite_count": self.find_first_data(data, 'favorite_count'),
            "reply_count": self.find_first_data(data, 'reply_count'),
            "retweet_count": self.find_first_data(data, 'retweet_count'),
            "source": self.find_first_data(data, 'source'),
            "lang": self.find_first_data(data, 'lang'),
            "media_info": {
                "entities": self.entities(data),
                "extended_entities": self.extended_entities(data),
            }
        }
        return info


class DataExtract(BaseExtract):

    def get_UserInfo(self, name, new_name):
        """
        获取推特用户信息
        :param name: 读取的redis库
        :param new_name: 写入的redis库
        :return:
        """
        for rest_id, item in self.iter_get_all(name):
            # rest_id为当前数据库信息id
            # 重新获取rest_id
            only_key = self.find_first_data(item, 'rest_id')

            info = {
                only_key: self.extract_userInfo(item)
            }
            effective = any([True if item else False for key, item in info[only_key].items()])
            if effective:
                logger.info(f"获取推特用户信息成功：{info}")
                self.execute(redis_name=new_name, redis_key=only_key, data=info, over=True)

    def get_UserLikesInfo(self, name, new_name, over=True):
        """"""
        for num, (rest_id, item) in enumerate(self.iter_get_all(name)):
            item = json.loads(item)
            if not isinstance(item, dict):
                continue

            rest_id = item.get('rest_id')

            owner_finder = self.find_first_data(item, 'user')
            legacy = self.find_last_data(item, 'legacy')

            if owner_finder and legacy:
                info = {
                    'rest_id': rest_id,
                    "UserInfo": self.extract_userInfo(owner_finder),
                    "userLikesInfo": self.extract_userLikesInfo(legacy),
                }
                self.execute(redis_name=new_name, redis_key=rest_id, data=info, over=over)

    def get_UserMediaInfo(self, name, new_name, over=True):
        """
        提取用户媒体信息
        :param name:
        :param new_name:
        :param over:
        :return:
        """
        for num, (rest_id, item) in enumerate(self.iter_get_all(name)):
            item = json.loads(item)
            if not isinstance(item, dict):
                continue

            owner = self.find_first_data(item, 'user')
            legacy = self.find_last_data(item, 'legacy')

            if owner and legacy:
                info = {
                    "rest_id": rest_id,
                    "UserInfo": self.extract_userInfo(owner),
                    "userMediaInfo": self.extract_userMediaInfo(legacy)
                }
                self.execute(redis_name=new_name, redis_key=rest_id, data=info, over=over)
                logger.info(f"提取用户媒体信息成功：{info}")


class DataAnalysis(DataExtract):

    def get_allTwitterUserInfo(self):
        """获取所有推特用户信息"""
        name_list = [
            self.redis_Following,
            self.redis_Follower,
            self.redis_FollowersYouKnow,
            self.redis_UserMedia,
            self.redis_UserLikes,
            self.redis_UserTweets
        ]

        for name in name_list:
            new_name = self.redis_userInfo_name + name
            self.get_UserInfo(name=name, new_name=new_name)

    def get_allUserMedia(self):
        """"""
        name = self.redis_UserMedia
        new_name = self.redis_UserMedia_name
        self.get_UserMediaInfo(name=name, new_name=new_name)

    def get_allUserLikesInfo(self):
        name = self.redis_UserLikes
        new_name = self.redis_UserLikes_name
        self.get_UserLikesInfo(name=name, new_name=new_name)


if __name__ == '__main__':
    d = DataAnalysis()
    # d.get_allUserMedia()
    # d.get_allTwitterUserInfo()
    # d.get_allUserLikesInfo()
