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
from utils.twitter_redis import TwitterRedis
from utils.utils import twitter_conversion


class BaseAnalysis(TwitterRedis, ExtractorApi):
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
            "name": self.find_first_data(data, 'name'),
            "screen_name": self.find_first_data(data, 'screen_name'),
            "location": self.find_first_data(data, 'location'),
            "created_at": twitter_conversion(self.find_first_data(data, 'created_at')),
            "friends_count": self.find_first_data(data, 'friends_count'),
            "followers_count": self.find_first_data(data, 'followers_count'),
            "favourites_count": self.find_first_data(data, 'favourites_count'),
            "media_count": self.find_first_data(data, 'media_count'),
            "description": self.find_first_data(data, 'description'),
            "profile_banner_url": self.find_first_data(data, 'profile_banner_url'),
            "profile_image_url_https": self.find_first_data(data, 'profile_image_url_https'),
            "entities": self.entities(data),
        }
        return user_info

    def extract_dataInfo(self, data):
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
            "entities": self.entities(data),
            "extended_entities": self.extended_entities(data),
        }
        return info


class DataExtract(BaseExtract):

    def _UserInfo(self, data):
        rest_id, item, new_name, kwargs = data
        # rest_id为当前数据库信息id
        # 重新获取rest_id
        only_key = self.find_first_data(item, 'rest_id')

        info = {
            only_key: self.extract_userInfo(item)
        }
        effective = any([True if item else False for key, item in info[only_key].items()])
        if effective:
            self.execute(redis_name=new_name, redis_key=only_key, data=info, **kwargs)
            logger.info(f"获取推特用户信息成功：{info}")

    def _dataInfo(self, data):
        rest_id, item, new_name, kwargs = data
        item = json.loads(item)
        if isinstance(item, dict):
            owner = self.find_first_data(item, 'user')
            legacy = self.find_last_data(item, 'legacy')
            if owner and legacy:
                info = {
                    "rest_id": rest_id,
                    "UserInfo": self.extract_userInfo(owner),
                    "dataInfo": self.extract_dataInfo(legacy)
                }
                self.execute(redis_name=new_name, redis_key=rest_id, data=info, **kwargs)
                logger.info(f"提取用户数据成功：{info}")

    def _downloadInfo(self,data):
        rest_id, item, new_name, kwargs = data
        data_info = {
            "rest_id": self.find_first_data(item, 'rest_id'),
            "name": self.find_first_data(item, 'name'),
            "screen_name": self.find_first_data(item, 'screen_name'),
        }
        # 视频
        video_info = self.find_all_data(item, 'video_info')
        if video_info and any(video_info):
            video = [_ for video in video_info if video for _ in self.find_all_data(video, 'variants')]
            data_info.update({"video_info": video})
        else:
            data_info.update({"video_info": []})

        # 图片
        url_list = self.find_all_data(item, 'media_url_https')
        img_list = [url for url in url_list if url.endswith('.jpg')] if url_list else []
        data_info.update({"img_info": img_list})

        # 语音

        self.execute(redis_name=new_name, redis_key=rest_id, data=data_info, **kwargs)
        logger.info(f"提取用户数据成功：{data_info}")

