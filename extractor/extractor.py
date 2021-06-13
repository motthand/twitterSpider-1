#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: extractor.py
@Ide: PyCharm
@Time: 2021-06-01 18:38:01
@Desc: 提取器
"""

import json

import tqdm

from extractor.json_path_finder import JsonDataFinder
from utils.logger import logger
from utils.twitter_redis import TwitterRedis
from utils.utils import twitter_conversion, thread_pool


class ExtractorApi(object):
    """
    数据提取器接口
    """

    def finder(self, resp):
        if isinstance(resp, dict):
            resp = resp.get('data', resp)
        elif isinstance(resp, str):
            resp = json.loads(resp).get('data', resp)

        finder = JsonDataFinder(resp)
        return finder

    def find_exists(self, resp, target: str) -> bool:
        """
        检查是否存在
        :return:
        """
        finder = self.finder(resp)
        exists = finder.find_first(target)
        if exists:
            return True
        return False

    def find_all_data(self, resp, target: str):
        """
        提取target下所有数据
        :param resp:
        :param target:
        :return:
        """
        finder = self.finder(resp)
        target_list = finder.find_all_data(target)
        if not target_list:
            return None
        if not isinstance(target_list, list):
            target_list = [target_list]
        return target_list

    def find_all_last_data(self, resp, target: str):
        """
        提取target下 的父级target值 或叫做 所有单个列表中最后一个target值
        :param resp:
        :param target:
        :return:
        """
        finder = self.finder(resp)
        target_list = finder.find_all_last_data(target)
        return target_list

    def find_first_data(self, resp, target: str):
        """
        提取target下第一个数据
        :param resp:
        :param target:
        :return:
        """
        finder = self.finder(resp)
        first_target = finder.find_first_data(target)
        return first_target

    def find_last_data(self, resp, target: str):
        """
        提取target下第最后一个数据
        :param resp:
        :param target:
        :return:
        """
        finder = self.finder(resp)
        first_target = finder.find_last_data(target)
        return first_target

    def find_assign_data(self, resp, target: str, index: int):
        """
        提取target下第index个数据
        :param resp:
        :param target:
        :param index:
        :return:
        """
        finder = self.finder(resp)
        assign_target = finder.find_assign_data(target, index)
        return assign_target

    def find_all_same_level_data(self, resp, target: str):
        finder = self.finder(resp)
        target_list = finder.find_all_same_level_data(target)
        return target_list

    def find_effective_data(self, resp, target: str):
        """
        获取有效数据
        推特有效数据默认排除最后两个
        :param resp:
        :param target:
        :return:
        """
        finder = self.finder(resp)
        target_list = finder.find_all_data(target)
        return target_list[:-2]


class BaseAnalysis(TwitterRedis, ExtractorApi):

    def iter_get_all(self, name, match=None, count=None):
        """
        分批获取redis所有数据
        :param name:
        :return:
        """
        for item in self.redis.hscan_iter(name=name, match=match, count=count):
            yield item

    def iter_get_all_by_key(self, name, match=None):
        keys = sorted(self.redis.hkeys(name))
        for key in keys:
            if match in key:
                data = self.redis.hget(name, key)
                yield key, data

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
        if not self.redis.hexists(new_name, rest_id):
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
        if not self.redis.hexists(new_name,rest_id):
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
                    # logger.info(f"提取用户数据成功：{info}")

    def _downloadInfo(self, data):
        rest_id, item, new_name, kwargs = data
        if not self.redis.hexists(new_name, rest_id):
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
            # logger.info(f"提取用户数据成功：{data_info}")


class DataExtractOneThread(DataExtract):

    def get_UserInfo(self, name, new_name, **kwargs):
        """
        提取推特用户信息
        :param name: 读取的redis库
        :param new_name: 写入的redis库
        :return:
        """
        prompt = f'提取推特用户信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._UserInfo(data=[rest_id, item, new_name, kwargs])

    def get_UserLikesInfo(self, name, new_name, **kwargs):
        """
        提取用户喜欢信息
        """
        prompt = f'提取用户喜欢信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._dataInfo(data=[rest_id, item, new_name, kwargs])

    def get_UserMediaInfo(self, name, new_name, **kwargs):
        """
        提取用户媒体信息
        :param name:
        :param new_name:
        :param kwargs over:
        :return:
        """
        prompt = f'提取用户媒体信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._dataInfo([rest_id, item, new_name, kwargs])

    def get_DownloadInfo(self, name, new_name, **kwargs):
        prompt = f'初始化下载信息->[from:{name}]->[save::{new_name}]'
        redis_data = list(self.iter_get_all(name))
        res = tqdm.tqdm(redis_data, total=len(redis_data))
        for rest_id, item in res:
            res.set_description(prompt)
            self._downloadInfo([rest_id, item, new_name, kwargs])


class DataExtractPool(DataExtractOneThread):

    def load_data(self, name, new_name):
        _len = len(self.rest_id_list)
        for num,user_id in enumerate(self.rest_id_list):
            num +=1
            match = f'{user_id}_*' if self.level == 1 else None
            _redis_data = [[rest_id, item, new_name, {'over': self.over}] for rest_id, item in
                           list(self.iter_get_all(name, match=match))]
            logger.info(f"开始提取{user_id}的数据")
            yield _redis_data,num,user_id,_len

    def _run(self, name, new_name, prompt, method):
        for redis_data in  self.load_data(name, new_name):
            _redis_data,num,user_id,_len = redis_data
            new_prompt = prompt + f"[{num}/{_len}]->{user_id}"
            thread_pool(method=method, data=_redis_data, prompt=new_prompt, thread_num=self.thread)

    def get_UserInfoPool(self, name, new_name):
        prompt = f'提取推特用户信息->[from:{name}]->[save:{new_name}]'
        method = self._UserInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method)

    def get_UserDataInfoPool(self, name, new_name):
        prompt = f'提取用户数据->[from:{name}]->[save:{new_name}]'
        method = self._dataInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method)

    def get_DownloadInfoPool(self, name, new_name):
        prompt = f'初始化下载信息->[from:{name}]->[save:{new_name}]'
        method = self._downloadInfo
        self._run(name=name, new_name=new_name, prompt=prompt, method=method)


class DataAnalysis(DataExtractPool):

    def get_allTwitterInfo(self):
        """获取所有推特用户信息"""

        # for spider, analysis in self.redis_name_item.get('spider_analysis_info').items():
        #     logger.info(f"[用户信息]正在处理redis数据库并获取:{spider} {analysis}")
        #     self.get_UserInfoPool(name=spider, new_name=analysis)

        for num, (spider, analysis) in enumerate(self.redis_name_item.get('spider_analysis').items()):
            logger.info(f"[用户数据]正在处理redis数据库并获取:{spider} {analysis}")
            self.get_UserDataInfoPool(name=spider, new_name=analysis)

    def init_downloadInfo(self):
        analysis = self._analysis_Info
        analysis_download = self._analysis_downloadInfo
        logger.info(f"[用户数据]正在处理redis数据库并获取:{analysis} {analysis_download}")
        self.get_DownloadInfoPool(name=analysis, new_name=analysis_download)
