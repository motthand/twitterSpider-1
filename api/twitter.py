#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: twitterSpider.py
@Ide: PyCharm
@Time: 2021-05-30 17:21:20
@Desc: 
"""
import json

from api.login import TwitterLogin
from extractor.extractor import ExtractorApi
from utils.fetch import fetch_dict, fetch_json
from utils.logger import logger


class TwitterApi(TwitterLogin, ExtractorApi):
    """
    推特API接口
    """

    def __init__(self, mode='mobile'):
        super(TwitterApi, self).__init__()
        self._login()
        self.init_url(mode)
        self.user_id = self.API_Notifications()

    def init_url(self, mode):
        """
        初始化url信息
        :return:
        """
        if mode.lower() == "mobile":
            self.base_url = "https://mobile.twitter.com/"
        elif mode.lower() == "pc":
            self.base_url = "https://twitter.com/"
        else:
            raise Exception(f"The mode cannot be set to {mode}, please set to 'pc' or 'mobile' (not case sensitive).")

        self.media_url = self.base_url + "{0}/media"
        self.likes_url = self.base_url + "{0}/likes"

        # api接口
        self.all = self.base_url + "i/api/2/notifications/all.json"
        self.Following = self.base_url + "i/api/graphql/5GZ21_sdcuDYcNfgT3sgbA/Following"
        self.Followers = self.base_url + "i/api/graphql/8-nEgbPoDML61atIQ6GLwQ/Followers"
        self.UserMedia = self.base_url + "i/api/graphql/ep3EdGK189uKvABB-8uIlQ/UserMedia"
        self.Likes = self.base_url + "i/api/graphql/OU4zjDOFfM9ZHq2aTjUNCA/Likes"
        self.UserTweets = self.base_url + 'i/api/graphql/1DL8zlYnR-WKbi0BUG2rzQ/UserTweets'
        self.UserTweetsAndReplies = self.base_url + "i/api/graphql/Kq7XqqyDGn4Ly7Yh0AhK9w/UserTweetsAndReplies"
        self.FollowersYouKnow = self.base_url + "i/api/graphql/Vitbmqp6_6fOiDatDzbOAQ/FollowersYouKnow"
        self.UserByScreenNameWithoutResults = self.base_url + "i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults"

    def fetch_dict(self, url, method='get', session=None, **kwargs):
        session = self.session if session is None else session
        return fetch_dict(session=session, url=url, method=method, **kwargs)

    def fetch_json(self, url, method='get', session=None, **kwargs):
        session = self.session if session is None else session
        return fetch_json(session=session, url=url, method=method, **kwargs)

    def selector(self, url, params, **kwargs) -> dict:
        """
        选择器
        :param url: url
        :param params: 参数
        :param return_dict: 是否返回字典格式
        :param fetch: 是否请求
        :return:
        """
        fetch = kwargs.get('fetch', True)
        return_dict = kwargs.get('return_dict', True)

        if not isinstance(fetch, bool):
            raise Exception(type(fetch))
        if not isinstance(return_dict, bool):
            raise Exception(type(return_dict))

        if fetch:
            if return_dict:
                return self.fetch_dict(url=url, params=params)
            return self.fetch_json(url=url, params=params)
        return {"url": url, "params": params}

    def API_UserByScreenNameWithoutResults(self, screen_name, **kwargs):
        """
        :param screen_name:
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.UserByScreenNameWithoutResults
        params = {
            "variables": json.dumps({"screen_name": screen_name, "withHighlightedLabel": True})
        }
        return self.selector(url=url, params=params, **kwargs)

    def API_Notifications(self):
        """
        通知API
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param return_dict: 是否返回字典格式
        :return:
        """
        url = self.all
        params = {
            'cards_platform': 'Web-12',
            'count': '20',
            'ext': 'mediaStats,highlightedLabel',
            'include_blocked_by': '1',
            'include_blocking': '1',
            'include_can_dm': '1',
            'include_can_media_tag': '1',
            'include_cards': '1',
            'include_entities': True,
            'include_ext_alt_text': True,
            'include_ext_media_availability': True,
            'include_ext_media_color': True,
            'include_followed_by': '1',
            'include_mute_edge': '1',
            'include_profile_interstitial_type': '1',
            'include_quote_count': True,
            'include_reply_count': '1',
            'include_user_entities': True,
            'include_want_retweets': '1',
            'send_error_codes': True,
            'simple_quoted_tweet': True,
            'skip_status': '1',
            'tweet_mode': 'extended'
        }
        resp = self.fetch_dict(url, params=params)
        finder = self.find_first_data(resp, 'users')
        screen_names = self.find_all_data(finder, 'screen_name')
        names = self.find_all_data(finder, 'name')
        id_strs = self.find_all_data(finder, 'id_str')

        print("在通知API中获取到了以下几个用户：")
        for num, (name, id_str, screen_name) in enumerate(zip(names, id_strs, screen_names)):
            print(f"\t{num} {name} {id_str} {screen_name}")

        is_true = False
        choose = None
        while not is_true:
            input_str = input("\n请输入前面的序号确认你的账户：")
            if input_str.isdigit():
                choose = int(input_str)
                is_true = True
            else:
                print("请输入数字！")

        items = {num: id_str for num, id_str in enumerate(id_strs)}
        user_id = items[choose]
        logger.info(f"本账号的user_id: {user_id}")
        return user_id

    def API_Following(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        正在关注
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.Following
        params = {
            "variables": json.dumps({
                "userId": rest_id,
                "count": count,
                "cursor": cursor,
                "withHighlightedLabel": False,
                "withTweetQuoteCount": False,
                "includePromotedContent": False,
                "withTweetResult": False,
                "withReactions": False,
                "withUserResults": False,
                "withNonLegacyCard": True,
                "withBirdwatchPivots": False
            })
        }

        return self.selector(url=url, params=params, **kwargs)

    def API_Followers(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        关注者
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        ::param kwargs fetch: 是否请求
        :return:
        """
        url = self.Followers
        params = {
            "variables": json.dumps({
                'userId': rest_id,
                'count': count,
                'cursor': cursor,
                'withHighlightedLabel': False,
                'withTweetQuoteCount': False,
                'includePromotedContent': False,
                'withTweetResult': False,
                'withReactions': False,
                'withUserResults': False,
                'withNonLegacyCard': True,
                'withBirdwatchPivots': False
            })
        }
        return self.selector(url=url, params=params, **kwargs)

    def API_FollowersYouKnow(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        可能认识的人
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.FollowersYouKnow
        params = {"variables": json.dumps({
            'userId': rest_id,
            'count': count,
            'cursor': cursor,
            'withHighlightedLabel': False,
            'withTweetQuoteCount': False,
            'includePromotedContent': False,
            'withTweetResult': False,
            'withReactions': False,
            'withUserResults': False,
            'withNonLegacyCard': True,
            'withBirdwatchPivots': False
        })}
        return self.selector(url=url, params=params, **kwargs)

    def API_UserMedia(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        获取用户媒体
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.UserMedia
        params = {"variables": json.dumps({
            "userId": rest_id,
            "count": count,
            "cursor": cursor,
            "withHighlightedLabel": False,
            "withTweetQuoteCount": False,
            "includePromotedContent": False,
            "withTweetResult": False,
            "withReactions": False,
            "withUserResults": False,
            "withClientEventToken": False,
            "withBirdwatchNotes": False,
            "withBirdwatchPivots": False,
            "withVoice": False,
            "withNonLegacyCard": True
        })}
        return self.selector(url=url, params=params, **kwargs)

    def API_UserLikes(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        用户喜欢
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.Likes
        params = {"variables": json.dumps({
            'userId': rest_id,
            'count': count,
            "cursor": cursor,
            'withHighlightedLabel': False,
            'withTweetQuoteCount': False,
            'includePromotedContent': False,
            'withTweetResult': False,
            'withReactions': False,
            'withUserResults': False,
            'withClientEventToken': False,
            'withBirdwatchNotes': False,
            'withBirdwatchPivots': False,
            'withVoice': False,
            'withNonLegacyCard': True
        })}
        return self.selector(url, params, **kwargs)

    def API_UserTweets(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        用户推文
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.UserTweets
        params = {"variables": json.dumps({
            'userId': rest_id,
            'count': count,
            'cursor': cursor,
            'withHighlightedLabel': True,
            'withTweetQuoteCount': True,
            'includePromotedContent': True,
            'withTweetResult': False,
            'withReactions': False,
            'withUserResults': False,
            'withVoice': False,
            'withNonLegacyCard': True,
            'withBirdwatchPivots': False
        })}
        return self.selector(url=url, params=params, **kwargs)

    def API_UserTweetsAndReplies(self, rest_id, cursor=None, count=20, **kwargs) -> dict:
        """
        推文和回复
        :param rest_id: 用户id
        :param cursor: 筛选条件
        :param count: 数量
        :param kwargs return_dict: 是否返回字典格式
        :param kwargs fetch: 是否请求
        :return:
        """
        url = self.UserTweetsAndReplies
        params = {"variables": json.dumps({
            'userId': rest_id,
            'count': count,
            'cursor': cursor,
            'withHighlightedLabel': True,
            'withTweetQuoteCount': True,
            'includePromotedContent': True,
            'withTweetResult': False,
            'withReactions': False,
            'withUserResults': False,
            'withVoice': False,
            'withNonLegacyCard': True,
            'withBirdwatchPivots': False})}
        return self.selector(url=url, params=params, **kwargs)
