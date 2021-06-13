#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: run_spider.py
@Ide: PyCharm
@Time: 2021-06-01 16:56:01
@Desc:
"""
import inspect

from api.twitter import TwitterApi
from extractor.extractor import DataAnalysis
from utils.exception import TSException
from utils.logger import logger
from utils.twitter_redis import TwitterRedis


class SpiderAPI(TwitterApi, DataAnalysis):
    """
    抓取接口所有数据
    """

    def __init__(self):
        super(SpiderAPI, self).__init__()
        self.spider_redis = TwitterRedis()
        self.redis = self.spider_redis.redis
        self.num = 1

    @staticmethod
    def check_type_is_list(data):
        if isinstance(data, str) or isinstance(data, int):
            data = [str(data)]
        return data

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
        return_dict = kwargs.get('return_dict', True)
        fetch = kwargs.get('fetch', True)
        if not isinstance(return_dict, bool):
            raise TSException(f"类型错误应该为bool:{type(return_dict)}")
        if not isinstance(fetch, bool):
            raise TSException(f"类型错误应该为bool:{type(fetch)}")
        return return_dict, fetch

    def get_function(self):
        """
        获取被指定调用的方法
        """
        current = inspect.currentframe()
        get_outer = inspect.getouterframes(current, 2)
        function_name = get_outer[2][3]
        function = getattr(self, function_name)
        return function,function_name

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

        return_dict, fetch = self.init_params(**kwargs)
        count, over = self.count, self.over

        function,function_name = self.get_function()
        start_prompt, end_prompt = self.doc_info(function)

        # 获取数据
        resp = api(rest_id=rest_id, cursor=cursor, count=count, return_dict=return_dict, fetch=fetch)

        # 提取信息
        itemContents = self.find_all_data(resp, 'itemContent')

        # 检查是否提取完毕
        if not itemContents:
            self.num = 1
            return

        for n, itemContent in enumerate(itemContents,start=1):
            rest_id = self.find_first_data(itemContent, 'rest_id')
            name = self.find_first_data(itemContent, 'name')
            self.rest_id_list.append(rest_id)

            # 某人的某信息level=1
            redis_key = f"{self.user_id}_{rest_id}" if self.level == 1 else rest_id

            # 保存数据
            self.spider_redis.execute(redis_name=redis_name, redis_key=redis_key, data=itemContent, over=over)

            logger.info(f"Success {start_prompt} [{self.user_id}]: [{self.num}][{n}/{len(itemContents)}] {rest_id}->{name}")
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
        return_dict, fetch = self.init_params(**kwargs)
        count, over = self.count, self.over
        function,function_name = self.get_function()
        start_prompt, end_prompt = self.doc_info(function)

        resp = api(rest_id=rest_id, cursor=cursor, count=count, return_dict=return_dict, fetch=fetch)
        cursor = self.find_last_data(resp, 'value')

        # 检查是否已经下载
        have_only_name = f"{self.spider_have}_{function_name}"
        have_only_key = f"{rest_id}_{cursor}"
        if self.redis.hexists(name=have_only_name,key=have_only_key):
            self.num = 1
            return

        # 检查是否提取完毕
        if not self.find_exists(resp, 'tweet'):
            self.num = 1
            return

        # 获取数据来源
        entries_list = self.find_effective_data(resp, target='entries')

        # 筛选数据  可能出现无效数据
        tweet_list = self.find_all_data(entries_list, target='tweet')

        # 清除不完整数据
        new_tweet_list = tweet_list.copy()
        for _ in tweet_list:
            if len(_) == 1:
                new_tweet_list.remove(_)

        # 检查是否提取完毕
        if not new_tweet_list:
            self.num = 1
            return

        # 可能出现脏数据 原因未知
        user_list = self.find_all_data(new_tweet_list, 'user')
        if not user_list:
            self.num = 1
            return

        # 推特信息原创者的rest_id
        user_rest_ids = self.check_type_is_list(self.find_all_data(user_list, 'rest_id'))

        # 处理脏数据
        user_rest_id = max(user_rest_ids, key=user_rest_ids.count)
        user_rest_ids = [user_rest_id for _ in range(len(new_tweet_list))]

        # 推特rest_id
        twitter_rest_ids = self.check_type_is_list(self.find_all_last_data(new_tweet_list, 'rest_id'))

        zip_data = zip(user_rest_ids, twitter_rest_ids, new_tweet_list)
        for n, (user_rest_id, twitter_rest_id, tweet) in enumerate(zip_data,start=1):
            # 保存数据
            only_key = f'{user_rest_id}_{twitter_rest_id}'
            self.spider_redis.execute(redis_name=redis_name, redis_key=only_key, data=tweet, over=over)

            logger.info(f"Success {end_prompt} [{self.user_id}]: [{self.num}][{n}/{len(user_rest_ids)}] {only_key}")
            self.num += 1

        # 存储下载信息
        self.spider_redis.execute(redis_name=have_only_name, redis_key=have_only_key, data=new_tweet_list, over=over)

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
        :param kwargs level: 爬取层数
        :return:
        """
        logger.info("获取用户所有正在关注信息")
        redis_name = self.spider_redis.spider_Following
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
        redis_name = self.spider_redis.spider_Follower
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
        redis_name = self.spider_redis.spider_FollowersYouKnow
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
        redis_name = self.spider_redis.spider_UserMedia
        api = self.API_UserMedia

        # 多线程赋值
        if isinstance(rest_id, dict):
            num = rest_id.get('num')
            name = rest_id.get('name')
            screen_name = rest_id.get('screen_name')
            rest_id = rest_id.get('rest_id')
            logger.info(f"开始获取[{num}]【{rest_id}——{screen_name}——{name}】的信息")
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
        redis_name = self.spider_redis.spider_UserLikes
        api = self.API_UserLikes
        # 多线程赋值
        if isinstance(rest_id, dict):
            num = rest_id.get('num')
            name = rest_id.get('name')
            screen_name = rest_id.get('screen_name')
            rest_id = rest_id.get('rest_id')
            logger.info(f"开始获取[{num}]【{rest_id}——{screen_name}——{name}】的信息")
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
        redis_name = self.spider_redis.spider_UserTweets
        api = self.API_UserTweets
        self.user_judge(rest_id=rest_id, cursor=cursor, redis_name=redis_name, api=api, **kwargs)

    def get_Home(self, rest_id=None, cursor=None, **kwargs):
        """
        刷推特
        """
        # TODO 开发中
        import json
        # redis_name = self.spider_redis.spider_Home
        resp = self.API_home(cursor='HBbO/v/ro8yr+SYAAA==')
        with open("2.json",'w')as f:
            f.write(json.dumps(resp,ensure_ascii=False))
        # self.spider_redis.execute(redis_name=redis_name, redis_key='only_key', data=resp, over=True)
        # self.spider_redis.execute(redis_name=redis_name, redis_key='tweets', data=tweets, over=True)

        with open("1.json",'r')as f:
            resp = f.read()

        users = self.find_all_data(resp,'users')
        id_str_list = self.find_all_data(users,'id_str')
        print("用户：",len(id_str_list))

        tweets = self.find_all_data(resp,'tweets')
        user_id_str_list = self.find_all_data(tweets,'user_id_str')
        print("推文用户：",len(user_id_str_list))

        a = list()
        for user in id_str_list:
            for tweet in user_id_str_list:
                if tweet == user:
                    a.append(tweet)
        print(len(a))



