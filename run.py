#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: run.py
@Ide: PyCharm
@Time: 2021-06-08 20:32:16
@Desc: 
"""
from api.spider import SpiderAPI
from download.download import Download
from utils.logger import logger
from utils.utils import thread_pool, send_email


class BaseSpider(SpiderAPI):
    def __init__(self):
        super(BaseSpider, self).__init__()
        self.user_id = self.choose_input()

    def searchRestIdByScreenName(self, screen_name):
        item = self.API_UserByScreenNameWithoutResults(screen_name)
        rest_id = self.find_first_data(item, 'rest_id')
        return rest_id

    def choose_input(self):
        resp = self.API_Notifications()
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
            input_str = input("\n请输入前面的序号确认你的账户，输入其他数字跳过：")
            if input_str.isdigit():
                choose = int(input_str)
                is_true = True
            else:
                print("请输入数字！")

        items = {num: id_str for num, id_str in enumerate(id_strs)}
        user_id = None
        try:
            user_id = items[choose]
        except KeyError:
            user_id_is_true = False
            while not user_id_is_true:
                input_str = input("\n请输入你要爬取的screen_name：")
                user_id = self.searchRestIdByScreenName(input_str)
                if not user_id:
                    print("screen_name无信息，请重新输入")
                else:
                    user_id_is_true = True
        print(f"爬取入口的user_id: {user_id}")
        logger.info(f"爬取入口的user_id: {user_id}")
        return user_id


class TwitterSpider(BaseSpider):

    def __init__(self):
        super(TwitterSpider, self).__init__()
        self.data = None

    def initFollowingData(self):
        for num, (user_id, item) in enumerate(self.data,start=1):
            user_id = str(user_id).split("_")[-1] if self.level == 1 else user_id
            name = self.find_first_data(item, 'name')
            screen_name = self.find_first_data(item, 'screen_name')
            if self.thread == 0:
                yield user_id, num, name, screen_name
            else:
                yield {'rest_id': user_id, 'num': num, "name": name, "screen_name": screen_name}

    def getFollowingData(self):
        self.getOne_Likes(rest_id=self.user_id)
        if self.thread == 0:
            for item in self.initFollowingData():
                user_id, num, name, screen_name = item
                logger.info(f"开始获取[{num}]【{user_id}——{screen_name}——{name}】的信息")
                self.getOne_UserMedia(rest_id=user_id, over=self.over, count=self.count)
        else:
            thread_pool(self.getOne_UserMedia, list(self.initFollowingData()), prompt="获取用户媒体信息",
                        thread_num=self.thread)

    def run_spider(self):
        logger.info("开始运行...")
        # 获取关注的人
        print("获取关注的人...")
        self.getOne_Following()  # 必须打开
        # 爬取内容
        match = f'{self.user_id}_*' if self.level == 1 else None
        self.data = self.iter_get_all(name=self.spider_redis.spider_Following, match=match)
        self.getFollowingData()

    def run_extractor(self):
        # 提取内容
        self.get_allTwitterInfo()
        # 提取下载信息
        self.init_downloadInfo()

    def run_download(self):
        self.rest_id_list.append(self.user_id)
        d = Download(rest_id_list=self.rest_id_list)
        d.run_download()

    def test(self):
        self.get_Home()


def main():
    import time
    t = TwitterSpider()
    spider_wait = t.spider_wait * 60 * 60

    # 每当爬取完毕后 间隔spider_wait小时运行一次爬虫
    while True:
        spider_start_time = int(time.time())
        t.run_spider()
        spider_end_time = int(time.time())
        logger.info(f"爬虫耗时：{spider_end_time - spider_start_time}")

        extractor_start_time = int(time.time())
        t.run_extractor()
        extractor_end_time = int(time.time())
        logger.info(f"提取数据耗时：{extractor_start_time - extractor_end_time}")

        download_start_time = int(time.time())
        t.run_download()
        download_end_time = int(time.time())
        logger.info(f"下载耗时：{download_start_time - download_end_time}")

        logger.info(f"开始等待：时间{spider_wait}")

        time.sleep(spider_wait)

    # 测试接口
    # self.test()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        send_email(e)
