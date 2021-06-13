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
from utils.email import Email
from utils.logger import logger
from utils.utils import thread_pool


class TimingMonitoring(object):
    """计时监控"""

    def __init__(self, function, prompt):
        import time
        from datetime import datetime
        self.prompt = prompt
        start_localtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_time = int(time.time())
        function()
        end_time = int(time.time())
        end_localtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[爬虫结束]: [{end_localtime} -> {start_localtime}] 耗时：{self.sec_to_data(end_time - start_time)}")

    def convert_time_to_str(self, time):
        # 时间数字转化成字符串，不够10的前面补个0
        if (time < 10):
            time = '0' + str(time)
        else:
            time = str(time)
        return time

    def sec_to_data(self, y):
        h = int(y // 3600 % 24)
        d = int(y // 86400)
        m = int((y % 3600) // 60)
        s = round(y % 60, 2)
        h = self.convert_time_to_str(h)
        m = self.convert_time_to_str(m)
        s = self.convert_time_to_str(s)
        d = self.convert_time_to_str(d)
        # 天 小时 分钟 秒
        return d + ":" + h + ":" + m + ":" + s


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

    def initFollowingData(self):
        match = f'{self.user_id}_*' if self.level == 1 else None
        data = self.iter_get_all(name=self.spider_redis.spider_Following, match=match)
        for num, (user_id, item) in enumerate(data, start=1):
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
        """爬取信息"""
        logger.info("开始运行...")
        self.getOne_Following()  # 必须打开
        Email(title="爬取信息", content="[1/6]获取正在关注人信息成功")
        self.getFollowingData()
        Email(title="爬取信息", content="[2/6]获取媒体信息成功")

    def run_extractor(self):
        """提取内容&提取下载信息"""
        self.get_allTwitterInfo()
        Email(title="提取内容", content="[3/6]提取内容信息成功")
        self.init_downloadInfo()
        Email(title="提取下载信息", content="[4/6]提取下载信息成功")

    def run_download(self):
        """下载数据"""
        self.rest_id_list.append(self.user_id)
        Email(title="开始下载数据", content="[5/6]正在下载数据...")
        d = Download(rest_id_list=self.rest_id_list)
        d.run_download()

    def test(self):
        self.get_Home()


def main():
    import time
    t = TwitterSpider()
    # 每当爬取完毕后 间隔spider_wait小时运行一次爬虫
    spider_wait = t.spider_wait * 60 * 60
    SPIDER_NUM = 1
    while True:
        # 爬虫模块
        TimingMonitoring(function=t.run_spider, prompt='爬虫')
        # 提取模块
        TimingMonitoring(function=t.run_extractor, prompt='提取')
        # 下载模块
        TimingMonitoring(function=t.run_download, prompt='下载')
        Email(title="数据下载完成", content="[6/6]数据下载完成。\n已完成{}次爬虫，将等待{}小时再次爬取".format(SPIDER_NUM, spider_wait))
        SPIDER_NUM += 1

        logger.info(f"开始等待：时间{spider_wait}")
        time.sleep(spider_wait)

    # 测试接口
    # self.test()


if __name__ == '__main__':
    # main()
    try:
        main()
    except Exception as e:
        Email(title="爬虫被迫中断", content="Twitter爬虫异常", error=e)
