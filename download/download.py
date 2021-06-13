#!/Users/llb/xuni/Spider/bin python
# -*- coding: utf-8 -*-
"""
@Author: llb
@Contact: geektalk@qq.com
@WeChat: llber233
@project: twitterSpider
@File: download.py
@Ide: PyCharm
@Time: 2021-06-03 12:59:53
@Desc: 
"""
import json
import os
import re

import requests

from extractor.extractor import BaseAnalysis
from utils.fetch import fetch
from utils.logger import logger
from utils.utils import thread_pool


class Download(BaseAnalysis):
    session = requests.session()

    def __init__(self, rest_id_list):
        super(Download, self).__init__()
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        self.rest_id_list = rest_id_list
        self.headers = self.User_Agent

    @staticmethod
    def save_img(path, resp):
        with open(path, 'wb') as f:
            f.write(resp.content)

    @staticmethod
    def save_video(path, resp):
        with open(path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                f.write(chunk)

    def download(self, kwargs):
        path = kwargs.get('path')
        if not os.path.exists(path):
            url = kwargs.get('url')
            file_type = kwargs.get('file_type')
            resp = fetch(session=self.session, url=url, check_code=False)
            if file_type == "img":
                self.save_img(path, resp)
            elif file_type == "video":
                self.save_video(path, resp)

    @staticmethod
    def check_folder(path):
        if not os.path.exists(path):
            os.makedirs(path)

    def init_downloadInfo(self, match):
        redis_name = self._analysis_downloadInfo
        for rest_id, item in self.iter_get_all(redis_name, match=match):
            user_id = str(rest_id).split('_')[0]
            item = json.loads(item)
            name = item.get('name')
            screen_name = item.get('screen_name')
            base_path = os.path.join(self.download_path, f"{user_id}_{screen_name}_{name}")
            download_item = dict()
            # 下载图片
            img_info = item.get('img_info')
            if img_info:
                img_path = os.path.join(base_path, 'img')
                self.check_folder(img_path)
                download_item['img'] = [{"url": _, "path": os.path.join(img_path, _.split("/")[-1])} for _ in img_info]

            video_info = item.get('video_info')
            if video_info:
                video_path = os.path.join(base_path, 'video')
                self.check_folder(video_path)
                new_video_info = [video for video in video_info if video.get('content_type') == "video/mp4"]
                new_video_info = sorted(new_video_info, key=lambda e: e.__getitem__('bitrate'), reverse=True)

                url = new_video_info[0].get('url')  # 取质量最好的mp4
                path = os.path.join(video_path, re.search("(.+\.mp4)", url.split("/")[-1]).group(1))
                download_item['video'] = {
                    "url": url,
                    "path": path
                }
            if download_item:
                yield download_item

    def run_download_thread_pool(self):
        logger.info("准备下载数据中...")
        download_info = list()
        for num, match in enumerate(self.rest_id_list, start=1):
            for info in self.init_downloadInfo(match=f"{match}_*"):
                img = info.get('img')
                if img:
                    for i in img:
                        i.update({'file_type': 'img'})
                        download_info.append(i)

                video = info.get('video')
                if video:
                    video.update({'file_type': 'video'})
                    download_info.append(video)
            logger.info(f"[{num}/{len(self.rest_id_list)}][{match}] 数据准备成功...")

        thread_pool(method=self.download, data=download_info, prompt="下载图片", thread_num=self.thread)

    def run_download(self):
        logger.info("准备下载数据中...")

        for num, match in enumerate(self.rest_id_list, start=1):
            download_info = list()
            for info in self.init_downloadInfo(match=f"{match}_*"):
                img = info.get('img')
                if img:
                    for i in img:
                        i.update({'file_type': 'img'})
                        download_info.append(i)

                video = info.get('video')
                if video:
                    video.update({'file_type': 'video'})
                    download_info.append(video)
            logger.info(f"[{num}/{len(self.rest_id_list)}][{match}] 开始下载...")
            thread_pool(method=self.download, data=download_info, prompt="下载图片", thread_num=self.thread)
