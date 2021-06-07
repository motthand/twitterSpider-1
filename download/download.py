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
from os.path import normpath

import requests
import tqdm
import numpy as np

from analysis.data_analysis import BaseAnalysis
from config import global_config
from extractor.json_path_finder import JsonDataFinder
from utils.fetch import fetch
from utils.utils import thread_pool
from urllib.parse import urlunparse, urlparse

DOWNLOAD_PATH = global_config.getRaw('download', 'download_path')
if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)


class Download(BaseAnalysis):
    session = requests.session()
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36'
    }

    def __init__(self):
        super(Download, self).__init__()
        self.run()

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

    def get_downloadInfo(self):
        redis_name = self._analysis_downloadInfo
        download_info = list(self.iter_get_all(redis_name))
        for rest_id, item in download_info:
            item = json.loads(item)
            name = item.get('name')
            base_path = os.path.join(DOWNLOAD_PATH, name)
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

    def run(self):
        download_item = self.get_downloadInfo()
        download_list = list(download_item)

        download_info = list()
        for _ in download_list:
            img = _.get('img')
            if img:
                for i in img:
                    i.update({'file_type': 'img'})
                    download_info.append(i)

            video = _.get('video')
            if video:
                video.update({'file_type': 'video'})
                download_info.append(video)

        thread_pool(method=self.download,data=download_info,prompt="下载图片",thread_num=2)


if __name__ == '__main__':
    d = Download()
