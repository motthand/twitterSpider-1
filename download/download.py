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

import requests

from analysis.data_analysis import DataAnalysis
from extractor.json_path_finder import JsonDataFinder
from utils.fetch import fetch
from utils.utils import thread_pool


class Download(DataAnalysis):
    session = requests.session()

    def download(self, kwargs):
        url = kwargs.get('url')
        filepath = kwargs.get('filepath')
        resp = fetch(session=self.session, url=url, check_code=False)
        if resp.status_code == 200:
            with open(filepath, 'wb') as f:
                # for chunk in resp.iter_content(chunk_size=1024):
                #     f.write(chunk)
                f.write(resp.content)

    def download_img(self, base_path=None):
        download_info = []
        for item in self.iter_get_all(self.redis_UserMedia_name):
            key, item = item
            item = json.loads(item)
            # print(item)
            finder = JsonDataFinder(item)
            name = finder.find_first_data('昵称')
            path = os.path.join(base_path if base_path else os.getcwd(), self.redis_UserLikes_name, name)

            url_list = finder.find_all_data('media_url_https')
            if url_list:
                for url in url_list:
                    if url.endswith('.jpg'):

                        if not os.path.exists(path):
                            os.makedirs(path)
                        filename = url.split('/')[-1]
                        filepath = os.path.join(path, filename)
                        if not os.path.exists(filepath):
                            download_info.append({
                                "url": url,
                                "filepath": filepath,
                            })

        thread_pool(self.download, data=download_info, thread_num=3)


if __name__ == '__main__':
    d = Download()
